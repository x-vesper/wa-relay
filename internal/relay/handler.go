package relay

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (r *Relay) handleMessage(ctx context.Context, item taskItem) {
	start := time.Now()
	r.log.Info("task start id=%s text_len=%d", item.ID, len(item.Text))
	if r.handleCommand(item, start) {
		return
	}

	// Serah terima eksplisit ke tim teknis ("serahin aja ke Pak Budi") menang
	// atas lane apa pun: stepi bertindak sebagai resepsionis dan meneruskan
	// tugas, bukan mengeksekusi sendiri.
	if r.cfg.IsHandover(item.Text) {
		r.handleHandover(ctx, item)
		r.log.Info("task done id=%s handover duration=%.1fs", item.ID, time.Since(start).Seconds())
		return
	}

	// Konfirmasi bos ("setuju", dst) ke delegasi pending: jalankan pipeline
	// delegasi di background supaya chat tidak terblokir sambil build jalan.
	if confirmationRe.MatchString(item.Text) {
		if d, ok := r.delegations.Get(item.Chat); ok {
			r.delegations.Clear(item.Chat)
			go r.runDelegation(context.WithoutCancel(ctx), item, d, true)
			r.markRead(item.Chat)
			r.log.Info("delegation confirmed id=%s delegate_id=%s", item.ID, d.ID)
			return
		}
		r.delegations.Clear(item.Chat)
	}

	// Tugas berat: stepi duluan meringkas requirement dan meminta konfirmasi
	// bos. Tim teknis baru dikerjakan setelah bos balas konfirmasi.
	if item.Lane == LaneWork {
		r.proposeDelegation(ctx, item)
		return
	}
	// Ack pertama & follow-up hanya dikirim kalau task masih jalan (lihat
	// sendScheduled). Task yang cepat selesai cukup dapat jawaban langsung.
	taskCtx, cancelTask := context.WithCancel(ctx)
	defer cancelTask()

	// sendScheduled TIDAK boleh ikut context handleMessage, karena context itu
	// di-cancel begitu handleMessage return dan bisa memotong followup terakhir
	// pas reply final keburu dikirim. Pakai context yang dilepas (WithoutCancel),
	// lalu stop eksplisit setelah reply final terkirim sambil menunggu kiriman
	// yang sedang jalan selesai dulu.
	schedCtx, stopSchedule := context.WithCancel(context.WithoutCancel(ctx))
	scheduleDone := make(chan struct{})
	go func() {
		defer close(scheduleDone)
		r.sendScheduled(schedCtx, item)
	}()
	defer func() {
		stopSchedule()
		<-scheduleDone
	}()

	// Lampiran (foto/file) dari bos diunduh dulu supaya agent bisa membacanya.
	promptText := strings.TrimSpace(item.Text)
	if item.Media != nil && r.cfg.IncomingMediaEnabled {
		enriched, err := r.prepareMedia(item)
		if err != nil {
			r.log.Error("media download failed id=%s: %v", item.ID, err)
			if promptText == "" {
				r.markRead(item.Chat)
				r.sendChunked("(error: lampiran tidak bisa diunduh)")
				r.log.Info("task failed id=%s duration=%.1fs", item.ID, time.Since(start).Seconds())
				return
			}
			promptText += "\n\n(lampiran yang dikirim tidak bisa diunduh)"
		} else {
			promptText = enriched
		}
	}

	// Lanjutkan sesi opencode yang sama supaya agent ingat obrolan sebelumnya.
	// Sesi yang sudah basi (nganggur kelamaan, umur kelamaan, atau kebanyakan
	// pesan) di-rollover di sini: id-nya dilepas supaya pesan ini mulai sesi
	// baru. Sesi lama tidak dihapus dari DB, cuma dicatat di prev_id.
	sessionID := r.beginSession(item.Lane, time.Now())
	prompt := fmt.Sprintf(promptTemplate, promptText)
	stdout, stderr, usedID, err := r.runRecovering(taskCtx, r.runnerFor(item.Lane), item.Lane, sessionID, item.ID, prompt)
	if usedID == "" {
		r.saveSessionFromOutput(item.Lane, stdout)
	}
	if err != nil {
		r.log.Error("opencode failed: %v (stderr %dB)", err, len(stderr))
		r.markRead(item.Chat)
		detail := agentErrorDetail(stdout, stderr)
		note := errorNote(err, detail)
		if isQuotaExhausted(detail) {
			note = "(error: token habis)"
		}
		r.sendChunked(note)
		r.log.Info("task failed id=%s duration=%.1fs", item.ID, time.Since(start).Seconds())
		return
	}
	r.log.Info("opencode done duration=%.1fs stdout=%dB stderr=%dB", time.Since(start).Seconds(), len(stdout), len(stderr))
	reply := r.extractReply(stdout)
	r.markRead(item.Chat)
	r.sendChunked(reply)
	r.log.Info("task done id=%s reply_len=%d duration=%.1fs", item.ID, len(reply), time.Since(start).Seconds())
}

// helpText menyusun daftar perintah berdasarkan prefix yang aktif.
func helpText(cfg *Config) string {
	p := CommandPrefix(cfg)
	return fmt.Sprintf(
		"Perintah yang tersedia:\n\n"+
			"%shelp — tampilkan bantuan\n"+
			"%sping — cek relay hidup\n"+
			"%sreset — hapus sesi, mulai obrolan baru\n"+
			"%srestart — alias %sreset\n"+
			"%smodel — lihat/ganti model chat (contoh: %smodel 2)\n"+
			"%sworker — cek status worker build (alias %scek)\n\n"+
			"Perintah tidak peka huruf besar/kecil.",
		p, p, p, p, p, p, p, p, p,
	)
}

// handleCommand menjalankan perintah fast-path tanpa memanggil agent. Return
// true berarti pesan sudah ditangani (termasuk perintah tak dikenal).
func (r *Relay) handleCommand(item taskItem, start time.Time) bool {
	name, prefixed := commandName(r.cfg, item.Text)
	if !prefixed {
		switch {
		case strings.EqualFold(strings.TrimSpace(item.Text), "ping"):
			name = "ping"
		case isResetCommand(r.cfg, item.Text):
			name = "reset"
		default:
			return false
		}
	}

	// Alias ringkas: !model1 / !model2.
	if strings.HasPrefix(name, "model") && len(name) > len("model") {
		if n, err := strconv.Atoi(strings.TrimPrefix(name, "model")); err == nil {
			r.handleModelCommand(item, []string{strconv.Itoa(n)})
			r.log.Info("task done id=%s model duration=%.1fs", item.ID, time.Since(start).Seconds())
			return true
		}
	}

	switch name {
	case "help":
		r.markRead(item.Chat)
		r.sendChunked(helpText(r.cfg))
		r.log.Info("task done id=%s help duration=%.1fs", item.ID, time.Since(start).Seconds())
	case "ping":
		r.markRead(item.Chat)
		r.sendChunked("pong")
		r.log.Info("task done id=%s fast-path duration=%.1fs", item.ID, time.Since(start).Seconds())
	case "model":
		r.handleModelCommand(item, commandArgs(r.cfg, item.Text)[1:])
		r.log.Info("task done id=%s model duration=%.1fs", item.ID, time.Since(start).Seconds())
	case "worker", "workers", "cek":
		r.handleWorkerCommand(item)
		r.log.Info("task done id=%s worker duration=%.1fs", item.ID, time.Since(start).Seconds())
	case "reset", "restart":
		if r.session != nil {
			if err := r.session.Set(""); err != nil {
				r.log.Error("session reset failed: %v", err)
			}
		}
		if r.delegations != nil {
			r.delegations.Clear(item.Chat)
		}
		r.markRead(item.Chat)
		r.sendChunked("oke, kita mulai dari awal ya 😊")
		r.log.Info("task done id=%s reset duration=%.1fs", item.ID, time.Since(start).Seconds())
	default:
		p := CommandPrefix(r.cfg)
		r.markRead(item.Chat)
		r.sendChunked(fmt.Sprintf("perintah %s%s belum dikenal. Coba %shelp.", p, name, p))
		r.log.Info("task done id=%s unknown-command duration=%.1fs", item.ID, time.Since(start).Seconds())
	}
	return true
}

// currentChatModel mengembalikan model chat yang sedang dipakai runner.
func (r *Relay) currentChatModel() string {
	if getter, ok := r.runner.(interface{ CurrentModel() string }); ok {
		if m := getter.CurrentModel(); m != "" {
			return m
		}
	}
	return r.cfg.ChatModelName()
}

// setChatModel mengganti model chat pada runner (bila runner mendukung).
func (r *Relay) setChatModel(model string) bool {
	setter, ok := r.runner.(interface{ SetModel(string) })
	if !ok {
		return false
	}
	setter.SetModel(model)
	return true
}

// handleModelCommand menampilkan atau mengganti model chat tanpa memanggil
// agent. Contoh: "!model", "!model 2", "!model2", "!model opencode/..." .
func (r *Relay) handleModelCommand(item taskItem, args []string) {
	models := r.cfg.ChatModelList()
	current := r.currentChatModel()
	if len(models) == 0 {
		r.markRead(item.Chat)
		r.sendChunked("belum ada model chat yang dikonfigurasi.")
		return
	}

	if len(args) == 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "Model chat sekarang: %s\n\n", current)
		for i, m := range models {
			marker := ""
			if m == current {
				marker = " (aktif)"
			}
			fmt.Fprintf(&b, "%d. %s%s\n", i+1, m, marker)
		}
		b.WriteString("\nGanti: ")
		b.WriteString(CommandPrefix(r.cfg))
		b.WriteString("model <nomor/nama>")
		r.markRead(item.Chat)
		r.sendChunked(strings.TrimSpace(b.String()))
		return
	}

	choice := args[0]
	target := ""
	if n, err := strconv.Atoi(choice); err == nil {
		if n >= 1 && n <= len(models) {
			target = models[n-1]
		}
	} else {
		for _, m := range models {
			if strings.EqualFold(m, choice) {
				target = m
				break
			}
		}
	}
	if target == "" {
		r.markRead(item.Chat)
		r.sendChunked(fmt.Sprintf("model %q belum dikenal. Pilih 1-%d atau nama modelnya. Coba %smodel.", choice, len(models), CommandPrefix(r.cfg)))
		return
	}
	if !r.setChatModel(target) {
		r.markRead(item.Chat)
		r.sendChunked("gagal ganti model: runner tidak mendukung pergantian saat runtime.")
		return
	}
	r.markRead(item.Chat)
	r.sendChunked(fmt.Sprintf("oke, model chat sekarang: %s", target))
}

// handleWorkerCommand menampilkan status slot build tanpa memanggil agent.
// Contoh: "!worker", "!workers", atau "!cek".
func (r *Relay) handleWorkerCommand(item taskItem) {
	var slots []poolSlot
	if r.workPool != nil {
		slots = r.workPool.status()
	}
	r.markRead(item.Chat)
	if len(slots) == 0 {
		r.sendChunked("Worker build: belum ada slot yang dikonfigurasi.")
		return
	}

	busy := 0
	for _, s := range slots {
		if s.Busy {
			busy++
		}
	}
	free := len(slots) - busy

	var b strings.Builder
	fmt.Fprintf(&b, "Worker build: %d slot, %d aktif, %d bebas.\n", len(slots), busy, free)
	for i, s := range slots {
		state := "bebas"
		if s.Busy {
			state = "aktif"
		}
		name := s.Workdir
		if name == "" {
			name = fmt.Sprintf("slot %d", i+1)
		}
		fmt.Fprintf(&b, "%d. %s — %s\n", i+1, name, state)
	}
	if free == 0 {
		b.WriteString("Status: semua slot terpakai, tugas baru akan ditolak.")
	} else {
		b.WriteString("Status: siap menerima tugas.")
	}
	r.sendChunked(strings.TrimSpace(b.String()))
}

// proposeDelegation mengarahkan stepi untuk meringkas requirement tugas berat
// dan meminta konfirmasi bos sebelum delegasi ke tim teknis. Bila semua slot
// build sedang terpakai, tugas tidak diusulkan: stepi langsung mengabari bos
// bahwa tim sedang sibuk semua.
func (r *Relay) proposeDelegation(ctx context.Context, item taskItem) {
	if !r.workAvailable() {
		r.sendBusy(ctx, item)
		r.log.Info("delegation busy id=%s", item.ID)
		return
	}
	sessionID := r.beginSession(LaneChat, time.Now())
	prompt := fmt.Sprintf(delegationPromptTemplate, strings.TrimSpace(item.Text))
	stdout, _, usedID, err := r.runRecovering(ctx, r.runnerFor(LaneChat), LaneChat, sessionID, item.ID, prompt)
	if usedID == "" {
		r.saveSessionFromOutput(LaneChat, stdout)
	}
	if err != nil {
		r.log.Error("delegation proposal failed: %v", err)
		r.markRead(item.Chat)
		r.sendChunked(errorNote(err, ""))
		return
	}
	reply := r.extractReply(stdout)
	r.markRead(item.Chat)
	r.sendChunked(reply)
	if r.delegations != nil {
		r.delegations.Set(&Delegation{
			ID:        item.ID,
			Text:      item.Text,
			Chat:      item.Chat,
			CreatedAt: time.Now(),
		})
	}
	r.log.Info("delegation proposed id=%s", item.ID)
}

// handleHandover menangani perintah eksplisit bos untuk menyerahkan tugas ke
// tim teknis ("serahin aja ke Pak Budi"): kabari bos bahwa tugas diserahkan,
// lalu susun spesifikasi + jalankan build di latar belakang supaya worker chat
// tidak terblokir. Bila semua slot build terpakai, minta stepi mengabari bos.
func (r *Relay) handleHandover(ctx context.Context, item taskItem) {
	if !r.workAvailable() {
		r.sendBusy(ctx, item)
		r.log.Info("handover busy id=%s", item.ID)
		return
	}
	r.markRead(item.Chat)
	r.sendChunked(r.cfg.HandoverAckMessage())
	r.log.Info("handover accepted id=%s", item.ID)
	go r.runHandover(context.WithoutCancel(ctx), item)
}

// runHandover menyusun spesifikasi tugas dari konteks obrolan (stepi), lalu
// menyerahkannya ke pipeline delegasi (build + rangkuman). Fallback ke teks
// perintah bos bila stepi gagal merumuskan.
func (r *Relay) runHandover(ctx context.Context, item taskItem) {
	sessionID := r.beginSession(LaneChat, time.Now())
	prompt := fmt.Sprintf(handoverPromptTemplate, strings.TrimSpace(item.Text))
	stdout, _, usedID, err := r.runRecovering(ctx, r.runnerFor(LaneChat), LaneChat, sessionID, item.ID, prompt)
	if usedID == "" {
		r.saveSessionFromOutput(LaneChat, stdout)
	}
	if err != nil {
		r.log.Error("handover spec failed: %v", err)
	}
	spec := strings.TrimSpace(r.extractReply(stdout))
	if spec == "" {
		spec = strings.TrimSpace(item.Text)
	}
	d := &Delegation{ID: item.ID, Text: spec, Chat: item.Chat, CreatedAt: time.Now()}
	// Ack serah terima sudah dikirim handleHandover, jadi jangan jadwalkan ack
	// "udah aku baca pesanmu" lagi (dulu ini bikin pesan auto nyusul 5 menit
	// setelah stepi balas).
	r.runDelegation(ctx, item, d, false)
}

// sendBusy meminta stepi mengabari bos bahwa semua slot build sedang terpakai.
// Kalau stepi gagal, pesan cadangan tetap dikirim supaya bos tidak didiamkan.
func (r *Relay) sendBusy(ctx context.Context, item taskItem) {
	r.markRead(item.Chat)
	prompt := strings.TrimSpace(r.cfg.WorkBusyPrompt)
	if prompt == "" {
		prompt = busyPromptTemplate
	}
	sessionID := r.beginSession(LaneChat, time.Now())
	stdout, _, usedID, err := r.runRecovering(ctx, r.runnerFor(LaneChat), LaneChat, sessionID, item.ID, prompt)
	if usedID == "" {
		r.saveSessionFromOutput(LaneChat, stdout)
	}
	if err != nil {
		r.log.Error("busy notice failed: %v", err)
		r.sendChunked(busyFallback)
		return
	}
	reply := r.extractReply(stdout)
	if reply == "" {
		reply = busyFallback
	}
	r.sendChunked(reply)
}

// runDelegation menjalankan pipeline delegasi di background: agent teknis
// (build) mengerjakan tugas, lalu stepi merangkum hasilnya untuk bos. Jalankan
// dalam goroutine sendiri agar chat tidak terblokir sambil build berjalan.
// scheduleAck true menyalakan ack susulan "udah aku baca pesanmu" selama build;
// jalur serah terima (handover) sudah mengirim ack sendiri jadi harus false.
func (r *Relay) runDelegation(ctx context.Context, item taskItem, d *Delegation, scheduleAck bool) {
	// Pesan satu slot build. Bila dua-duanya sudah terpakai, jangan antre diam:
	// kabari bos lewat stepi bahwa tim lagi sibuk semua.
	buildRunner, releaseBuild, ok := r.acquireWork()
	if !ok {
		r.sendBusy(ctx, taskItem{ID: item.ID, Chat: d.Chat})
		r.log.Info("delegation busy at confirm id=%s delegate_id=%s", item.ID, d.ID)
		return
	}
	defer releaseBuild()

	if scheduleAck {
		schedCtx, stopSchedule := context.WithCancel(context.WithoutCancel(ctx))
		scheduleDone := make(chan struct{})
		go func() {
			defer close(scheduleDone)
			r.sendScheduled(schedCtx, item)
		}()
		defer func() {
			stopSchedule()
			<-scheduleDone
		}()
	}

	// 1. Agent teknis mengerjakan tugas (slot + sesi work terpisah).
	buildID := r.beginSession(LaneWork, time.Now())
	buildPrompt := fmt.Sprintf(buildPromptTemplate, d.Text)
	stdout, stderr, _, err := r.runRecovering(ctx, buildRunner, LaneWork, buildID, d.ID, buildPrompt)
	if err != nil {
		r.log.Error("delegation build failed: %v (stderr %dB)", err, len(stderr))
		r.markRead(d.Chat)
		r.notifyDelegationFailure(ctx, d, stdout, stderr, err)
		r.log.Info("delegation failed id=%s delegate_id=%s duration=%.1fs", item.ID, d.ID, time.Since(d.CreatedAt).Seconds())
		return
	}
	r.log.Info("delegation build done delegate_id=%s stdout=%dB", d.ID, len(stdout))

	// 2. Stepi merangkum hasil untuk bos. Pakai sesi sekali pakai (bukan sesi
	// chat): rangkuman tidak mengotori memori obrolan dan tidak memegang gate
	// sesi chat, jadi balasan chat tidak ikut menunggu rangkuman. Output build
	// dipotong sebagai jaring pengaman ukuran prompt.
	sumPrompt := fmt.Sprintf(summarizePromptTemplate, truncateBytes(stdout, summarizeMaxBytes))
	sumStdout, _, _, err := r.runRecovering(ctx, r.runnerFor(LaneChat), LaneChat, "", "summary-"+d.ID, sumPrompt)
	if err != nil {
		r.log.Error("delegation summarize failed: %v", err)
		r.markRead(d.Chat)
		r.sendChunked(errorNote(err, agentErrorDetail(sumStdout, "")))
		return
	}
	reply := r.extractReply(sumStdout)
	r.markRead(d.Chat)
	r.sendChunked(reply)
	r.log.Info("delegation done delegate_id=%s reply_len=%d duration=%.1fs", d.ID, len(reply), time.Since(d.CreatedAt).Seconds())
}

// notifyDelegationFailure menjelaskan kegagalan worker lewat stepi, memakai
// sesi chat supaya percakapan mengingat penyebabnya. Kalau stepi ikut gagal
// (mis. model chat kena limit), kirim detail mentah sebagai fallback.
func (r *Relay) notifyDelegationFailure(ctx context.Context, d *Delegation, stdout, stderr string, err error) {
	raw := agentErrorDetail(stdout, stderr)
	fallback := errorNote(err, raw)
	detail := raw
	if detail == "" {
		detail = fallback
	}

	prompt := fmt.Sprintf(failurePromptTemplate, truncateBytes(d.Text, summarizeMaxBytes), detail)
	sessionID := r.beginSession(LaneChat, time.Now())
	out, _, usedID, ferr := r.runRecovering(ctx, r.runnerFor(LaneChat), LaneChat, sessionID, "failure-"+d.ID, prompt)
	if usedID == "" {
		r.saveSessionFromOutput(LaneChat, out)
	}
	if ferr != nil {
		r.log.Error("delegation failure explain failed: %v", ferr)
		r.sendChunked(fallback)
		return
	}
	reply := r.extractReply(out)
	if strings.TrimSpace(reply) == "" {
		r.sendChunked(fallback)
		return
	}
	r.sendChunked(reply)
}

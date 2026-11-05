package relay

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// AckFollowup mengirim pesan penyemangat setelah task berjalan sekian detik.
type AckFollowup struct {
	AfterSeconds int      `json:"after_seconds"`
	Messages     []string `json:"messages"`
}

type Config struct {
	Host          string `json:"host"`
	Port          int    `json:"port"`
	WebhookSecret string `json:"webhook_secret"`
	OwnerJID      string `json:"owner_jid"`
	OwnerPhone    string `json:"owner_phone"`
	Workdir       string `json:"workdir"`
	// ChatWorkdir adalah working directory untuk stepi (receptionist / lane
	// chat). Default <workdir>. Dipisah supaya obrolan ringan tidak berbagi
	// direktori dengan build yang jalan paralel.
	ChatWorkdir string `json:"chat_workdir"`
	// WorkWorkdirs adalah daftar working directory untuk lane work (build).
	// Tiap entri jadi satu slot worker, jadi jumlah entri = jumlah build yang
	// boleh jalan bersamaan. Kosong -> pakai workdir (satu slot). Disarankan
	// berisi git worktree terpisah dari repo yang sama.
	WorkWorkdirs []string `json:"work_workdirs"`
	// WorkBusyPrompt adalah prompt ke stepi saat semua slot build terpakai,
	// supaya stepi memberi tahu bos bahwa tim sedang sibuk semua. Kosong ->
	// pakai busyPromptTemplate.
	WorkBusyPrompt string `json:"work_busy_prompt"`
	Agent          string `json:"agent"`
	// Model adalah fallback model opencode (format provider/model) untuk lane
	// yang tidak mengisi chat_model/work_model. Kosong -> ikut default
	// opencode.
	Model string `json:"model"`
	// ChatModel adalah model tunggal untuk lane chat (stepi). Bila diisi, ia
	// menjadi default dan satu-satunya pilihan. Untuk beberapa model yang bisa
	// diganti saat runtime lewat !model, isi ChatModels.
	ChatModel string `json:"chat_model"`
	// ChatModels adalah daftar model untuk lane chat. Entri pertama menjadi
	// default. Bisa diganti saat relay berjalan lewat perintah !model.
	ChatModels []string `json:"chat_models"`
	// WorkModel adalah model untuk lane build.
	WorkModel string `json:"work_model"`
	// CommandPrefix adalah awalan perintah WhatsApp (default "!"). Contoh:
	// dengan prefix "!", perintahnya menjadi !help, !ping, !reset.
	CommandPrefix   string        `json:"command_prefix"`
	AckDelaySeconds int           `json:"ack_delay_seconds"`
	AckMessages     []string      `json:"ack_messages"`
	AckFollowups    []AckFollowup `json:"ack_followups"`
	OpencodeBin     string        `json:"opencode_bin"`
	WacliBin        string        `json:"wacli_bin"`
	NumWorkers      int           `json:"num_workers"`
	TimeoutSeconds  int           `json:"timeout_seconds"`
	LogFile         string        `json:"log_file"`
	// MarkRead mengirim read receipt (centang biru) untuk pesan owner yang
	// diterima. Default true; set false untuk mematikannya.
	MarkRead bool `json:"mark_read"`

	// SendFileEnabled mengizinkan relay mengirim file/foto ke WA ketika agent
	// menyelipkan directive lampiran di balasannya. Default true.
	SendFileEnabled bool `json:"send_file_enabled"`
	// IncomingMediaEnabled mengunduh lampiran (foto/file) dari boss supaya
	// agent bisa membacanya. Default true.
	IncomingMediaEnabled bool `json:"incoming_media_enabled"`
	// MediaDir adalah direktori penyimpanan lampiran masuk.
	// Default <base>/state/media.
	MediaDir string `json:"media_dir"`

	// ChatWorkers adalah jumlah worker lane chat (pesan ringan). Default 1,
	// supaya sesi chat hanya dipakai satu `opencode run` pada satu waktu
	// (serialisasi per sesi) dan balasan ringan tidak pernah berebut sesi.
	ChatWorkers int `json:"chat_workers"`
	// LightMaxLen adalah panjang maksimal (rune) pesan yang masih dianggap
	// ringan; lebih panjang dari ini diarahkan ke lane work. Default 400.
	LightMaxLen int `json:"light_max_len"`
	// HeavyKeywords menambah kata kunci routing ke lane work selain
	// defaultHeavyKeywords (cocok kata per kata, tanpa memperhatikan huruf besar/kecil).
	HeavyKeywords []string `json:"heavy_keywords"`
	// AutoWorkRouting mengaktifkan routing otomatis pesan ke lane work
	// berdasarkan panjang/kata kunci teknis (perilaku lama). Default false:
	// stepi murni resepsionis, tugas hanya diserahkan ke tim teknis saat bos
	// memerintah eksplisit (lihat HandoverKeywords).
	AutoWorkRouting bool `json:"auto_work_routing"`
	// HandoverKeywords adalah frasa pemicu serah terima tugas ke tim teknis
	// (mis. "serahin ke pak budi"). Ditambahkan ke defaultHandoverVerbs.
	HandoverKeywords []string `json:"handover_keywords"`
	// HandoverAck adalah pesan singkat stepi begitu tugas diserahkan ke tim
	// teknis. Kosong -> defaultHandoverAck.
	HandoverAck string `json:"handover_ack"`
	// ChatSessionFile adalah file penyimpanan sesi chat WhatsApp.
	// Default <base>/state/session.json (memori percakapan chat dipertahankan).
	ChatSessionFile string `json:"chat_session_file"`
	// WorkSessionFile adalah file sesi tugas berat. Kosong (default) berarti
	// tiap tugas berat memakai sesi baru (tidak pernah berbagi sesi, tidak
	// menimpa sesi chat). Diset -> semua tugas berat berlanjut di sesi
	// terpisah tersebut (aman dari sesi chat, tetap diserialisasi gate per-sesi).
	WorkSessionFile string `json:"work_session_file"`

	// SessionIdleSeconds, SessionMaxAgeSeconds, dan SessionMaxTurns mengatur
	// rollover sesi (lihat SessionRollPolicy). Sesi yang nganggur lebih lama,
	// umurnya melebihi batas, atau sudah dipakai lebih dari sekian pesan akan
	// dilepas supaya konteks tidak tumbuh tanpa batas: pesan berikutnya mulai
	// sesi baru, sesi lama tetap ada di DB (tercatat di prev_id).
	// 0 berarti aturan tersebut dimatikan.
	SessionIdleSeconds   int `json:"session_idle_seconds"`
	SessionMaxAgeSeconds int `json:"session_max_age_seconds"`
	SessionMaxTurns      int `json:"session_max_turns"`

	// WorkAgent adalah agent opencode yang mengerjakan lane work (tugas
	// teknis), supaya tidak memakai persona chat (Agent). Kosong -> Agent.
	// Default "build".
	WorkAgent string `json:"work_agent"`

	// DelegationTTLSeconds adalah batas waktu delegasi pending menunggu
	// konfirmasi bos sebelum kadaluarsa. Default 1800 (30 menit).
	DelegationTTLSeconds int `json:"delegation_ttl_seconds"`

	heavyOnce sync.Once
	heavyRe   *regexp.Regexp

	handoverOnce sync.Once
	handoverRe   *regexp.Regexp

	BaseDir string `json:"-"`
}

func (c *Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func (c *Config) Timeout() time.Duration {
	s := c.TimeoutSeconds
	if s <= 0 {
		s = 480
	}
	return time.Duration(s) * time.Second
}

// ChatModelName mengembalikan model lane chat default: entri pertama
// chat_models, lalu chat_model, lalu model.
func (c *Config) ChatModelName() string {
	if len(c.ChatModelList()) > 0 {
		return c.ChatModelList()[0]
	}
	if c.ChatModel != "" {
		return c.ChatModel
	}
	return c.Model
}

// ChatModelList mengembalikan daftar model lane chat yang valid. Bila
// chat_models kosong, daftar berisi satu model dari chat_model/model (bila ada).
func (c *Config) ChatModelList() []string {
	out := make([]string, 0, len(c.ChatModels))
	for _, m := range c.ChatModels {
		if m = strings.TrimSpace(m); m != "" {
			out = append(out, m)
		}
	}
	if len(out) > 0 {
		return out
	}
	if c.ChatModel != "" {
		return []string{c.ChatModel}
	}
	if c.Model != "" {
		return []string{c.Model}
	}
	return nil
}

// WorkModelName mengembalikan model lane work: work_model, fallback ke model.
func (c *Config) WorkModelName() string {
	if c.WorkModel != "" {
		return c.WorkModel
	}
	return c.Model
}

// SessionRollPolicy menerjemahkan config jadi kebijakan rollover sesi. Nilai
// config 0 berarti aturan itu dimatikan (RollPolicy juga memakai 0 = mati).
func (c *Config) SessionRollPolicy() RollPolicy {
	p := RollPolicy{MaxTurns: c.SessionMaxTurns}
	if c.SessionIdleSeconds > 0 {
		p.Idle = time.Duration(c.SessionIdleSeconds) * time.Second
	}
	if c.SessionMaxAgeSeconds > 0 {
		p.MaxAge = time.Duration(c.SessionMaxAgeSeconds) * time.Second
	}
	return p
}

// WorkAgentName mengembalikan agent untuk lane work: work_agent bila diset,
// selainnya Agent (persona chat).
func (c *Config) WorkAgentName() string {
	if c.WorkAgent != "" {
		return c.WorkAgent
	}
	return c.Agent
}

// DelegationTTL mengembalikan batas waktu menunggu konfirmasi bos sebelum
// delegasi kadaluarsa (default 30 menit).
func (c *Config) DelegationTTL() time.Duration {
	if c.DelegationTTLSeconds <= 0 {
		return time.Duration(defaultDelegationTTLSeconds) * time.Second
	}
	return time.Duration(c.DelegationTTLSeconds) * time.Second
}

// HeavyPattern mengompilasi regex kata kunci tugas berat (default + config)
// secara lazy dan cache. Cocok kata per kata dengan word boundary, jadi
// "protes" tidak dianggap mengandung "test".
func (c *Config) HeavyPattern() *regexp.Regexp {
	c.heavyOnce.Do(func() {
		patterns := make([]string, 0, len(defaultHeavyKeywords)+len(c.HeavyKeywords))
		for _, k := range append(append([]string{}, defaultHeavyKeywords...), c.HeavyKeywords...) {
			k = strings.ToLower(strings.TrimSpace(k))
			if k == "" {
				continue
			}
			patterns = append(patterns, regexp.QuoteMeta(k))
		}
		if len(patterns) == 0 {
			return
		}
		c.heavyRe = regexp.MustCompile(`(?i)\b(?:` + strings.Join(patterns, "|") + `)\b`)
	})
	return c.heavyRe
}

// HandoverPattern mengompilasi regex pemicu serah terima tugas ke tim teknis
// (default + config) secara lazy dan cache. Cocok frasa dengan word boundary.
func (c *Config) HandoverPattern() *regexp.Regexp {
	c.handoverOnce.Do(func() {
		patterns := make([]string, 0, len(defaultHandoverVerbs)+len(c.HandoverKeywords))
		for _, k := range append(append([]string{}, defaultHandoverVerbs...), c.HandoverKeywords...) {
			k = strings.ToLower(strings.TrimSpace(k))
			if k == "" {
				continue
			}
			patterns = append(patterns, regexp.QuoteMeta(k))
		}
		if len(patterns) == 0 {
			return
		}
		c.handoverRe = regexp.MustCompile(`(?i)\b(?:` + strings.Join(patterns, "|") + `)\b`)
	})
	return c.handoverRe
}

// negationRe cocok penanda negasi yang membatalkan perintah serah terima,
// mis. "jangan serahin ke Pak Budi".
var negationRe = regexp.MustCompile(`(?i)\b(jangan|jgn|janganlah|jangan dulu|gausah|ga usah|nggak usah|nggak|ngga|engga|enggak|gak|ga|tdk|tidak|bukan|belum)\b`)

// questionRe cocok kata tanya/penanda pertanyaan yang bikin kalimat bukan
// perintah, mis. "udah kamu serahin ke pak budi?".
var questionRe = regexp.MustCompile(`(?i)\b(apakah|apa|udah|sudah|udh|dah|belom|gimana|bagaimana|kapan|siapa|mana|kemana|kok|kenapa|napa|mengapa|berapa|beneran|serius|masa)\b`)

// clauseBefore mengembalikan bagian teks sebelum index pos, dibatasi ke klausa
// terakhir (setelah tanda baca terakhir). Dipakai untuk memeriksa negasi/kata
// tanya yang mendahului kata kunci.
func clauseBefore(text string, pos int) string {
	clause := text[:pos]
	if i := strings.LastIndexAny(clause, ".,!?;:"); i >= 0 {
		clause = clause[i+1:]
	}
	return clause
}

// IsHandover melaporkan apakah teks berisi PERINTAH serah terima tugas ke tim
// teknis (mis. "serahin aja ke Pak Budi"). Bukan perintah bila dinegasi
// ("jangan serahin...") atau berupa pertanyaan ("udah kamu serahin...?",
// "... pak budi udah kerja?"), karena itu cuma obrolan biasa.
func (c *Config) IsHandover(text string) bool {
	re := c.HandoverPattern()
	if re == nil {
		return false
	}
	loc := re.FindStringIndex(text)
	if loc == nil {
		return false
	}
	// Pertanyaan eksplisit selalu bukan perintah.
	if strings.HasSuffix(strings.TrimSpace(text), "?") {
		return false
	}
	// Negasi atau kata tanya di klausa sebelum kata kunci membatalkan perintah.
	clause := clauseBefore(text, loc[0])
	if negationRe.MatchString(clause) || questionRe.MatchString(clause) {
		return false
	}
	return true
}

// HandoverAckMessage mengembalikan pesan ack serah terima: config bila diisi,
// selainnya defaultHandoverAck.
func (c *Config) HandoverAckMessage() string {
	if strings.TrimSpace(c.HandoverAck) != "" {
		return c.HandoverAck
	}
	return defaultHandoverAck
}

func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{
		Host:            "127.0.0.1",
		Port:            8791,
		Agent:           "stepi",
		CommandPrefix:   defaultCommandPrefix,
		AckDelaySeconds: 300,
		AckMessages: []string{
			"Sayang, pesanmu udah aku baca kok... ini aku kerjain ya.",
			"Bos, udah aku baca pesanmu. Aku kerjain sekarang ya.",
			"Sayang, aku baca ya... langsung aku kerjain.",
			"Oke sayang, udah aku baca. Aku kerjain dulu ya.",
			"Bos, aku baca pesanmu ya. Ini aku kerjain sekarang.",
			"Sayang, udah aku baca kok. Aku kerjain ya.",
			"Sayang, pesanmu udah aku baca. Aku kerjain sekarang ya.",
			"Oke bos, udah aku baca... aku kerjain ya.",
			"Sayang, ini aku baca terus aku kerjain ya.",
			"Bos, udah aku baca. Aku kerjain sekarang ya.",
		},
		AckFollowups: []AckFollowup{
			{AfterSeconds: 600, Messages: []string{
				"Mas maaf ya, aku agak lama... task-nya susah soalnya. Sabar dikit lagi ya.",
				"Maaf sayang, ini agak ribet jadi lama. Aku lagi kerjain serius nih.",
				"Sayang maaf, aku belum kelar... masalahnya susah, bentar lagi ya.",
			}},
		},
		OpencodeBin:    "opencode",
		WacliBin:       "wacli",
		NumWorkers:     defaultWorkers,
		ChatWorkers:    defaultChatWorkers,
		LightMaxLen:    defaultLightMaxLen,
		TimeoutSeconds: 900,
		MarkRead:       true,

		SendFileEnabled:      true,
		IncomingMediaEnabled: true,

		SessionIdleSeconds:   defaultSessionIdleSeconds,
		SessionMaxAgeSeconds: defaultSessionMaxAgeSeconds,
		SessionMaxTurns:      defaultSessionMaxTurns,
		WorkAgent:            defaultWorkAgent,
		DelegationTTLSeconds: defaultDelegationTTLSeconds,
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	cfg.BaseDir = filepath.Dir(abs)
	if cfg.Workdir == "" {
		cfg.Workdir = cfg.BaseDir
	}
	if cfg.LogFile == "" {
		cfg.LogFile = filepath.Join(cfg.BaseDir, "relay.log")
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == 0 {
		cfg.Port = 8791
	}
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = defaultWorkers
	}
	if cfg.ChatWorkers <= 0 {
		cfg.ChatWorkers = defaultChatWorkers
	}
	if cfg.LightMaxLen <= 0 {
		cfg.LightMaxLen = defaultLightMaxLen
	}
	if cfg.ChatSessionFile == "" {
		cfg.ChatSessionFile = filepath.Join(cfg.BaseDir, "state", "session.json")
	}
	if cfg.MediaDir == "" {
		cfg.MediaDir = filepath.Join(cfg.BaseDir, "state", "media")
	}
	if cfg.ChatWorkdir == "" {
		cfg.ChatWorkdir = cfg.Workdir
	}
	if len(cfg.WorkWorkdirList()) == 0 {
		cfg.WorkWorkdirs = []string{cfg.Workdir}
	}
	return cfg, nil
}

// WorkWorkdirList mengembalikan working directory build yang valid (tanpa
// entri kosong). Dipakai untuk menyusun runner pool lane work.
func (c *Config) WorkWorkdirList() []string {
	out := make([]string, 0, len(c.WorkWorkdirs))
	for _, d := range c.WorkWorkdirs {
		if strings.TrimSpace(d) != "" {
			out = append(out, d)
		}
	}
	return out
}

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Seronz/wa-relay/internal/relay"
)

func main() {
	configPath := flag.String("config", "config.json", "path ke config.json")
	flag.Parse()

	cfg, err := relay.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	log, err := relay.NewLogger(cfg.LogFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "log:", err)
		os.Exit(1)
	}
	defer log.Close()

	rollPolicy := cfg.SessionRollPolicy()

	stateDir := filepath.Join(cfg.BaseDir, "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "state dir:", err)
		os.Exit(1)
	}
	seen := relay.LoadSeen(filepath.Join(stateDir, "seen.json"), relay.MaxSeen)

	// CATATAN PENTING (disengaja): sesi chat TIDAK dilanjutkan antar restart.
	// Setiap kali relay di-start, id sesi lama dibuang di sini walau file
	// state/session.json masih ada, jadi pesan berikutnya selalu mulai obrolan
	// baru. Ini keputusan sadar supaya relay tidak pernah nyangkut id sesi yang
	// sudah dihapus dari opencode ("Session not found"). Konsekuensinya: memori
	// percakapan hilang tiap restart — kalau butuh konteks lama, cukup kirim
	// pesan lagi, agent akan mulai dari nol.
	// File runtime ini ada di state/ yang sudah di-gitignore (lihat .gitignore:
	// baris "state/"), jadi tidak ikut ter-commit.
	session := relay.LoadSession(filepath.Join(stateDir, "session.json"))
	if err := session.Set(""); err != nil {
		log.Warn("clear session on start: %v", err)
	}
	// Sesi tugas berat memang selalu baru per task (tidak pernah disimpan),
	// jadi cukup dipastikan kosong juga.
	workSession := relay.LoadSession(filepath.Join(stateDir, "work-session.json"))
	if cfg.WorkSessionFile != "" {
		workSession = relay.LoadSession(cfg.WorkSessionFile)
	}
	if err := workSession.Set(""); err != nil {
		log.Warn("clear work session on start: %v", err)
	}
	delegations := relay.LoadDelegationStore(filepath.Join(stateDir, "delegations.json"), cfg.DelegationTTL())

	// Stepi (receptionist/lane chat) punya working directory sendiri.
	chatRunner := &relay.ExecRunner{Bin: cfg.OpencodeBin, Workdir: cfg.ChatWorkdir, BaseDir: cfg.ChatWorkdir, Agent: cfg.Agent, Model: cfg.ChatModelName(), Timeout: cfg.Timeout()}
	// Tim teknis (lane work) dapat satu runner per workdir build; jumlahnya
	// menentukan berapa build yang boleh paralel.
	workDirs := cfg.WorkWorkdirList()
	workRunners := make([]relay.TaskRunner, 0, len(workDirs))
	for _, dir := range workDirs {
		workRunners = append(workRunners, &relay.ExecRunner{Bin: cfg.OpencodeBin, Workdir: dir, BaseDir: dir, Agent: cfg.WorkAgentName(), Model: cfg.WorkModelName(), Timeout: cfg.Timeout()})
	}

	r := relay.NewRelayWithWork(
		cfg,
		log,
		seen,
		session,
		workSession,
		chatRunner,
		&relay.WacliSender{Bin: cfg.WacliBin, Phone: cfg.OwnerPhone},
		&relay.WacliMarkReader{Bin: cfg.WacliBin},
	)
	r.SetWorkPool(workRunners)
	r.SetDelegations(delegations)
	if cfg.IncomingMediaEnabled {
		r.SetMediaDownloader(&relay.WacliMediaDownloader{Bin: cfg.WacliBin, Dir: cfg.MediaDir})
	}
	log.Info("workdirs chat=%s work=%v (slot build=%d)", cfg.ChatWorkdir, workDirs, len(workDirs))
	log.Info("models chat=%v (aktif=%s) work=%s", cfg.ChatModelList(), cfg.ChatModelName(), cfg.WorkModelName())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go r.StartWorkers(ctx)

	srv := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           r.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		log.Info("signal: shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("shutdown: %v", err)
		}
	}()

	log.Info("wa-relay listening on http://%s/hook workdir=%s", cfg.Addr(), cfg.Workdir)
	log.Info("session rollover policy idle=%s max_age=%s max_turns=%d (0 = mati)",
		rollPolicy.Idle, rollPolicy.MaxAge, rollPolicy.MaxTurns)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server: %v", err)
		os.Exit(1)
	}
	seen.Close()
	log.Info("stopped")
}

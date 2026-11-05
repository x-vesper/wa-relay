package relay

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var testRollPolicy = RollPolicy{Idle: 24 * time.Hour, MaxAge: 7 * 24 * time.Hour, MaxTurns: 60}

func writeSessionFile(t *testing.T, path string, st State) {
	t.Helper()
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSessionBeginContinuesLiveSession(t *testing.T) {
	s := LoadSession(filepath.Join(t.TempDir(), "session.json"))
	if err := s.Set("ses_a"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	id, reason := s.Begin(now, testRollPolicy)
	if id != "ses_a" || reason != "" {
		t.Fatalf("Begin = (%q, %q), want (ses_a, \"\")", id, reason)
	}
	st := s.Snapshot()
	if st.Turns != 2 {
		t.Fatalf("turns = %d, want 2 (1 dari Set + 1 dari Begin)", st.Turns)
	}
	if !st.LastUsed.Equal(now) {
		t.Fatalf("last_used = %v, want %v", st.LastUsed, now)
	}
}

func TestSessionBeginRollsOverWhenIdle(t *testing.T) {
	s := LoadSession(filepath.Join(t.TempDir(), "session.json"))
	now := time.Now()
	writeSessionFile(t, s.Path(), State{
		ID:        "ses_old",
		StartedAt: now.Add(-2 * time.Hour),
		LastUsed:  now.Add(-25 * time.Hour),
		Turns:     5,
	})
	s = LoadSession(s.Path())

	id, reason := s.Begin(now, testRollPolicy)
	if id != "" || reason != "idle" {
		t.Fatalf("Begin = (%q, %q), want (\"\", idle)", id, reason)
	}
	st := s.Snapshot()
	if st.ID != "" || st.Turns != 0 {
		t.Fatalf("state = %+v, want id kosong & turns 0", st)
	}
	if st.PrevID != "ses_old" || st.Rolled != 1 {
		t.Fatalf("prev_id/rolled = %q/%d, want ses_old/1", st.PrevID, st.Rolled)
	}
	// Rollover harus ikut tertulis ke disk (biar tetap kena walau relay restart).
	persisted := LoadSession(s.Path()).Snapshot()
	if persisted.ID != "" || persisted.PrevID != "ses_old" {
		t.Fatalf("state tersimpan = %+v, want id kosong + prev_id ses_old", persisted)
	}
}

func TestSessionBeginRolloverReasons(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		st   State
		want string
	}{
		{
			name: "umur melebihi max age",
			st:   State{ID: "ses_x", StartedAt: now.Add(-8 * 24 * time.Hour), LastUsed: now.Add(-time.Minute), Turns: 2},
			want: "age",
		},
		{
			name: "turn sudah mentok",
			st:   State{ID: "ses_x", StartedAt: now.Add(-time.Hour), LastUsed: now.Add(-time.Minute), Turns: 60},
			want: "turns",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := LoadSession(filepath.Join(t.TempDir(), "session.json"))
			writeSessionFile(t, s.Path(), tc.st)
			s = LoadSession(s.Path())
			id, reason := s.Begin(now, testRollPolicy)
			if id != "" || reason != tc.want {
				t.Fatalf("Begin = (%q, %q), want (\"\", %q)", id, reason, tc.want)
			}
		})
	}
}

func TestSessionBeginKeepsSessionWhenPolicyDisabled(t *testing.T) {
	now := time.Now()
	s := LoadSession(filepath.Join(t.TempDir(), "session.json"))
	writeSessionFile(t, s.Path(), State{
		ID:        "ses_old",
		StartedAt: now.Add(-90 * 24 * time.Hour),
		LastUsed:  now.Add(-30 * 24 * time.Hour),
		Turns:     999,
	})
	s = LoadSession(s.Path())

	id, reason := s.Begin(now, RollPolicy{})
	if id != "ses_old" || reason != "" {
		t.Fatalf("Begin = (%q, %q), want (ses_old, \"\")", id, reason)
	}
}

// File sesi lama (cuma berisi {"id":...}) harus tetap lanjut dipakai, bukan
// dianggap basi hanya karena belum punya metadata umur.
func TestSessionBeginBackwardCompatibleOldFile(t *testing.T) {
	s := LoadSession(filepath.Join(t.TempDir(), "session.json"))
	if err := os.WriteFile(s.Path(), []byte(`{"id":"ses_legacy"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s = LoadSession(s.Path())

	now := time.Now()
	id, reason := s.Begin(now, testRollPolicy)
	if id != "ses_legacy" || reason != "" {
		t.Fatalf("Begin = (%q, %q), want (ses_legacy, \"\")", id, reason)
	}
	st := s.Snapshot()
	if st.StartedAt.IsZero() || !st.LastUsed.Equal(now) {
		t.Fatalf("metadata tidak diisi untuk file lama: %+v", st)
	}
}

func TestSessionSetEmptyResetsMetadata(t *testing.T) {
	s := LoadSession(filepath.Join(t.TempDir(), "session.json"))
	if err := s.Set("ses_a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(""); err != nil {
		t.Fatal(err)
	}
	st := s.Snapshot()
	if st.ID != "" || st.Turns != 0 || !st.StartedAt.IsZero() {
		t.Fatalf("state setelah /reset = %+v, want kosong", st)
	}
}

// missingSessionRunner meniru opencode yang menolak id sesi tak dikenal
// ("Session not found") tapi sukses saat diminta membuat sesi baru.
type missingSessionRunner struct {
	mu       sync.Mutex
	sessions []string
	stdout   string
}

func (m *missingSessionRunner) Run(_ context.Context, _ string, sessionID string) (string, string, error) {
	m.mu.Lock()
	m.sessions = append(m.sessions, sessionID)
	stdout := m.stdout
	m.mu.Unlock()
	if sessionID != "" {
		return "", "Error: Session not found", errors.New("exit status 1")
	}
	return stdout, "", nil
}

func (m *missingSessionRunner) used() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.sessions...)
}

// Sesi yang sudah dihapus dari DB tidak boleh bikin relay macet permanen:
// percobaan pertama gagal "Session not found", relay melepas pointer lalu
// mengulang dengan sesi baru dan membalas normal.
func TestRelayRecoversFromMissingSession(t *testing.T) {
	cfg := testConfig(t)
	runner := &missingSessionRunner{stdout: `{"type":"step_start","sessionID":"ses_new"}` + "\n" +
		`{"type":"text","part":{"text":"halo"}}`}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)

	writeSessionFile(t, r.session.Path(), State{
		ID:        "ses_dead",
		StartedAt: time.Now().Add(-time.Hour),
		LastUsed:  time.Now().Add(-time.Minute),
		Turns:     2,
	})
	r.session = LoadSession(r.session.Path())

	r.handleMessage(context.Background(), taskItem{ID: "m1", Text: "halo", Chat: "6281@s.whatsapp.net"})

	if got := runner.used(); len(got) != 2 || got[0] != "ses_dead" || got[1] != "" {
		t.Fatalf("sesi yang dipakai = %v, want [ses_dead \"\"]", got)
	}
	if st := r.session.Snapshot(); st.ID != "ses_new" {
		t.Fatalf("id sesi = %q, want ses_new", st.ID)
	}
	if msgs := sender.messages(); len(msgs) == 0 || msgs[len(msgs)-1] != "halo" {
		t.Fatalf("balasan terkirim = %v, want halo", msgs)
	}
}

// handleMessage harus melepas sesi chat yang sudah basi: run pertama sesudahnya
// memakai sesi baru (sessionID kosong), lalu id barunya disimpan.
func TestRelayRollsOverStaleChatSession(t *testing.T) {
	cfg := testConfig(t)
	cfg.SessionIdleSeconds = int((24 * time.Hour).Seconds())
	sender := &stubSender{}
	runner := &stubRunner{stdout: `{"type":"step_start","sessionID":"ses_new"}` + "\n" +
		`{"type":"text","part":{"text":"halo"}}`}
	r := newTestRelay(t, cfg, runner, sender)

	writeSessionFile(t, r.session.Path(), State{
		ID:        "ses_old",
		StartedAt: time.Now().Add(-25 * time.Hour),
		LastUsed:  time.Now().Add(-25 * time.Hour),
		Turns:     4,
	})
	r.session = LoadSession(r.session.Path())

	r.handleMessage(context.Background(), taskItem{ID: "m1", Text: "halo", Chat: "6281@s.whatsapp.net"})

	if len(runner.sessions) != 1 || runner.sessions[0] != "" {
		t.Fatalf("sesi yang dipakai = %v, want [\"\"] (sesi baru)", runner.sessions)
	}
	st := r.session.Snapshot()
	if st.ID != "ses_new" {
		t.Fatalf("id sesi = %q, want ses_new", st.ID)
	}
	if st.PrevID != "ses_old" {
		t.Fatalf("prev_id = %q, want ses_old", st.PrevID)
	}
	if len(sender.sent) == 0 || sender.sent[len(sender.sent)-1] != "halo" {
		t.Fatalf("balasan terkirim = %v, want halo", sender.sent)
	}
}

package relay

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func testConfig(t *testing.T) *Config {
	t.Helper()
	base := t.TempDir()
	return &Config{
		Host:           "127.0.0.1",
		Port:           8791,
		WebhookSecret:  "topsecret",
		OwnerJID:       "6281234567890@s.whatsapp.net",
		OwnerPhone:     "6281234567890",
		Workdir:        base,
		OpencodeBin:    "opencode",
		WacliBin:       "wacli",
		TimeoutSeconds: 1,
		LogFile:        filepath.Join(base, "relay.log"),
		BaseDir:        base,
	}
}

func newTestRelay(t *testing.T, cfg *Config, runner TaskRunner, sender Sender) *Relay {
	t.Helper()
	log, err := newLogger(io.Discard, filepath.Join(t.TempDir(), "test.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(log.Close)
	seen := LoadSeen(filepath.Join(t.TempDir(), "seen.json"), MaxSeen)
	t.Cleanup(seen.Close)
	return &Relay{
		cfg:         cfg,
		log:         log,
		seen:        seen,
		session:     LoadSession(filepath.Join(t.TempDir(), "session.json")),
		workSession: LoadSession(filepath.Join(t.TempDir(), "work-session.json")),
		chatQueue:   make(chan taskItem, 16),
		workQueue:   make(chan taskItem, 16),
		queue:       make(chan taskItem, 16),
		runner:      runner,
		delegations: LoadDelegationStore(filepath.Join(t.TempDir(), "delegations.json"), time.Minute),
		sender:      sender,
	}
}

type stubSender struct {
	mu    sync.Mutex
	sent  []string
	files []string
	fail  bool
	rec   func(event string)
}

func (s *stubSender) Send(text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, text)
	if s.rec != nil {
		s.rec("send:" + text)
	}
	if s.fail {
		return errors.New("send failed")
	}
	return nil
}

func (s *stubSender) SendFile(path, caption string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files = append(s.files, path)
	if s.rec != nil {
		s.rec("sendfile:" + path)
	}
	if s.fail {
		return errors.New("send failed")
	}
	return nil
}

func (s *stubSender) reset() {
	s.mu.Lock()
	s.sent = nil
	s.files = nil
	s.mu.Unlock()
}

func (s *stubSender) messages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

func (s *stubSender) filesSent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.files...)
}

type stubMarkReader struct {
	mu    sync.Mutex
	chats []string
	err   error
	rec   func(event string)
}

func (s *stubMarkReader) MarkRead(chat string) error {
	s.mu.Lock()
	s.chats = append(s.chats, chat)
	err := s.err
	rec := s.rec
	s.mu.Unlock()
	if rec != nil {
		rec("mark-read:" + chat)
	}
	return err
}

func (s *stubMarkReader) calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.chats...)
}

// eventLog mencatat urutan peristiwa lintas stub (runner, mark-read, sender)
// supaya urutan mark-read vs balasan final bisa diverifikasi deterministik.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (e *eventLog) add(ev string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, ev)
}

func (e *eventLog) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

func assertEvents(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

// waitEvent menunggu satu peristiwa terekam di log (untuk sinkronisasi
// dengan goroutine handleMessage yang berjalan paralel dengan test).
func waitEvent(t *testing.T, e *eventLog, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, ev := range e.list() {
			if ev == want {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("event %q tidak tercatat: %v", want, e.list())
}

type stubRunner struct {
	mu       sync.Mutex
	stdout   string
	stderr   string
	err      error
	calls    int
	prompt   string
	delay    time.Duration
	sessions []string
	model    string
	rec      func(event string)
}

func (s *stubRunner) SetModel(model string) {
	s.mu.Lock()
	s.model = model
	s.mu.Unlock()
}

func (s *stubRunner) CurrentModel() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.model
}

func (s *stubRunner) Run(_ context.Context, prompt, sessionID string) (string, string, error) {
	s.mu.Lock()
	s.calls++
	s.prompt = prompt
	s.sessions = append(s.sessions, sessionID)
	stdout, stderr, err, delay, rec := s.stdout, s.stderr, s.err, s.delay, s.rec
	s.mu.Unlock()
	if rec != nil {
		rec("runner-start")
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	if rec != nil {
		rec("runner-done")
	}
	return stdout, stderr, err
}

// seqRunner mengembalikan stdout berbeda per panggilan (call ke-1, ke-2, dst),
// supaya bisa membedakan langkah-langkah dalam satu pipeline.
type seqRunner struct {
	mu      sync.Mutex
	outs    []string
	calls   int
	prompts []string
}

func (s *seqRunner) Run(_ context.Context, prompt, _ string) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.calls
	s.calls++
	s.prompts = append(s.prompts, prompt)
	if i >= len(s.outs) {
		i = len(s.outs) - 1
	}
	if i < 0 {
		return "", "", nil
	}
	return s.outs[i], "", nil
}

func (s *seqRunner) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// poolRunner mencatat berapa banyak Run() yang jalan bersamaan, supaya bisa
// membuktikan worker pool benar-benar paralel.
type poolRunner struct {
	mu        sync.Mutex
	active    int
	maxActive int
	release   chan struct{}
	once      sync.Once
}

func (p *poolRunner) Run(context.Context, string, string) (string, string, error) {
	p.mu.Lock()
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	reached := p.active >= defaultWorkers
	p.mu.Unlock()
	if reached {
		p.once.Do(func() { close(p.release) })
	}
	<-p.release
	p.mu.Lock()
	p.active--
	p.mu.Unlock()
	return `{"type":"text","part":{"text":"ok"}}`, "", nil
}

func signBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func postHook(t *testing.T, r *Relay, body []byte, sig string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/hook", bytes.NewReader(body))
	if sig != "" {
		req.Header.Set("X-Wacli-Signature", sig)
	}
	rec := httptest.NewRecorder()
	r.Routes().ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("invalid json response %q: %v", rec.Body.String(), err)
	}
	return out
}

// hookItem mem-post payload ke /hook dan mengembalikan item yang masuk
// antrean (atau gagal bila tidak ada).
func hookItem(t *testing.T, r *Relay, cfg *Config, payload map[string]any) taskItem {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	rec := postHook(t, r, body, signBody(cfg.WebhookSecret, body))
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	if got := decodeBody(t, rec)["queued"]; got != true {
		t.Fatalf("queued = %v, want true", got)
	}
	select {
	case item := <-r.queue:
		return item
	case item := <-r.chatQueue:
		return item
	case item := <-r.workQueue:
		return item
	default:
		t.Fatal("expected an item in queue")
		return taskItem{}
	}
}

func TestVerifySignature(t *testing.T) {
	secret := "s3cret"
	body := []byte(`{"Chat":"x"}`)
	valid := signBody(secret, body)

	cases := []struct {
		name   string
		secret string
		body   []byte
		header string
		want   bool
	}{
		{"good", secret, body, valid, true},
		{"good without prefix", secret, body, strings.TrimPrefix(valid, "sha256="), true},
		{"good uppercase hex", secret, body, "sha256=" + strings.ToUpper(strings.TrimPrefix(valid, "sha256=")), true},
		{"bad digest", secret, body, "sha256=deadbeef", false},
		{"wrong secret", "other", body, valid, false},
		{"changed body", secret, []byte(`{"Chat":"y"}`), valid, false},
		{"missing header", secret, body, "", false},
		{"empty secret skips check", "", body, "", true},
		{"empty secret ignores bad header", "", body, "sha256=nonsense", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := verifySignature(tc.secret, tc.body, tc.header); got != tc.want {
				t.Fatalf("verifySignature() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHookRejectsBadSignature(t *testing.T) {
	cfg := testConfig(t)
	r := newTestRelay(t, cfg, &stubRunner{}, &stubSender{})
	body := []byte(`{"Chat":"6281234567890@s.whatsapp.net","ID":"m1","SenderJID":"6281234567890@s.whatsapp.net","Text":"hi"}`)

	rec := postHook(t, r, body, "sha256=deadbeef")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401", rec.Code)
	}
	if got := decodeBody(t, rec)["error"]; got != "bad signature" {
		t.Fatalf("error = %v, want bad signature", got)
	}

	if rec := postHook(t, r, body, ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing signature code = %d, want 401", rec.Code)
	}
	if rec := postHook(t, r, []byte(`{`), signBody(cfg.WebhookSecret, []byte(`{`))); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid json code = %d, want 400", rec.Code)
	}
}

func validPayload() map[string]any {
	return map[string]any{
		"Chat":      "6281234567890@s.whatsapp.net",
		"ID":        "MSG1",
		"SenderJID": "6281234567890@s.whatsapp.net",
		"Timestamp": "2026-10-15T10:00:00Z",
		"FromMe":    false,
		"Text":      "halo",
		"ChatName":  "Owner",
		"Media":     nil,
	}
}

func TestHookIgnoreFilters(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(map[string]any)
		clearOwner bool
		want       string
	}{
		{"event receipt", func(m map[string]any) { m["EventType"] = "receipt" }, false, "event"},
		{"event presence", func(m map[string]any) { m["EventType"] = "chat_presence" }, false, "event"},
		{"from me", func(m map[string]any) { m["FromMe"] = true }, false, "fromme"},
		{"not owner", func(m map[string]any) { m["SenderJID"] = "6289999999999@s.whatsapp.net" }, false, "not-owner"},
		{"group chat", func(m map[string]any) { m["Chat"] = "1234-5678@g.us"; m["SenderJID"] = "1234-5678@g.us" }, true, "group/status"},
		{"newsletter", func(m map[string]any) { m["Chat"] = "1234@newsletter"; m["SenderJID"] = "1234@newsletter" }, true, "group/status"},
		{"status broadcast", func(m map[string]any) { m["Chat"] = "status@broadcast"; m["SenderJID"] = "status@broadcast" }, true, "group/status"},
		{"sender is group", func(m map[string]any) { m["SenderJID"] = "9999@g.us" }, true, "group/status"},
		{"empty text", func(m map[string]any) { m["Text"] = "" }, false, "empty"},
		{"whitespace text", func(m map[string]any) { m["Text"] = "   \n\t " }, false, "empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			if tc.clearOwner {
				cfg.OwnerJID = ""
			}
			r := newTestRelay(t, cfg, &stubRunner{}, &stubSender{})
			payload := validPayload()
			tc.mutate(payload)
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			rec := postHook(t, r, body, signBody(cfg.WebhookSecret, body))
			if rec.Code != http.StatusOK {
				t.Fatalf("code = %d, want 200", rec.Code)
			}
			if got := decodeBody(t, rec)["ignored"]; got != tc.want {
				t.Fatalf("ignored = %v, want %v", got, tc.want)
			}
			if len(r.queue) != 0 || len(r.chatQueue) != 0 || len(r.workQueue) != 0 {
				t.Fatal("nothing should be queued")
			}
		})
	}
}

func TestHookQueuesValidMessageAndDedupes(t *testing.T) {
	cfg := testConfig(t)
	r := newTestRelay(t, cfg, &stubRunner{}, &stubSender{})
	payload := validPayload()
	payload["Text"] = "  halo dunia  "
	payload["EventType"] = "message"
	body, _ := json.Marshal(payload)

	rec := postHook(t, r, body, signBody(cfg.WebhookSecret, body))
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	got := decodeBody(t, rec)
	if got["queued"] != true || got["ok"] != true {
		t.Fatalf("response = %v, want queued true", got)
	}
	var item taskItem
	select {
	case item = <-r.queue:
	case item = <-r.chatQueue:
	case item = <-r.workQueue:
	default:
		t.Fatal("expected an item in queue")
	}
	if item.ID != "MSG1" || item.Text != "halo dunia" {
		t.Fatalf("queued item = %+v", item)
	}

	rec = postHook(t, r, body, signBody(cfg.WebhookSecret, body))
	if got := decodeBody(t, rec)["ignored"]; got != "duplicate" {
		t.Fatalf("second post ignored = %v, want duplicate", got)
	}
}

func TestHealthEndpoint(t *testing.T) {
	r := newTestRelay(t, testConfig(t), &stubRunner{}, &stubSender{})
	rec := httptest.NewRecorder()
	r.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	if got := decodeBody(t, rec)["ok"]; got != true {
		t.Fatalf("ok = %v, want true", got)
	}

	rec = httptest.NewRecorder()
	r.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("not found code = %d, want 404", rec.Code)
	}
}

func TestSeenStorePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seen.json")
	s := LoadSeen(path, MaxSeen)
	defer s.Close()
	if s.Seen("m1") {
		t.Fatal("m1 should be new")
	}
	if !s.Seen("m1") {
		t.Fatal("m1 should be duplicate")
	}
	if s.Seen("m2") {
		t.Fatal("m2 should be new")
	}

	// Tiap pesan masuk tidak boleh langsung menulis file (debounce); baru
	// ditulis saat flush berkala / Flush() dipanggil.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("seen file sudah tertulis sebelum flush, err=%v", err)
	}
	s.Flush()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("seen file not written: %v", err)
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		t.Fatalf("seen file invalid: %v", err)
	}
	if len(ids) != 2 || ids[0] != "m1" || ids[1] != "m2" {
		t.Fatalf("ids = %v, want [m1 m2]", ids)
	}

	reloaded := LoadSeen(path, MaxSeen)
	defer reloaded.Close()
	if !reloaded.Seen("m1") {
		t.Fatal("m1 should still be duplicate after reload")
	}
	if reloaded.Seen("m3") {
		t.Fatal("m3 should be new")
	}
}

func TestSeenStoreKeepsLast500(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seen.json")
	s := LoadSeen(path, MaxSeen)
	defer s.Close()
	for i := 0; i < 600; i++ {
		s.Seen(fmt.Sprintf("x%d", i))
	}
	s.Flush()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		t.Fatal(err)
	}
	if len(ids) != MaxSeen {
		t.Fatalf("persisted len = %d, want %d", len(ids), MaxSeen)
	}
	if ids[0] != "x100" || ids[len(ids)-1] != "x599" {
		t.Fatalf("window = [%s..%s], want [x100..x599]", ids[0], ids[len(ids)-1])
	}
	if len(s.ids) != MaxSeen {
		t.Fatalf("memory len = %d, want %d", len(s.ids), MaxSeen)
	}
	if !s.Seen("x599") || s.Seen("x600") {
		t.Fatal("dedupe window incorrect after eviction")
	}
}

func TestChunkText(t *testing.T) {
	cases := []struct {
		name string
		text string
		want []int
	}{
		{"short", "abc", []int{3}},
		{"exact", strings.Repeat("a", 3000), []int{3000}},
		{"one over", strings.Repeat("a", 3001), []int{3000, 1}},
		{"7000", strings.Repeat("a", 7000), []int{3000, 3000, 1000}},
		{"empty", "", []int{8}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chunks := chunkText(tc.text, defaultChunk)
			if len(chunks) != len(tc.want) {
				t.Fatalf("got %d chunks %v, want %v", len(chunks), chunks, tc.want)
			}
			for i, want := range tc.want {
				if len([]rune(chunks[i])) != want {
					t.Fatalf("chunk %d len = %d, want %d", i, len([]rune(chunks[i])), want)
				}
			}
		})
	}
	if got := chunkText("", defaultChunk); len(got) != 1 || got[0] != "(kosong)" {
		t.Fatalf("empty text = %v, want [(kosong)]", got)
	}
	// Multi-byte runes must not be split.
	chunks := chunkText(strings.Repeat("é", 3001), defaultChunk)
	if len(chunks) != 2 || len([]rune(chunks[0])) != 3000 || chunks[1] != "é" {
		t.Fatalf("multi-byte chunks = %d (%d runes first)", len(chunks), len([]rune(chunks[0])))
	}
}

func TestExtractReply(t *testing.T) {
	cases := []struct {
		name   string
		stdout string
		want   string
	}{
		{"real format", `{"type":"text","part":{"text":"OK"}}`, "OK"},
		{
			"multiple text parts with noise",
			"{\"type\":\"step-start\",\"part\":{}}\n" +
				"{\"type\":\"text\",\"part\":{\"text\":\"baris satu\"}}\n" +
				"{\"type\":\"tool\",\"part\":{\"text\":\"abaikan\"}}\n" +
				"{\"type\":\"text\",\"part\":{\"text\":\"baris dua\"}}\n",
			"baris satu\nbaris dua",
		},
		{"fallback text field", `{"msg":"x","text":"jawaban cadangan"}`, "jawaban cadangan"},
		{"fallback nested text field", `{"data":{"text":"nested"}}`, "nested"},
		{"messy fallback", "bukan json\n\n   \njawaban terakhir  \n", "jawaban terakhir"},
		{"empty", "", ""},
		{"only invalid json", "{not json}\nstill not json\n", "still not json"},
		{"text type without part text falls back", `{"type":"text","part":{}}` + "\n" + `{"type":"log","text":"lain"}`, "lain"},
		{"thinking inline", `{"type":"text","part":{"text":"<thinking>plan</thinking>Halo sayang"}}`, "Halo sayang"},
		{
			"thinking-only part then reply",
			`{"type":"text","part":{"text":"reasoning panjang</thinking>"}}` + "\n" +
				`{"type":"text","part":{"text":"<thinking>lagi</thinking>jawaban asli"}}`,
			"jawaban asli",
		},
		{"no thinking unchanged", `{"type":"text","part":{"text":"biasa aja"}}`, "biasa aja"},
		{"unterminated thinking", `{"type":"text","part":{"text":"<thinking>belum kelar"}}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isStepi := strings.Contains(tc.name, "thinking")
			if got := extractReply(tc.stdout, isStepi); got != tc.want {
				t.Fatalf("extractReply() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestWorkerPingFastPath(t *testing.T) {
	runner := &stubRunner{}
	sender := &stubSender{}
	r := newTestRelay(t, testConfig(t), runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "1", Text: "  PiNg "})
	if runner.calls != 0 {
		t.Fatalf("runner calls = %d, want 0", runner.calls)
	}
	got := sender.messages()
	if len(got) != 1 || got[0] != "pong" {
		t.Fatalf("sent = %v, want [pong]", got)
	}
}

func TestWorkerPoolRunsInParallel(t *testing.T) {
	cfg := testConfig(t)
	cfg.NumWorkers = 3
	runner := &poolRunner{release: make(chan struct{})}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.StartWorkers(ctx)

	for i := 0; i < cfg.NumWorkers; i++ {
		r.queue <- taskItem{ID: fmt.Sprintf("p%d", i), Text: "halo"}
	}

	deadline := time.After(3 * time.Second)
	for len(sender.messages()) < cfg.NumWorkers {
		select {
		case <-deadline:
			t.Fatalf("cuma %d tugas selesai, worker pool tidak paralel", len(sender.messages()))
		case <-time.After(20 * time.Millisecond):
		}
	}
	runner.mu.Lock()
	max := runner.maxActive
	runner.mu.Unlock()
	if max < cfg.NumWorkers {
		t.Fatalf("max paralel = %d, mau >= %d (worker cuma satu?)", max, cfg.NumWorkers)
	}
}

func TestWorkerFastTaskSendsNoAck(t *testing.T) {
	cfg := testConfig(t)
	cfg.AckDelaySeconds = 300
	cfg.AckMessages = []string{"lagi diproses"}
	runner := &stubRunner{stdout: `{"type":"text","part":{"text":"jawaban"}}`}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "a", Text: "halo"})
	got := sender.messages()
	if len(got) != 1 || got[0] != "jawaban" {
		t.Fatalf("sent = %v, want [jawaban] (task cepat tidak boleh kirim ack)", got)
	}
}

type recordingRunner struct {
	mu sync.Mutex
	n  int
}

func (r *recordingRunner) Run(context.Context, string, string) (string, string, error) {
	r.mu.Lock()
	r.n++
	r.mu.Unlock()
	return `{"type":"text","part":{"text":"ok"}}`, "", nil
}

func (r *recordingRunner) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

func TestWorkLaneUsesWorkRunner(t *testing.T) {
	cfg := testConfig(t)
	chatRunner := &recordingRunner{}
	workRunner := &recordingRunner{}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, chatRunner, sender)
	r.SetWorkRunner(workRunner)
	chat := "6281234567890@s.whatsapp.net"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Proposal pakai chat runner (stepi), bukan work runner.
	r.handleMessage(ctx, taskItem{ID: "w1", Text: "coding", Lane: LaneWork, Chat: chat})
	deadline := time.After(time.Second)
	for chatRunner.count()+workRunner.count() < 1 {
		select {
		case <-deadline:
			t.Fatalf("runner tidak dipanggil chat=%d work=%d", chatRunner.count(), workRunner.count())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if chatRunner.count() != 1 || workRunner.count() != 0 {
		t.Fatalf("chat=%d work=%d, mau chat=1 work=0 (proposal lewat stepi)", chatRunner.count(), workRunner.count())
	}

	// Konfirmasi bos -> work runner dipakai (build + rangkuman lewat stepi).
	sender.sent = nil
	r.handleMessage(ctx, taskItem{ID: "c1", Text: "setuju", Lane: LaneChat, Chat: chat})
	deadline = time.After(time.Second)
	for chatRunner.count()+workRunner.count() < 3 {
		select {
		case <-deadline:
			t.Fatalf("delegasi tidak dieksekusi chat=%d work=%d", chatRunner.count(), workRunner.count())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if chatRunner.count() != 2 || workRunner.count() != 1 {
		t.Fatalf("chat=%d work=%d, mau chat=2 work=1", chatRunner.count(), workRunner.count())
	}
}

func TestClassifyRoutingToggle(t *testing.T) {
	cfg := testConfig(t)
	heavy := "tolong fix bug di server lalu deploy"
	if got := Classify(cfg, heavy); got != LaneChat {
		t.Fatalf("Classify default = %s, mau chat (routing otomatis mati)", got)
	}
	cfg.AutoWorkRouting = true
	if got := Classify(cfg, heavy); got != LaneWork {
		t.Fatalf("Classify auto = %s, mau work", got)
	}
}

func TestIsHandover(t *testing.T) {
	cfg := testConfig(t)
	yes := []string{"serahin aja ke pak budi", "tolong serahkan ke tim teknis", "delegasikan ke tim build", "jangan lupa ya, serahin ke pak budi", "serah ke pak budi dong"}
	for _, s := range yes {
		if !cfg.IsHandover(s) {
			t.Fatalf("IsHandover(%q) = false, mau true", s)
		}
	}
	no := []string{
		"halo apa kabar", "tolong fix bug ini", "makasih ya",
		// negasi
		"jangan serahin ke pak budi", "ga usah delegasikan ke tim teknis", "jangan serahin aja tugasnya ke pak budi",
		// pertanyaan (jangan memicu serah terima baru)
		"tepi pak budi udah kerja?", "udah kamu serahin ke pak budi?", "tep udah kamu serahin ke pak budi",
		"pak budi udah kerja", "kamu sudah serahkan ke tim teknis belum?",
	}
	for _, s := range no {
		if cfg.IsHandover(s) {
			t.Fatalf("IsHandover(%q) = true, mau false", s)
		}
	}
}

func TestHandoverKeTimTeknis(t *testing.T) {
	cfg := testConfig(t)
	chatRunner := &seqRunner{outs: []string{
		`{"type":"text","part":{"text":"spesifikasi tugas dari stepi"}}`,
		`{"type":"text","part":{"text":"rangkuman selesai untuk bos"}}`,
	}}
	workRunner := &stubRunner{stdout: `{"type":"text","part":{"text":"build ok"}}`}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, chatRunner, sender)
	r.SetWorkRunner(workRunner)
	chat := "6281234567890@s.whatsapp.net"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Perintah serah terima: stepi ack langsung, tugas diteruskan.
	r.handleMessage(ctx, taskItem{ID: "h1", Text: "serahin aja tugasnya ke pak budi", Lane: LaneChat, Chat: chat})
	if msgs := sender.messages(); len(msgs) == 0 || msgs[0] != cfg.HandoverAckMessage() {
		t.Fatalf("ack = %v, mau diawali %q", msgs, cfg.HandoverAckMessage())
	}

	// Build (work) + rangkuman (chat) jalan di latar belakang.
	deadline := time.After(2 * time.Second)
	for chatRunner.count() < 2 || workRunner.calls < 1 {
		select {
		case <-deadline:
			t.Fatalf("handover tidak selesai: chat=%d work=%d", chatRunner.count(), workRunner.calls)
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Build menerima spesifikasi hasil rumusan stepi, bukan perintah mentah.
	workRunner.mu.Lock()
	workPrompt := workRunner.prompt
	workRunner.mu.Unlock()
	if !strings.Contains(workPrompt, "spesifikasi tugas dari stepi") {
		t.Fatalf("prompt build = %q, mau memuat spesifikasi stepi", workPrompt)
	}
	// Rangkuman akhir terkirim ke bos.
	msgs := sender.messages()
	if last := msgs[len(msgs)-1]; !strings.Contains(last, "rangkuman selesai untuk bos") {
		t.Fatalf("balasan akhir = %q, mau rangkuman build", last)
	}
}

func TestHandoverTidakKirimAckSusulan(t *testing.T) {
	cfg := testConfig(t)
	// Ack delay pendek supaya (kalau bug) ack susulan keburu terkirim saat
	// build yang lambat masih jalan.
	cfg.AckDelaySeconds = 1
	cfg.AckMessages = []string{"lagi diproses"}
	chatRunner := &seqRunner{outs: []string{
		`{"type":"text","part":{"text":"spesifikasi tugas"}}`,
		`{"type":"text","part":{"text":"rangkuman selesai"}}`,
	}}
	workRunner := &stubRunner{stdout: `{"type":"text","part":{"text":"build ok"}}`, delay: 1500 * time.Millisecond}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, chatRunner, sender)
	r.SetWorkRunner(workRunner)
	chat := "6281234567890@s.whatsapp.net"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.handleMessage(ctx, taskItem{ID: "h1", Text: "serahin aja tugasnya ke pak budi", Lane: LaneChat, Chat: chat})

	deadline := time.After(5 * time.Second)
	for {
		msgs := sender.messages()
		if len(msgs) > 0 && strings.Contains(msgs[len(msgs)-1], "rangkuman selesai") {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("handover tidak selesai: msgs=%v", sender.messages())
		case <-time.After(20 * time.Millisecond):
		}
	}
	// Beri waktu: kalau scheduler ack keliru menyala, di sini bakal nongol.
	time.Sleep(500 * time.Millisecond)
	for _, m := range sender.messages() {
		if m == "lagi diproses" {
			t.Fatalf("ack susulan ikut terkirim padahal handover sudah ack: %v", sender.messages())
		}
	}
}

func TestWorkHeavyTaskDoesNotBlockChat(t *testing.T) {
	cfg := testConfig(t)
	cfg.NumWorkers = 1
	cfg.ChatWorkers = 1
	chatRunner := &stubRunner{stdout: `{"type":"text","part":{"text":"pong"}}`}
	workRunner := &poolRunner{release: make(chan struct{})}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, chatRunner, sender)
	r.SetWorkRunner(workRunner)
	chat := "6281234567890@s.whatsapp.net"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.StartWorkers(ctx)

	// Tugas berat: proposal cepat (chat runner), tidak nunggu build.
	r.workQueue <- taskItem{ID: "w1", Text: "buat kode", Lane: LaneWork, Chat: chat}
	deadline := time.After(2 * time.Second)
	for len(sender.messages()) < 1 {
		select {
		case <-deadline:
			t.Fatal("proposal lambat")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if len(sender.messages()) != 1 || sender.messages()[0] != "pong" {
		t.Fatalf("sent = %v, want [pong]", sender.messages())
	}

	// Konfirmasi -> build diblokir (poolRunner), tapi chat masih balas cepat.
	// Sambil build diblokir belum boleh ada balasan baru.
	sender.sent = nil
	r.handleMessage(ctx, taskItem{ID: "c2", Text: "setuju", Lane: LaneChat, Chat: chat})
	if len(sender.messages()) != 0 {
		t.Fatalf("build masih diblokir tapi sudah ada balasan: %v", sender.messages())
	}
	r.chatQueue <- taskItem{ID: "c3", Text: "halo", Lane: LaneChat, Chat: chat}
	deadline = time.After(2 * time.Second)
	for len(sender.messages()) < 1 {
		select {
		case <-deadline:
			t.Fatal("chat diblokir sambil build jalan")
		case <-time.After(10 * time.Millisecond):
		}
	}
	got := sender.messages()
	if len(got) != 1 || got[0] != "pong" {
		t.Fatalf("sent = %v, want [pong]", got)
	}
	close(workRunner.release)
}

type echoRunner struct {
	mu    sync.Mutex
	calls []string
}

func (e *echoRunner) Run(_ context.Context, prompt string, _ string) (string, string, error) {
	e.mu.Lock()
	e.calls = append(e.calls, prompt)
	n := len(e.calls)
	e.mu.Unlock()
	switch n {
	case 1:
		return `{"type":"text","part":{"text":"noted bos, aku rangkumin requirement: 1, 2, dst. Aku delegasikan tugas ini ke tim teknis, bos setuju?"}}`, "", nil
	case 2:
		return `{"type":"text","part":{"text":"kode sudah selesai dikerjakan tim teknis"}}`, "", nil
	case 3:
		return `{"type":"text","part":{"text":"siap bos, fitur X sudah selesai dan siap dipakai"}}`, "", nil
	}
	return `{"type":"text","part":{"text":"ok"}}`, "", nil
}

func (e *echoRunner) nth(i int) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if i < 0 || i >= len(e.calls) {
		return ""
	}
	return e.calls[i]
}

func TestDelegationDikonfirmasiBos(t *testing.T) {
	cfg := testConfig(t)
	runner := &echoRunner{}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)
	r.SetWorkRunner(runner)
	chat := "6281234567890@s.whatsapp.net"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Tugas berat: stepi usulkan delegasi + minta konfirmasi.
	r.handleMessage(ctx, taskItem{ID: "w1", Text: "buat fitur X", Lane: LaneWork, Chat: chat})
	if got := sender.messages(); len(got) != 1 || !strings.Contains(got[0], "bos setuju") {
		t.Fatalf("proposal = %v, ingin ada konfirmasi ke bos", got)
	}

	// 2. Bos setuju: build + rangkuman di latar belakang.
	sender.sent = nil
	r.handleMessage(ctx, taskItem{ID: "c1", Text: "setuju", Lane: LaneChat, Chat: chat})

	deadline := time.After(2 * time.Second)
	for len(sender.messages()) < 1 {
		select {
		case <-deadline:
			t.Fatal("delegasi tidak dieksekusi setelah konfirmasi")
		case <-time.After(10 * time.Millisecond):
		}
	}
	final := sender.messages()
	if len(final) != 1 || !strings.Contains(final[0], "selesai") {
		t.Fatalf("final = %v, ingin rangkuman selesai", final)
	}
	if got := runner.nth(0); !strings.Contains(got, "Ringkaskan requirement") {
		t.Fatalf("panggilan ke-1 bukan proposal: %q", got)
	}
	if got := runner.nth(1); !strings.Contains(got, "Selesaikan sebaik-baiknya") {
		t.Fatalf("panggilan ke-2 bukan build: %q", got)
	}
	if got := runner.nth(2); !strings.Contains(got, "Rangkum hasilnya") {
		t.Fatalf("panggilan ke-3 bukan rangkuman: %q", got)
	}
}

// TestDelegationSummarizePakaiSesiBaru memastikan langkah rangkuman tidak
// memakai sesi chat (sessionID harus ""), supaya rangkuman tidak mengunci sesi
// chat dan balasan obrolan tidak ikut menunggu.
func TestDelegationSummarizePakaiSesiBaru(t *testing.T) {
	cfg := testConfig(t)
	chatRunner := &stubRunner{stdout: `{"type":"text","sessionID":"ses_chat","part":{"text":"ok bos"}}`}
	workRunner := &stubRunner{stdout: `{"type":"text","sessionID":"ses_build","part":{"text":"build selesai"}}`}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, chatRunner, sender)
	r.SetWorkRunner(workRunner)
	chat := "6281234567890@s.whatsapp.net"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Proposal (chat) lalu konfirmasi -> build (work) + rangkuman (chat, sesi baru).
	r.handleMessage(ctx, taskItem{ID: "w1", Text: "buat fitur X", Lane: LaneWork, Chat: chat})
	r.handleMessage(ctx, taskItem{ID: "c1", Text: "setuju", Lane: LaneChat, Chat: chat})

	deadline := time.After(2 * time.Second)
	for {
		chatRunner.mu.Lock()
		n := chatRunner.calls
		chatRunner.mu.Unlock()
		if n >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("rangkuman tidak jalan")
		case <-time.After(10 * time.Millisecond):
		}
	}

	chatRunner.mu.Lock()
	sessions := append([]string(nil), chatRunner.sessions...)
	chatRunner.mu.Unlock()
	if sessions[1] != "" {
		t.Fatalf("rangkuman pakai sesi %q, mau sesi baru (\"\") agar tidak mengunci sesi chat", sessions[1])
	}
}

func TestDelegationFailureExplainedByStepi(t *testing.T) {
	cfg := testConfig(t)
	chatRunner := &stubRunner{stdout: `{"type":"text","sessionID":"ses_chat","part":{"text":"Worker kena limit OpenCode. Coba lagi nanti ya."}}`}
	workRunner := &stubRunner{
		stdout: `{"type":"error","error":{"type":"provider.quota","message":"Free usage limit reached"}}`,
		err:    errors.New("exit status 1"),
	}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, chatRunner, sender)
	r.SetWorkPool([]TaskRunner{workRunner})
	if err := r.session.Set("ses_existing"); err != nil {
		t.Fatal(err)
	}

	d := &Delegation{ID: "w1", Text: "fix login", Chat: "chat", CreatedAt: time.Now()}
	r.runDelegation(context.Background(), taskItem{ID: "c1", Chat: "chat"}, d, false)

	got := sender.messages()
	if len(got) != 1 || !strings.Contains(got[0], "limit OpenCode") {
		t.Fatalf("notifikasi = %v, want penjelasan stepi", got)
	}
	chatRunner.mu.Lock()
	prompt := chatRunner.prompt
	calls := chatRunner.calls
	sessions := append([]string(nil), chatRunner.sessions...)
	chatRunner.mu.Unlock()
	if calls != 1 {
		t.Fatalf("chat calls = %d, want 1 (stepi menjelaskan kegagalan)", calls)
	}
	for _, want := range []string{"Free usage limit reached", "fix login"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt stepi = %q, tidak memuat %q", prompt, want)
		}
	}
	if len(sessions) != 1 || sessions[0] != "ses_existing" {
		t.Fatalf("sesi stepi = %v, want sesi chat yang sudah ada", sessions)
	}
}

func TestDelegationBukanKonfirmasiDibatalkan(t *testing.T) {
	cfg := testConfig(t)
	runner := &echoRunner{}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)
	r.SetWorkRunner(runner)
	chat := "6281234567890@s.whatsapp.net"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.handleMessage(ctx, taskItem{ID: "w1", Text: "buat fitur X", Lane: LaneWork, Chat: chat})

	// Balasan non-konfirmasi sambil pending: batalin delegasi, obrolan biasa.
	r.handleMessage(ctx, taskItem{ID: "c2", Text: "halo apa kabar", Lane: LaneChat, Chat: chat})

	// Tugas berat baru: usul lagi, jangan langsung build.
	r.handleMessage(ctx, taskItem{ID: "w3", Text: "buat fitur Y", Lane: LaneWork, Chat: chat})

	if got := runner.nth(0); !strings.Contains(got, "Ringkaskan requirement") {
		t.Fatalf("panggilan ke-1 bukan proposal: %q", got)
	}
	if got := runner.nth(1); !strings.Contains(got, "Balas sebagai dirimu sendiri") {
		t.Fatalf("panggilan ke-2 bukan chat biasa: %q", got)
	}
	if got := runner.nth(2); !strings.Contains(got, "Ringkaskan requirement") {
		t.Fatalf("panggilan ke-3 bukan proposal: %q", got)
	}
	for i := 0; i < len(runner.calls); i++ {
		if strings.Contains(runner.calls[i], "Selesaikan sebaik-baiknya") {
			t.Fatalf("build seharusnya belum dijalankan: panggilan %d = %q", i+1, runner.calls[i])
		}
	}
}

func TestWorkerAckSentOnlyWhenSlow(t *testing.T) {
	cfg := testConfig(t)
	cfg.AckDelaySeconds = 1
	cfg.AckMessages = []string{"lagi diproses"}
	runner := &stubRunner{stdout: `{"type":"text","part":{"text":"jawaban"}}`, delay: 1500 * time.Millisecond}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "b", Text: "halo"})
	got := sender.messages()
	if len(got) != 2 || got[0] != "lagi diproses" || got[1] != "jawaban" {
		t.Fatalf("sent = %v, want [lagi diproses jawaban]", got)
	}
}

func TestWorkerPingSkipsAck(t *testing.T) {
	cfg := testConfig(t)
	cfg.AckDelaySeconds = 1
	cfg.AckMessages = []string{"lagi diproses"}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, &stubRunner{}, sender)

	r.handleMessage(context.Background(), taskItem{ID: "p", Text: "ping"})
	got := sender.messages()
	if len(got) != 1 || got[0] != "pong" {
		t.Fatalf("sent = %v, want [pong]", got)
	}
}

func TestPickRandom(t *testing.T) {
	if got := pickRandom(nil); got != "" {
		t.Fatalf("pickRandom(nil) = %q, want empty", got)
	}
	if got := pickRandom([]string{"  ", ""}); got != "" {
		t.Fatalf("pickRandom(blank) = %q, want empty", got)
	}
	if got := pickRandom([]string{"only"}); got != "only" {
		t.Fatalf("pickRandom(single) = %q, want only", got)
	}
	pool := []string{"a", "b", "c"}
	for i := 0; i < 20; i++ {
		if got := pickRandom(pool); got != "a" && got != "b" && got != "c" {
			t.Fatalf("pickRandom out of pool: %q", got)
		}
	}
	// Kosong di tengah pool harus dilewati.
	if got := pickRandom([]string{"", "x", "  "}); got != "x" {
		t.Fatalf("pickRandom skips blank = %q, want x", got)
	}
}

func TestSendScheduledFiresAckThenStopsOnCancel(t *testing.T) {
	cfg := testConfig(t)
	cfg.AckDelaySeconds = 1
	cfg.AckMessages = []string{"ack"}
	cfg.AckFollowups = []AckFollowup{{AfterSeconds: 2, Messages: []string{"masih diproses"}}}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, &stubRunner{}, sender)

	r.sendScheduled(context.Background(), taskItem{ID: "s"})
	got := sender.messages()
	if len(got) != 2 || got[0] != "ack" || got[1] != "masih diproses" {
		t.Fatalf("sent = %v, want [ack masih diproses]", got)
	}

	// Jika context sudah dibatalkan, ack/follow-up tidak boleh terkirim.
	sender2 := &stubSender{}
	r2 := newTestRelay(t, cfg, &stubRunner{}, sender2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r2.sendScheduled(ctx, taskItem{ID: "s2"})
	if got := sender2.messages(); len(got) != 0 {
		t.Fatalf("sent = %v, want none after cancel", got)
	}
}

func TestSendScheduledSkipsNonPositive(t *testing.T) {
	cfg := testConfig(t)
	// ack_delay 0 => tidak ada ack; follow-up 0/negatif dilewati.
	cfg.AckDelaySeconds = 0
	cfg.AckMessages = []string{"ack"}
	cfg.AckFollowups = []AckFollowup{
		{AfterSeconds: 0, Messages: []string{"nope"}},
		{AfterSeconds: -5, Messages: []string{"nope2"}},
	}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, &stubRunner{}, sender)
	r.sendScheduled(context.Background(), taskItem{ID: "s"})
	if got := sender.messages(); len(got) != 0 {
		t.Fatalf("sent = %v, want none", got)
	}
}

func TestHandleMessageStopsScheduleAfterReply(t *testing.T) {
	cfg := testConfig(t)
	cfg.AckDelaySeconds = 1
	cfg.AckMessages = []string{"ack"}
	cfg.AckFollowups = []AckFollowup{{AfterSeconds: 2, Messages: []string{"followup"}}}
	runner := &stubRunner{stdout: `{"type":"text","part":{"text":"jawaban"}}`, delay: 1200 * time.Millisecond}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "s", Text: "halo"})
	// Task kelar ~1.2s: ack (1s) sudah terkirim, followup (2s) tidak boleh nyusul
	// setelah reply final terkirim.
	got := sender.messages()
	if len(got) != 2 || got[0] != "ack" || got[1] != "jawaban" {
		t.Fatalf("sent = %v, want [ack jawaban]", got)
	}
	time.Sleep(1500 * time.Millisecond)
	if got := sender.messages(); len(got) != 2 {
		t.Fatalf("followup nyusul setelah reply final: %v", got)
	}
}

func TestWorkerResetClearsSession(t *testing.T) {
	for _, in := range []string{" !reset ", "!restart", " !RESTART ", "/reset"} {
		t.Run(in, func(t *testing.T) {
			sender := &stubSender{}
			runner := &stubRunner{stdout: `{"type":"text","part":{"text":"jawaban"}}`}
			r := newTestRelay(t, testConfig(t), runner, sender)
			if err := r.session.Set("ses_old"); err != nil {
				t.Fatal(err)
			}

			r.handleMessage(context.Background(), taskItem{ID: "r1", Text: in})
			if runner.calls != 0 {
				t.Fatalf("runner calls = %d, want 0 (reset tidak memanggil agent)", runner.calls)
			}
			if got := r.session.ID(); got != "" {
				t.Fatalf("session id = %q, want empty", got)
			}
			msgs := sender.messages()
			if len(msgs) != 1 || !strings.Contains(msgs[0], "mulai") {
				t.Fatalf("sent = %v, want a reset confirmation", msgs)
			}
		})
	}
}

func TestWorkerHelpListsCommands(t *testing.T) {
	sender := &stubSender{}
	runner := &stubRunner{}
	r := newTestRelay(t, testConfig(t), runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "h", Text: " !HELP "})
	if runner.calls != 0 {
		t.Fatalf("runner calls = %d, want 0 (help tidak memanggil agent)", runner.calls)
	}
	got := sender.messages()
	if len(got) != 1 {
		t.Fatalf("sent = %d messages, want 1", len(got))
	}
	for _, want := range []string{"!help", "!ping", "!reset", "!restart", "!model", "!worker", "!cek"} {
		if !strings.Contains(got[0], want) {
			t.Fatalf("help = %q, tidak memuat %q", got[0], want)
		}
	}
}

func TestWorkerUnknownCommandDoesNotCallAgent(t *testing.T) {
	sender := &stubSender{}
	runner := &stubRunner{}
	r := newTestRelay(t, testConfig(t), runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "u", Text: "!ngawur"})
	if runner.calls != 0 {
		t.Fatalf("runner calls = %d, want 0", runner.calls)
	}
	got := sender.messages()
	if len(got) != 1 || !strings.Contains(got[0], "belum dikenal") || !strings.Contains(got[0], "!help") {
		t.Fatalf("sent = %v, want petunjuk !help", got)
	}
}

func TestWorkerCustomCommandPrefix(t *testing.T) {
	cfg := testConfig(t)
	cfg.CommandPrefix = "#"
	sender := &stubSender{}
	runner := &stubRunner{}
	r := newTestRelay(t, cfg, runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "c", Text: "#help"})
	if runner.calls != 0 {
		t.Fatalf("runner calls = %d, want 0", runner.calls)
	}
	got := sender.messages()
	if len(got) != 1 || !strings.Contains(got[0], "#help") || strings.Contains(got[0], "!help") {
		t.Fatalf("help = %v, want prefix #", got)
	}
}

func TestWorkerModelCommandSwitchesChatModel(t *testing.T) {
	cfg := testConfig(t)
	cfg.ChatModels = []string{"opencode/model-a", "opencode/model-b"}
	sender := &stubSender{}
	runner := &stubRunner{}
	r := newTestRelay(t, cfg, runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "m1", Text: "!model 2"})
	if runner.calls != 0 {
		t.Fatalf("runner calls = %d, want 0 (ganti model tidak memanggil agent)", runner.calls)
	}
	if got := runner.CurrentModel(); got != "opencode/model-b" {
		t.Fatalf("model = %q, want opencode/model-b", got)
	}

	// Alias ringkas !model1 harus kembali ke model pertama.
	r.handleMessage(context.Background(), taskItem{ID: "m2", Text: "!model1"})
	if got := runner.CurrentModel(); got != "opencode/model-a" {
		t.Fatalf("model = %q, want opencode/model-a", got)
	}

	// !model tanpa argumen menampilkan daftar dan model aktif.
	sender.reset()
	r.handleMessage(context.Background(), taskItem{ID: "m3", Text: "!model"})
	got := sender.messages()
	if len(got) != 1 {
		t.Fatalf("sent = %d messages, want 1", len(got))
	}
	for _, want := range []string{"opencode/model-a", "opencode/model-b", "(aktif)"} {
		if !strings.Contains(got[0], want) {
			t.Fatalf("model list = %q, tidak memuat %q", got[0], want)
		}
	}
	if runner.calls != 0 {
		t.Fatalf("runner calls = %d, want 0", runner.calls)
	}
}

func TestWorkerCommandShowsBuildSlots(t *testing.T) {
	cfg := testConfig(t)
	sender := &stubSender{}
	runner := &stubRunner{}
	r := newTestRelay(t, cfg, runner, sender)
	r.SetWorkPool([]TaskRunner{
		&ExecRunner{Workdir: "/tmp/build-1"},
		&ExecRunner{Workdir: "/tmp/build-2"},
	})

	_, release, ok := r.workPool.acquire()
	if !ok {
		t.Fatal("slot pertama harusnya bebas")
	}
	defer release()

	r.handleMessage(context.Background(), taskItem{ID: "w", Text: "!worker"})
	if runner.calls != 0 {
		t.Fatalf("runner calls = %d, want 0 (status worker tidak memanggil agent)", runner.calls)
	}
	got := sender.messages()
	if len(got) != 1 {
		t.Fatalf("sent = %d messages, want 1", len(got))
	}
	for _, want := range []string{"2 slot", "1 aktif", "1 bebas", "/tmp/build-1", "/tmp/build-2", "siap"} {
		if !strings.Contains(got[0], want) {
			t.Fatalf("status worker = %q, tidak memuat %q", got[0], want)
		}
	}

	// Alias !cek harus menampilkan status yang sama.
	sender.reset()
	r.handleMessage(context.Background(), taskItem{ID: "w2", Text: "!cek"})
	got = sender.messages()
	if len(got) != 1 || !strings.Contains(got[0], "Worker build: 2 slot") {
		t.Fatalf("!cek = %v, want status worker", got)
	}
}

func TestExecRunnerSetModel(t *testing.T) {
	runner := &ExecRunner{Workdir: "/w", Model: "model-a"}
	if got := runner.CurrentModel(); got != "model-a" {
		t.Fatalf("CurrentModel = %q, want model-a", got)
	}
	runner.SetModel("model-b")
	args := strings.Join(runner.args(""), " ")
	if !strings.Contains(args, "--model model-b") || strings.Contains(args, "--model model-a") {
		t.Fatalf("args = %q, want model-b", args)
	}
}

func TestWorkerAgentReply(t *testing.T) {
	runner := &stubRunner{stdout: `{"type":"text","part":{"text":"halo dari agent"}}`}
	sender := &stubSender{}
	r := newTestRelay(t, testConfig(t), runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "2", Text: "apa kabar"})
	got := sender.messages()
	if len(got) != 1 || got[0] != "halo dari agent" {
		t.Fatalf("sent = %v", got)
	}
	if runner.calls != 1 {
		t.Fatalf("runner calls = %d, want 1", runner.calls)
	}
	if !strings.Contains(runner.prompt, "apa kabar") || !strings.Contains(runner.prompt, "WhatsApp") {
		t.Fatalf("prompt not wrapped as expected: %q", runner.prompt)
	}
}

func TestWorkerEmptyReplySendsKosong(t *testing.T) {
	sender := &stubSender{}
	r := newTestRelay(t, testConfig(t), &stubRunner{stdout: ""}, sender)
	r.handleMessage(context.Background(), taskItem{ID: "3", Text: "kosong"})
	got := sender.messages()
	if len(got) != 1 || got[0] != "(kosong)" {
		t.Fatalf("sent = %v, want [(kosong)]", got)
	}
}

func TestWorkerRunErrorSendsErrorNote(t *testing.T) {
	sender := &stubSender{}
	r := newTestRelay(t, testConfig(t), &stubRunner{err: errors.New("boom"), stderr: "agent gagal"}, sender)
	r.handleMessage(context.Background(), taskItem{ID: "4", Text: "error"})
	got := sender.messages()
	if len(got) != 1 || !strings.HasPrefix(got[0], "(error:") {
		t.Fatalf("sent = %v, want an error note", got)
	}
}

func TestWorkerLongReplyIsChunked(t *testing.T) {
	long := strings.Repeat("z", 7000)
	runner := &stubRunner{stdout: fmt.Sprintf(`{"type":"text","part":{"text":%q}}`, long)}
	sender := &stubSender{}
	r := newTestRelay(t, testConfig(t), runner, sender)
	r.handleMessage(context.Background(), taskItem{ID: "5", Text: "panjang"})
	got := sender.messages()
	if len(got) != 3 {
		t.Fatalf("sent %d chunks, want 3", len(got))
	}
	if len([]rune(got[0])) != 3000 || len([]rune(got[1])) != 3000 || len([]rune(got[2])) != 1000 {
		t.Fatalf("chunk sizes = %d/%d/%d", len([]rune(got[0])), len([]rune(got[1])), len([]rune(got[2])))
	}
}

func TestExecRunnerArgs(t *testing.T) {
	withAgent := strings.Join((&ExecRunner{Workdir: "/w", Agent: "stepi"}).args(""), " ")
	wantWith := "run --format json --title wa-relay --agent stepi"
	if withAgent != wantWith {
		t.Fatalf("args = %q, want %q", withAgent, wantWith)
	}

	withSession := strings.Join((&ExecRunner{Workdir: "/w", Agent: "stepi"}).args("ses_1"), " ")
	wantSession := "run --format json --title wa-relay --agent stepi --session ses_1"
	if withSession != wantSession {
		t.Fatalf("args = %q, want %q", withSession, wantSession)
	}

	withoutAgent := strings.Join((&ExecRunner{Workdir: "/w"}).args(""), " ")
	wantWithout := "run --format json --title wa-relay"
	if withoutAgent != wantWithout {
		t.Fatalf("args = %q, want %q", withoutAgent, wantWithout)
	}

	withModel := strings.Join((&ExecRunner{Workdir: "/w", Agent: "stepi", Model: "opencode/deepseek-v4.1-flash-free"}).args(""), " ")
	wantModel := "run --format json --title wa-relay --agent stepi --model opencode/deepseek-v4.1-flash-free"
	if withModel != wantModel {
		t.Fatalf("args = %q, want %q", withModel, wantModel)
	}
}

func TestConfigPerLaneModelFallback(t *testing.T) {
	cfg := &Config{Model: "fallback"}
	if got := cfg.ChatModelName(); got != "fallback" {
		t.Fatalf("ChatModelName fallback = %q, want fallback", got)
	}
	if got := cfg.WorkModelName(); got != "fallback" {
		t.Fatalf("WorkModelName fallback = %q, want fallback", got)
	}

	cfg.ChatModel = "chat-model"
	cfg.WorkModel = "work-model"
	if got := cfg.ChatModelName(); got != "chat-model" {
		t.Fatalf("ChatModelName = %q, want chat-model", got)
	}
	if got := cfg.WorkModelName(); got != "work-model" {
		t.Fatalf("WorkModelName = %q, want work-model", got)
	}

	cfg.ChatModels = []string{" chat-a ", "", "chat-b"}
	if got := cfg.ChatModelName(); got != "chat-a" {
		t.Fatalf("ChatModelName list = %q, want chat-a", got)
	}
	if got := cfg.ChatModelList(); len(got) != 2 || got[0] != "chat-a" || got[1] != "chat-b" {
		t.Fatalf("ChatModelList = %v, want [chat-a chat-b]", got)
	}
}

func TestParseSessionID(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"first event", `{"type":"step_start","sessionID":"ses_abc"}` + "\n" + `{"type":"text","sessionID":"ses_abc"}`, "ses_abc"},
		{"later event only", `{"type":"text","sessionID":"ses_xyz"}`, "ses_xyz"},
		{"no session field", `{"type":"text"}`, ""},
		{"not json", "bukan json\n", ""},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseSessionID(tc.in); got != tc.want {
				t.Fatalf("parseSessionID() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRelayPersistsAndReusesSession(t *testing.T) {
	cfg := testConfig(t)
	runner := &stubRunner{stdout: `{"type":"step_start","sessionID":"ses_test"}` + "\n" + `{"type":"text","part":{"text":"halo"}}`}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "1", Text: "hai"})
	if got := r.session.ID(); got != "ses_test" {
		t.Fatalf("session id = %q, want ses_test", got)
	}
	if got := LoadSession(r.session.Path()).ID(); got != "ses_test" {
		t.Fatalf("persisted session = %q, want ses_test", got)
	}

	r.handleMessage(context.Background(), taskItem{ID: "2", Text: "lanjut"})
	if len(runner.sessions) != 2 || runner.sessions[0] != "" || runner.sessions[1] != "ses_test" {
		t.Fatalf("runner sessions = %v, want [\"\" ses_test]", runner.sessions)
	}
}

func TestExecRunnerCapturesStdout(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-opencode")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho '{\"type\":\"text\",\"part\":{\"text\":\"OK\"}}'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	runner := &ExecRunner{Bin: script, Workdir: dir, BaseDir: dir, Timeout: 5 * time.Second}
	stdout, _, err := runner.Run(context.Background(), "hi", "")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := extractReply(stdout, false); got != "OK" {
		t.Fatalf("extractReply = %q, want OK", got)
	}
}

func TestExecRunnerUsesWorkdirAsCwd(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-opencode")
	if err := os.WriteFile(script, []byte("#!/bin/sh\npwd\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	runner := &ExecRunner{Bin: script, Workdir: dir, BaseDir: t.TempDir(), Timeout: 5 * time.Second}
	stdout, _, err := runner.Run(context.Background(), "hi", "")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := strings.TrimSpace(stdout); got != dir {
		t.Fatalf("cwd = %q, want %q", got, dir)
	}
}

func TestExecRunnerNotFound(t *testing.T) {
	runner := &ExecRunner{Bin: "definitely-not-a-real-binary-xyz", Timeout: time.Second}
	if _, _, err := runner.Run(context.Background(), "hi", ""); !errors.Is(err, exec.ErrNotFound) {
		t.Fatalf("err = %v, want exec.ErrNotFound", err)
	}
}

// TestExecRunnerLargePromptUsesStdin memastikan prompt besar tidak lagi dikirim
// lewat argv (yang kena batas ~128KB dan bikin "argument list too long"),
// melainkan lewat stdin.
func TestExecRunnerLargePromptUsesStdin(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-opencode")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nwc -c\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	runner := &ExecRunner{Bin: script, Workdir: dir, BaseDir: dir, Timeout: 5 * time.Second}
	prompt := strings.Repeat("a", 300*1024)
	stdout, _, err := runner.Run(context.Background(), prompt, "")
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := strings.TrimSpace(stdout); got != strconv.Itoa(len(prompt)) {
		t.Fatalf("stdin bytes = %q, want %d", got, len(prompt))
	}
}

func TestTruncateBytes(t *testing.T) {
	if got := truncateBytes("halo", 100); got != "halo" {
		t.Fatalf("truncateBytes pendek = %q, want halo", got)
	}
	long := strings.Repeat("a", 200)
	got := truncateBytes(long, 100)
	if !strings.HasPrefix(got, strings.Repeat("a", 100)) || !strings.Contains(got, "dipotong") {
		t.Fatalf("truncateBytes panjang tidak sesuai: %q", got)
	}
	// Pemotongan di tengah karakter UTF-8 multi-byte harus tetap valid.
	multi := strings.Repeat("é", 100) // tiap 'é' 2 byte
	gotMulti := truncateBytes(multi, 101)
	if !utf8.ValidString(gotMulti) {
		t.Fatalf("truncateBytes memotong di tengah rune: %q", gotMulti)
	}
}

func TestExecRunnerTimeoutKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-opencode")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	runner := &ExecRunner{Bin: script, Workdir: dir, BaseDir: dir, Timeout: 200 * time.Millisecond}
	start := time.Now()
	_, _, err := runner.Run(context.Background(), "hi", "")
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("timeout took %s, process group not killed", elapsed)
	}
}

func TestLoggerNoDuplicateWhenStdoutIsSameFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.log")
	out, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	log, err := newLogger(out, path)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("halo")
	log.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("log lines = %d, want 1 (tidak boleh ganda): %q", len(lines), string(data))
	}
	if !strings.Contains(lines[0], "halo") {
		t.Fatalf("log tidak berisi pesan: %q", string(data))
	}
}

func TestLoggerWritesToBothWhenDifferentTargets(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "relay.log")
	outPath := filepath.Join(dir, "stdout.log")
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}

	log, err := newLogger(out, logPath)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("ke-dua")
	log.Close()
	out.Close()

	for _, p := range []string{logPath, outPath} {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("baca %s: %v", p, err)
		}
		if !strings.Contains(string(data), "ke-dua") {
			t.Fatalf("%s tidak berisi log: %q", p, string(data))
		}
	}
}

func TestWacliMarkReaderArgs(t *testing.T) {
	got := strings.Join((&WacliMarkReader{Bin: "wacli"}).args("6281234567890@s.whatsapp.net"), " ")
	want := "chats mark-read --chat 6281234567890@s.whatsapp.net --receipts"
	if got != want {
		t.Fatalf("args = %q, want %q", got, want)
	}
}

func TestLoadConfigMarkReadDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"owner_jid":"6281234567890@s.whatsapp.net"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.MarkRead {
		t.Fatal("mark_read harus default true bila tidak diset")
	}

	if err := os.WriteFile(path, []byte(`{"mark_read":false}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MarkRead {
		t.Fatal("mark_read:false di config harus dihormati")
	}
}

func TestExampleConfigEnablesMarkRead(t *testing.T) {
	cfg, err := LoadConfig(filepath.Join("..", "..", "config.example.json"))
	if err != nil {
		t.Fatalf("config.example.json tidak bisa dibaca: %v", err)
	}
	if !cfg.MarkRead {
		t.Fatal("config.example.json harus memuat mark_read true")
	}
}

func TestHookDoesNotMarkReadOnQueue(t *testing.T) {
	cfg := testConfig(t)
	cfg.MarkRead = true
	r := newTestRelay(t, cfg, &stubRunner{}, &stubSender{})
	reader := &stubMarkReader{}
	r.reader = reader

	item := hookItem(t, r, cfg, validPayload())
	if item.ID != "MSG1" || item.Chat != cfg.OwnerJID {
		t.Fatalf("item = %+v, want id=MSG1 chat=%s", item, cfg.OwnerJID)
	}
	// Pesan yang baru masuk antrean harus tetap centang dua abu-abu;
	// mark-read hanya boleh terjadi saat balasan disiapkan di handleMessage.
	time.Sleep(30 * time.Millisecond)
	if got := reader.calls(); len(got) != 0 {
		t.Fatalf("mark-read dipanggil saat pesan masuk antrean: %v", got)
	}
}

func TestMarkReadAfterRunnerDoneBeforeReply(t *testing.T) {
	cfg := testConfig(t)
	cfg.MarkRead = true
	elog := &eventLog{}
	runner := &stubRunner{stdout: `{"type":"text","part":{"text":"halo dari agent"}}`, rec: elog.add}
	sender := &stubSender{rec: elog.add}
	r := newTestRelay(t, cfg, runner, sender)
	reader := &stubMarkReader{rec: elog.add}
	r.reader = reader

	item := hookItem(t, r, cfg, validPayload())
	r.handleMessage(context.Background(), item)

	if got := reader.calls(); len(got) != 1 || got[0] != cfg.OwnerJID {
		t.Fatalf("mark-read = %v, want [%s]", got, cfg.OwnerJID)
	}
	assertEvents(t, elog.list(), []string{
		"runner-start",
		"runner-done",
		"mark-read:" + cfg.OwnerJID,
		"send:halo dari agent",
	})
}

func TestMarkReadNotCalledBeforeRunnerDone(t *testing.T) {
	cfg := testConfig(t)
	cfg.MarkRead = true
	elog := &eventLog{}
	runner := &stubRunner{
		stdout: `{"type":"text","part":{"text":"jawaban"}}`,
		delay:  300 * time.Millisecond,
		rec:    elog.add,
	}
	r := newTestRelay(t, cfg, runner, &stubSender{})
	reader := &stubMarkReader{}
	r.reader = reader

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.handleMessage(context.Background(), taskItem{ID: "slow", Text: "halo", Chat: cfg.OwnerJID})
	}()

	waitEvent(t, elog, "runner-start")
	if got := reader.calls(); len(got) != 0 {
		t.Fatalf("mark-read terjadi sebelum runner.Run selesai: %v", got)
	}
	waitEvent(t, elog, "runner-done")
	<-done
	if got := reader.calls(); len(got) != 1 || got[0] != cfg.OwnerJID {
		t.Fatalf("mark-read = %v, want [%s]", got, cfg.OwnerJID)
	}
}

func TestMarkReadOnErrorBeforeErrorNote(t *testing.T) {
	cfg := testConfig(t)
	cfg.MarkRead = true
	elog := &eventLog{}
	runner := &stubRunner{err: errors.New("boom"), stderr: "agent gagal", rec: elog.add}
	sender := &stubSender{rec: elog.add}
	r := newTestRelay(t, cfg, runner, sender)
	reader := &stubMarkReader{rec: elog.add}
	r.reader = reader

	item := hookItem(t, r, cfg, validPayload())
	r.handleMessage(context.Background(), item)

	if got := reader.calls(); len(got) != 1 || got[0] != cfg.OwnerJID {
		t.Fatalf("mark-read = %v, want [%s]", got, cfg.OwnerJID)
	}
	events := elog.list()
	prefix := []string{"runner-start", "runner-done", "mark-read:" + cfg.OwnerJID}
	if len(events) != len(prefix)+1 {
		t.Fatalf("events = %v, want %v + send error", events, prefix)
	}
	for i, ev := range prefix {
		if events[i] != ev {
			t.Fatalf("events = %v, want %v + send error", events, prefix)
		}
	}
	if !strings.HasPrefix(events[3], "send:(error:") {
		t.Fatalf("event terakhir = %q, want send:(error: ...)", events[3])
	}
}

func TestMarkReadOnFastPath(t *testing.T) {
	t.Run("ping", func(t *testing.T) {
		cfg := testConfig(t)
		cfg.MarkRead = true
		elog := &eventLog{}
		runner := &stubRunner{rec: elog.add}
		sender := &stubSender{rec: elog.add}
		r := newTestRelay(t, cfg, runner, sender)
		reader := &stubMarkReader{rec: elog.add}
		r.reader = reader

		r.handleMessage(context.Background(), taskItem{ID: "p", Text: " PiNg ", Chat: cfg.OwnerJID})
		if runner.calls != 0 {
			t.Fatalf("runner calls = %d, want 0", runner.calls)
		}
		assertEvents(t, elog.list(), []string{"mark-read:" + cfg.OwnerJID, "send:pong"})
	})

	t.Run("reset", func(t *testing.T) {
		cfg := testConfig(t)
		cfg.MarkRead = true
		elog := &eventLog{}
		runner := &stubRunner{rec: elog.add}
		sender := &stubSender{rec: elog.add}
		r := newTestRelay(t, cfg, runner, sender)
		reader := &stubMarkReader{rec: elog.add}
		r.reader = reader

		r.handleMessage(context.Background(), taskItem{ID: "r", Text: "!reset", Chat: cfg.OwnerJID})
		if runner.calls != 0 {
			t.Fatalf("runner calls = %d, want 0", runner.calls)
		}
		assertEvents(t, elog.list(), []string{
			"mark-read:" + cfg.OwnerJID,
			"send:oke, kita mulai dari awal ya 😊",
		})
	})
}

func TestMarkReadUsesFallbackTarget(t *testing.T) {
	cfg := testConfig(t)
	cfg.MarkRead = true
	r := newTestRelay(t, cfg, &stubRunner{}, &stubSender{})
	reader := &stubMarkReader{}
	r.reader = reader

	payload := validPayload()
	payload["Chat"] = ""
	item := hookItem(t, r, cfg, payload)
	if item.Chat != payload["SenderJID"] {
		t.Fatalf("item.Chat = %v, want %v", item.Chat, payload["SenderJID"])
	}
	r.handleMessage(context.Background(), item)

	got := reader.calls()
	if len(got) != 1 || got[0] != payload["SenderJID"] {
		t.Fatalf("mark-read = %v, want [%v]", got, payload["SenderJID"])
	}
}

func TestChatTargetPriority(t *testing.T) {
	owner := &Config{OwnerJID: "628111@s.whatsapp.net", OwnerPhone: "628111"}
	phoneOnly := &Config{OwnerPhone: "628222"}
	cases := []struct {
		name string
		cfg  *Config
		msg  *hookPayload
		want string
	}{
		{"chat diprioritaskan", owner, &hookPayload{Chat: "chat@s.whatsapp.net", SenderJID: "sender@s.whatsapp.net"}, "chat@s.whatsapp.net"},
		{"lalu sender", owner, &hookPayload{SenderJID: "sender@s.whatsapp.net"}, "sender@s.whatsapp.net"},
		{"lalu owner jid", owner, &hookPayload{}, "628111@s.whatsapp.net"},
		{"terakhir owner phone", phoneOnly, &hookPayload{}, "628222"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := chatTarget(tc.cfg, tc.msg); got != tc.want {
				t.Fatalf("chatTarget() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHookDoesNotMarkReadIgnored(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(map[string]any)
		clearOwner bool
		want       string
	}{
		{"event", func(m map[string]any) { m["EventType"] = "receipt" }, false, "event"},
		{"from me", func(m map[string]any) { m["FromMe"] = true }, false, "fromme"},
		{"not owner", func(m map[string]any) { m["SenderJID"] = "6289999999999@s.whatsapp.net" }, false, "not-owner"},
		{"group chat", func(m map[string]any) { m["Chat"] = "1234-5678@g.us"; m["SenderJID"] = "1234-5678@g.us" }, true, "group/status"},
		{"empty text", func(m map[string]any) { m["Text"] = "" }, false, "empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.MarkRead = true
			if tc.clearOwner {
				cfg.OwnerJID = ""
			}
			r := newTestRelay(t, cfg, &stubRunner{}, &stubSender{})
			reader := &stubMarkReader{}
			r.reader = reader

			payload := validPayload()
			tc.mutate(payload)
			body, _ := json.Marshal(payload)
			rec := postHook(t, r, body, signBody(cfg.WebhookSecret, body))
			if got := decodeBody(t, rec)["ignored"]; got != tc.want {
				t.Fatalf("ignored = %v, want %v", got, tc.want)
			}
			time.Sleep(30 * time.Millisecond)
			if got := reader.calls(); len(got) != 0 {
				t.Fatalf("mark-read dipanggil untuk pesan ignored: %v", got)
			}
		})
	}
}

func TestDuplicateMessageDoesNotMarkReadAgain(t *testing.T) {
	cfg := testConfig(t)
	cfg.MarkRead = true
	r := newTestRelay(t, cfg, &stubRunner{stdout: `{"type":"text","part":{"text":"jawaban"}}`}, &stubSender{})
	reader := &stubMarkReader{}
	r.reader = reader

	item := hookItem(t, r, cfg, validPayload())
	r.handleMessage(context.Background(), item)
	if got := reader.calls(); len(got) != 1 {
		t.Fatalf("mark-read = %v, want 1 panggilan", got)
	}

	body, _ := json.Marshal(validPayload())
	rec := postHook(t, r, body, signBody(cfg.WebhookSecret, body))
	if got := decodeBody(t, rec)["ignored"]; got != "duplicate" {
		t.Fatalf("ignored = %v, want duplicate", got)
	}
	if len(r.queue) != 0 || len(r.chatQueue) != 0 || len(r.workQueue) != 0 {
		t.Fatal("duplikat tidak boleh masuk antrean")
	}
	if got := reader.calls(); len(got) != 1 {
		t.Fatalf("mark-read untuk duplikat = %v, want cuma 1", got)
	}
}

func TestMarkReadDisabledByConfig(t *testing.T) {
	cfg := testConfig(t)
	cfg.MarkRead = false
	sender := &stubSender{}
	r := newTestRelay(t, cfg, &stubRunner{stdout: `{"type":"text","part":{"text":"jawaban"}}`}, sender)
	reader := &stubMarkReader{}
	r.reader = reader

	item := hookItem(t, r, cfg, validPayload())
	r.handleMessage(context.Background(), item)

	if got := reader.calls(); len(got) != 0 {
		t.Fatalf("mark-read jalan padahal config off: %v", got)
	}
	if got := sender.messages(); len(got) != 1 || got[0] != "jawaban" {
		t.Fatalf("sent = %v, want [jawaban]", got)
	}
}

func TestMarkReadNilReaderIsSafe(t *testing.T) {
	cfg := testConfig(t)
	cfg.MarkRead = true
	sender := &stubSender{}
	r := newTestRelay(t, cfg, &stubRunner{stdout: `{"type":"text","part":{"text":"jawaban"}}`}, sender)

	item := hookItem(t, r, cfg, validPayload())
	r.handleMessage(context.Background(), item)

	if got := sender.messages(); len(got) != 1 || got[0] != "jawaban" {
		t.Fatalf("sent = %v, want [jawaban]", got)
	}
}

func TestParseFileDirectives(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "a.png")
	if err := os.WriteFile(good, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	text := "halo bos\n[[file:" + good + "|ini gambar]]\ntrimakasih"
	clean, files := parseFileDirectives(text)
	if strings.Contains(clean, "[[file:") {
		t.Fatalf("directive belum dibuang: %q", clean)
	}
	if clean != "halo bos\ntrimakasih" {
		t.Fatalf("clean = %q", clean)
	}
	if len(files) != 1 || files[0].Path != good || files[0].Caption != "ini gambar" {
		t.Fatalf("files = %+v", files)
	}

	// Path tidak ada / relatif -> baris dibiarkan sebagai teks biasa.
	for _, line := range []string{"[[file:/tidak/ada.png|x]]", "[[file:rel.png|x]]", "[[file:/tmp|dir]]"} {
		clean, files := parseFileDirectives(line)
		if len(files) != 0 || clean != line {
			t.Fatalf("baris %q seharusnya tetap teks, dapat files=%+v clean=%q", line, files, clean)
		}
	}
}

func TestWorkerSendsFileDirective(t *testing.T) {
	cfg := testConfig(t)
	cfg.SendFileEnabled = true
	path := filepath.Join(t.TempDir(), "foto.jpg")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	reply := "Ini fotonya ya\n[[file:" + path + "|nih]]"
	runner := &stubRunner{stdout: fmt.Sprintf(`{"type":"text","part":{"text":%q}}`, reply)}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "f1", Text: "kirim foto"})

	if msgs := sender.messages(); len(msgs) != 1 || msgs[0] != "Ini fotonya ya" {
		t.Fatalf("messages = %v, want [Ini fotonya ya]", msgs)
	}
	if files := sender.filesSent(); len(files) != 1 || files[0] != path {
		t.Fatalf("files = %v, want [%s]", files, path)
	}
}

func TestWorkerFileOnlyReplyNoKosong(t *testing.T) {
	cfg := testConfig(t)
	cfg.SendFileEnabled = true
	path := filepath.Join(t.TempDir(), "only.png")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	runner := &stubRunner{stdout: fmt.Sprintf(`{"type":"text","part":{"text":%q}}`, "[[file:"+path+"|cap]]")}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)

	r.handleMessage(context.Background(), taskItem{ID: "f2", Text: "foto"})

	if msgs := sender.messages(); len(msgs) != 0 {
		t.Fatalf("messages = %v, want kosong", msgs)
	}
	if files := sender.filesSent(); len(files) != 1 || files[0] != path {
		t.Fatalf("files = %v, want [%s]", files, path)
	}
}

func TestShouldIgnoreAllowsMedia(t *testing.T) {
	cfg := testConfig(t)
	cfg.IncomingMediaEnabled = true
	owner := cfg.OwnerJID
	msg := &hookPayload{Chat: owner, SenderJID: owner, Media: json.RawMessage(`{"Type":"image","Filename":"a.jpg"}`)}
	if reason, ok := shouldIgnore(cfg, msg); ok {
		t.Fatalf("media tanpa teks diabaikan: %s", reason)
	}
	if mi := msg.mediaInfo(); mi == nil || mi.Type != "image" || mi.Filename != "a.jpg" {
		t.Fatalf("mediaInfo = %+v", mi)
	}
	cfg.IncomingMediaEnabled = false
	if _, ok := shouldIgnore(cfg, msg); !ok {
		t.Fatal("media harus diabaikan saat incoming_media_enabled=false")
	}
	if (&hookPayload{Media: json.RawMessage("null")}).hasMedia() {
		t.Fatal("Media null seharusnya tidak dianggap ada")
	}
}

type stubDownloader struct {
	path           string
	err            error
	chat, id, name string
}

func (s *stubDownloader) Download(chat, msgID, filename string) (string, error) {
	s.chat, s.id, s.name = chat, msgID, filename
	return s.path, s.err
}

func TestPrepareMediaPrompt(t *testing.T) {
	cfg := testConfig(t)
	cfg.IncomingMediaEnabled = true
	dl := &stubDownloader{path: "/tmp/x/photo.jpg"}
	r := newTestRelay(t, cfg, &stubRunner{}, &stubSender{})
	r.SetMediaDownloader(dl)

	item := taskItem{
		ID:   "m1",
		Chat: "c",
		Text: "ini apa?",
		Media: &MediaInfo{
			Type:     "image",
			Filename: "photo.jpg",
			MimeType: "image/jpeg",
			Caption:  "pantai",
		},
	}
	got, err := r.prepareMedia(item)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ini apa?", "/tmp/x/photo.jpg", "image", "image/jpeg", "photo.jpg", "pantai"} {
		if !strings.Contains(got, want) {
			t.Fatalf("prompt %q tidak memuat %q", got, want)
		}
	}
	if dl.chat != "c" || dl.id != "m1" || dl.name != "photo.jpg" {
		t.Fatalf("downloader args = %+v", dl)
	}
}

package relay

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

type hookPayload struct {
	Chat      string          `json:"Chat"`
	ID        string          `json:"ID"`
	SenderJID string          `json:"SenderJID"`
	Timestamp string          `json:"Timestamp"`
	FromMe    bool            `json:"FromMe"`
	Text      string          `json:"Text"`
	ChatName  string          `json:"ChatName"`
	Media     json.RawMessage `json:"Media"`
	EventType string          `json:"EventType"`
}

// MediaInfo adalah objek Media yang wacli sertakan pada pesan berlampiran.
// Pesan tanpa media memakai Media: null. Field attachment internal
// (MediaKey, DirectPath, dll.) tidak diekspor wacli, jadi unduhan dilakukan
// lewat `wacli media download`.
type MediaInfo struct {
	Type       string `json:"Type"`
	Caption    string `json:"Caption"`
	Filename   string `json:"Filename"`
	MimeType   string `json:"MimeType"`
	FileLength int64  `json:"FileLength"`
}

// hasMedia melaporkan apakah payload membawa lampiran (Media bukan null/kosong).
func (m *hookPayload) hasMedia() bool {
	s := strings.TrimSpace(string(m.Media))
	return s != "" && s != "null"
}

// mediaInfo mengembalikan objek Media yang ter-parse, atau nil bila tidak ada.
func (m *hookPayload) mediaInfo() *MediaInfo {
	if !m.hasMedia() {
		return nil
	}
	var mi MediaInfo
	if err := json.Unmarshal(m.Media, &mi); err != nil {
		return nil
	}
	return &mi
}

func verifySignature(secret string, body []byte, header string) bool {
	if secret == "" {
		return true
	}
	header = strings.TrimSpace(header)
	if header == "" {
		return false
	}
	header = strings.TrimPrefix(header, "sha256=")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(strings.ToLower(header)))
}

func isBlockedJID(jid string) bool {
	return strings.HasSuffix(jid, "@g.us") ||
		strings.HasSuffix(jid, "@newsletter") ||
		jid == "status@broadcast"
}

// shouldIgnore returns the reason (mirroring the Python relay) when a payload
// must not be processed.
func shouldIgnore(cfg *Config, msg *hookPayload) (string, bool) {
	if msg.EventType != "" && msg.EventType != "message" {
		return "event", true
	}
	if msg.FromMe {
		return "fromme", true
	}
	sender := msg.SenderJID
	if sender == "" {
		sender = msg.Chat
	}
	if cfg.OwnerJID != "" && sender != cfg.OwnerJID {
		return "not-owner", true
	}
	if isBlockedJID(msg.Chat) || isBlockedJID(sender) {
		return "group/status", true
	}
	// Pesan berlampiran boleh tanpa teks. Bila penanganan lampiran dimatikan,
	// lampiran diabaikan dan hanya teksnya yang diproses (jadi tanpa teks tetap
	// dibuang).
	if strings.TrimSpace(msg.Text) == "" {
		if !msg.hasMedia() || !cfg.IncomingMediaEnabled {
			return "empty", true
		}
	}
	return "", false
}

func respond(w http.ResponseWriter, code int, payload map[string]any) {
	body, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, `{"ok":false,"error":"internal"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(body)
}

// Routes mengembalikan handler HTTP untuk endpoint relay.
func (r *Relay) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, http.StatusOK, map[string]any{"ok": true})
	})
	mux.HandleFunc("POST /hook", r.handleHook)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		respond(w, http.StatusNotFound, map[string]any{"ok": false, "error": "not found"})
	})
	return mux
}

func (r *Relay) handleHook(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		respond(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "read body"})
		return
	}
	if !verifySignature(r.cfg.WebhookSecret, body, req.Header.Get("X-Wacli-Signature")) {
		r.log.Warn("bad signature from %s", req.RemoteAddr)
		respond(w, http.StatusUnauthorized, map[string]any{"ok": false, "error": "bad signature"})
		return
	}
	var msg hookPayload
	if err := json.Unmarshal(body, &msg); err != nil {
		respond(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid json"})
		return
	}
	if reason, ok := shouldIgnore(r.cfg, &msg); ok {
		respond(w, http.StatusOK, map[string]any{"ok": true, "ignored": reason})
		return
	}
	if msg.ID == "" || r.seen.Seen(msg.ID) {
		respond(w, http.StatusOK, map[string]any{"ok": true, "ignored": "duplicate"})
		return
	}
	item := taskItem{ID: msg.ID, Text: strings.TrimSpace(msg.Text), Chat: chatTarget(r.cfg, &msg), Lane: Classify(r.cfg, msg.Text)}
	if r.cfg.IncomingMediaEnabled {
		item.Media = msg.mediaInfo()
	}
	// Target antrean menurut lane, supaya tugas berat tidak pernah macet di
	// antrean chat dan sebaliknya. `queue` (legacy) tetap jadi fallback.
	var target chan taskItem
	switch item.Lane {
	case LaneWork:
		target = r.workQueue
	default:
		target = r.chatQueue
	}
	select {
	case target <- item:
		r.log.Info("queued id=%s len=%d lane=%s", item.ID, len(item.Text), item.Lane)
		respond(w, http.StatusOK, map[string]any{"ok": true, "queued": true})
	case r.queue <- item:
		r.log.Info("queued id=%s len=%d lane=unified", item.ID, len(item.Text))
		respond(w, http.StatusOK, map[string]any{"ok": true, "queued": true})
	default:
		respond(w, http.StatusOK, map[string]any{"ok": true, "ignored": "queue-full"})
	}
}

// chatTarget memilih identifier chat untuk mark-read: chat asli pesan bila
// ada, lalu sender, lalu fallback ke identitas owner dari config. Hasilnya
// dibawa lewat taskItem.Chat dan dipakai handleMessage tepat sebelum balasan
// final dikirim.
func chatTarget(cfg *Config, msg *hookPayload) string {
	switch {
	case msg.Chat != "":
		return msg.Chat
	case msg.SenderJID != "":
		return msg.SenderJID
	case cfg.OwnerJID != "":
		return cfg.OwnerJID
	default:
		return cfg.OwnerPhone
	}
}

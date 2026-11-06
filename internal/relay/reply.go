package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

func (r *Relay) sendChunked(text string) {
	text = strings.TrimSpace(text)
	// Balasan agent boleh menyelipkan directive lampiran (lihat fileDirectiveRe).
	// Ambil directivenya lebih dulu, sisa teks tetap dikirim sebagai pesan biasa.
	text, files := parseFileDirectives(text)
	text = strings.TrimSpace(text)
	if r.fileSendingEnabled() {
		if text == "" && len(files) == 0 {
			text = "(kosong)"
		}
	} else {
		if len(files) > 0 {
			r.log.Warn("send file dimatikan, %d lampiran diabaikan", len(files))
			files = nil
		}
		if text == "" {
			text = "(kosong)"
		}
	}
	if text != "" {
		chunks := chunkText(text, defaultChunk)
		for i, chunk := range chunks {
			err := r.sender.Send(chunk)
			if err != nil {
				r.log.Error("wacli send chunk %d/%d ok=false err=%v", i+1, len(chunks), err)
				if i == 0 && text != "(kosong)" {
					r.sender.Send("(kosong)")
				}
				continue
			}
			r.log.Info("wacli send chunk %d/%d ok=true", i+1, len(chunks))
		}
	}
	for _, f := range files {
		r.sendFile(f)
	}
}

// fileSendingEnabled melaporkan apakah directive lampiran boleh dikirim keluar.
// Default aktif bila config belum diisi (mis. di test).
func (r *Relay) fileSendingEnabled() bool {
	return r.cfg == nil || r.cfg.SendFileEnabled
}

func (r *Relay) sendFile(f outFile) {
	if err := r.sender.SendFile(f.Path, f.Caption); err != nil {
		r.log.Error("wacli send file ok=false path=%s err=%v", f.Path, err)
		return
	}
	r.log.Info("wacli send file ok=true path=%s caption=%t", f.Path, f.Caption != "")
}

// outFile adalah lampiran yang diminta agent lewat directive di balasannya.
type outFile struct {
	Path    string
	Caption string
}

// fileDirectiveRe mencocokkan satu baris directive lampiran:
//
//	[[file:/path/absolut/ke/file.jpg|caption opsional]]
//
// Directive harus menempati satu baris sendiri (bukan disisipkan di tengah
// kalimat) supaya teks biasa yang kebetulan memuat pola serupa tidak ikut
// terkirim sebagai lampiran.
var fileDirectiveRe = regexp.MustCompile(`^\[\[file:\s*(.+?)\s*(?:\|\s*(.*?)\s*)?\]\]$`)

// parseFileDirectives memisahkan directive lampiran dari teks balasan. Baris
// yang menyerupai directive tapi menunjuk ke path tidak valid (bukan absolut
// atau file tidak ada) dibiarkan sebagai teks biasa, sehingga diskusi kode
// tentang directive ini tidak memicu kiriman file.
func parseFileDirectives(text string) (string, []outFile) {
	if !strings.Contains(text, "[[file:") {
		return text, nil
	}
	lines := strings.Split(text, "\n")
	kept := make([]string, 0, len(lines))
	var files []outFile
	for _, line := range lines {
		m := fileDirectiveRe.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			kept = append(kept, line)
			continue
		}
		path := strings.TrimSpace(m[1])
		if !validOutFilePath(path) {
			kept = append(kept, line)
			continue
		}
		files = append(files, outFile{Path: path, Caption: strings.TrimSpace(m[2])})
	}
	return strings.Join(kept, "\n"), files
}

func validOutFilePath(path string) bool {
	if path == "" || !filepath.IsAbs(path) {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func chunkText(text string, size int) []string {
	if size <= 0 {
		size = defaultChunk
	}
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{"(kosong)"}
	}
	var out []string
	for i := 0; i < len(runes); i += size {
		end := i + size
		if end > len(runes) {
			end = len(runes)
		}
		out = append(out, string(runes[i:end]))
	}
	return out
}

// isMissingSession melaporkan apakah kegagalan opencode disebabkan sesi yang
// diminta sudah tidak ada lagi di DB (mis. sesi dihapus manual). opencode
// menulis "Error: Session not found" ke stderr lalu keluar dengan status 1.
func isMissingSession(stderr string) bool {
	return strings.Contains(strings.ToLower(stderr), "session not found")
}

// isQuotaExhausted melaporkan apakah kegagalan opencode disebabkan kuota/token
// provider habis (mis. "Insufficient account funds" / 402).
func isQuotaExhausted(detail string) bool {
	d := strings.ToLower(detail)
	return strings.Contains(d, "insufficient account funds") ||
		strings.Contains(d, "quota") ||
		strings.Contains(d, "402")
}

// agentErrorDetail mengambil pesan error dari event NDJSON `opencode run`
// (tipe "error") atau dari stderr. OpenCode menulis error provider seperti
// kuota/limit ke stdout, jadi tanpa ini penyebabnya cuma kelihatan di log.
func agentErrorDetail(stdout, stderr string) string {
	var msgs []string
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if len(line) < 2 || line[0] != '{' {
			continue
		}
		var ev struct {
			Type  string `json:"type"`
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal([]byte(line), &ev) != nil || ev.Type != "error" {
			continue
		}
		msg := strings.TrimSpace(ev.Error.Message)
		if msg == "" {
			continue
		}
		if label := strings.TrimSpace(ev.Error.Type); label != "" {
			msg = label + ": " + msg
		}
		msgs = append(msgs, msg)
	}
	detail := strings.Join(msgs, "\n")
	if detail == "" {
		detail = strings.TrimSpace(stderr)
	}
	return truncateRunes(detail, failureDetailMaxRune)
}

func errorNote(err error, stderr string) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "(error: timeout: agent terlalu lama)"
	}
	var notFound *exec.Error
	if errors.Is(err, exec.ErrNotFound) || errors.As(err, &notFound) {
		return "(error: opencode tidak ditemukan)"
	}
	var exitErr *exec.ExitError
	code := "unknown"
	if errors.As(err, &exitErr) {
		code = fmt.Sprintf("%d", exitErr.ExitCode())
	}
	note := "(error: opencode exit " + code
	if tail := truncateRunes(strings.TrimSpace(stderr), stderrMaxRune); tail != "" {
		note += ": " + tail
	}
	return note + ")"
}

// parseSessionID mengambil sessionID dari event NDJSON pertama yang memuatnya.
func parseSessionID(stdout string) string {
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if len(line) < 2 || line[0] != '{' {
			continue
		}
		var ev struct {
			SessionID string `json:"sessionID"`
		}
		if json.Unmarshal([]byte(line), &ev) == nil && ev.SessionID != "" {
			return ev.SessionID
		}
	}
	return ""
}

var thinkingRe = regexp.MustCompile(`(?s)<think(?:ing)?>.*?</think(?:ing)?>`)

var (
	thinkingClosers = []string{"</thinking>", "</think>"}
	thinkingOpeners = []string{"<thinking>", "<think>"}
)

// stripReasoning membuang blok/tag reasoning yang kadang ikut tercampur di dalam
// part teks (mis. "</thinking>" atau "</think>" nyasar), supaya yang terkirim
// hanya balasan asli.
func stripReasoning(s string, isStepi bool) string {
	if !isStepi || !strings.Contains(s, "think") {
		return s
	}
	s = thinkingRe.ReplaceAllString(s, "")
	// Tag penutup tanpa pembuka: ambil teks setelah kemunculan terakhir.
	best, tagLen := -1, 0
	for _, tag := range thinkingClosers {
		if i := strings.LastIndex(s, tag); i > best {
			best, tagLen = i, len(tag)
		}
	}
	if best >= 0 {
		s = s[best+tagLen:]
	}
	// Tag pembuka tanpa penutup: buang dari kemunculan pertama sampai akhir.
	best, tagLen = -1, 0
	for _, tag := range thinkingOpeners {
		if i := strings.Index(s, tag); i >= 0 && (best < 0 || i < best) {
			best, tagLen = i, len(tag)
		}
	}
	if best >= 0 {
		s = s[:best]
	}
	return s
}

// extractReply pulls the agent's answer out of NDJSON stdout. It never panics.
func extractReply(stdout string, isStepi bool) string {
	defer func() {
		if recover() != nil {
			return
		}
	}()
	var parts []string
	var fallbacks []string
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]any
		if json.Unmarshal([]byte(line), &obj) != nil {
			continue
		}
		if t, _ := obj["type"].(string); t == "text" {
			if part, ok := obj["part"].(map[string]any); ok {
				if text, ok := part["text"].(string); ok && text != "" {
					if isStepi {
						text = stripReasoning(text, true)
					}
					parts = append(parts, text)
				}
			}
		}
		fallbacks = append(fallbacks, collectTextFields(obj)...)
	}
	if len(parts) > 0 {
		result := strings.TrimSpace(strings.Join(parts, "\n"))
		if isStepi {
			result = stripReasoning(result, true)
		}
		return result
	}
	if len(fallbacks) > 0 {
		result := strings.TrimSpace(strings.Join(fallbacks, "\n"))
		if isStepi {
			result = stripReasoning(result, true)
		}
		return result
	}
	lines := strings.Split(stdout, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.TrimSpace(lines[i]) != "" {
			return strings.TrimSpace(stripReasoning(lines[i], isStepi))
		}
	}
	return ""
}

// Relay.extractReply adalah method yang hanya bisa digunakan di dalam Relay.
func (r *Relay) extractReply(stdout string) (result string) {
	isStepi := r.cfg.Agent == "stepi-chat"
	return extractReply(stdout, isStepi)
}

func collectTextFields(v any) []string {
	var out []string
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if k == "text" {
				if s, ok := t[k].(string); ok && s != "" {
					out = append(out, s)
					continue
				}
			}
			out = append(out, collectTextFields(t[k])...)
		}
	case []any:
		for _, item := range t {
			out = append(out, collectTextFields(item)...)
		}
	}
	return out
}

func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
}

// truncateBytes memotong string agar tidak melebihi max byte, dengan menjaga
// batas rune tetap valid (tidak memotong di tengah karakter UTF-8). Dipakai
// sebagai jaring pengaman ukuran prompt rangkuman.
func truncateBytes(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	cut := s[:max]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut + "\n\n[...output dipotong karena terlalu panjang...]"
}

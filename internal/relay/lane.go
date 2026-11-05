package relay

import (
	"strings"
	"sync"
	"unicode/utf8"
)

// Lane menentukan kanal (antrean + sesi) untuk satu pesan masuk.
type Lane string

const (
	// LaneChat mewakili chat WhatsApp ringan. Dijalankan pada sesi chat
	// miliknya sendiri (chat_session_file, default state/session.json) lewat
	// worker khusus, supaya ingatan percakapan tetap utuh dan balasan tidak
	// pernah menunggu tugas berat.
	LaneChat Lane = "chat"
	// LaneWork mewakili tugas berat (coding/eksekusi). Dijalankan pada
	// sesi/worker terpisah (work_session_file, atau sesi baru tiap task bila
	// tidak diset), sehingga tidak pernah menyentuh sesi chat.
	LaneWork Lane = "work"
)

// defaultHeavyKeywords memicu routing ke lane work. Dipilih dari istilah
// coding/eksekusi (EN+ID) supaya pesan obrolan santai tetap di lane chat dan
// tidak kehilangan ingatan percakapan karena salah klasifikasi.
var defaultHeavyKeywords = []string{
	"fix", "bug", "bugfix", "debug", "debugging", "debugger", "error", "crash",
	"exception", "traceback", "stacktrace", "stack trace", "hotfix", "patch",
	"refactor", "rebase", "rollback",
	"kode", "koding", "coding", "code", "script", "program", "programming",
	"function", "fungsi", "method", "class", "variable", "variabel", "syntax", "sintaks",
	"deploy", "deployment", "build", "compile", "kompilasi",
	"test", "testing", "install", "setup", "dependency", "dependensi",
	"commit", "push", "pull", "merge", "branch", "git", "repo", "repository",
	"server", "backend", "frontend", "docker", "container", "kubernetes", "nginx",
	"database", "query", "migration", "endpoint", "api",
	"config", "konfigurasi", "environment",
	"latency", "performa", "performance", "optimasi", "optimization",
	"file", "folder", "directory", "direktori",
	"python", "golang", "nodejs", "npm", "rust", "java", "typescript", "javascript",
	"readme", "dokumentasi", "issue", "relay", "webhook",
}

// defaultHandoverVerbs adalah kata kerja imperatif pemicu serah terima tugas
// ke tim teknis (stepi sebagai resepsionis), mis. "serahin aja tugasnya ke Pak
// Budi". Hanya kata kerja ini (plus handover_keywords dari config) yang
// memicu; nama target seperti "pak budi"/"tim teknis" BUKAN pemicu sendiri,
// supaya pertanyaan macam "pak budi udah kerja?" tidak ikut tertangkap.
// Perilaku ini terpisah dari routing otomatis (AutoWorkRouting).
var defaultHandoverVerbs = []string{
	"serahin", "serahkan", "serah", "limpahkan", "limpahin",
	"delegasikan", "delegasiin", "handover",
}

// Classify menentukan lane sebuah pesan. Secara default semuanya masuk lane
// chat (stepi resepsionis): tugas berat hanya diserahkan ke lane work saat bos
// memerintah eksplisit lewat frasa serah terima (lihat Config.IsHandover).
// Routing otomatis berdasarkan panjang/kata kunci (defaultHeavyKeywords +
// config heavy_keywords) hanya aktif bila cfg.AutoWorkRouting diset true.
func Classify(cfg *Config, text string) Lane {
	t := strings.TrimSpace(text)
	if t == "" || isFastCommand(cfg, t) {
		return LaneChat
	}
	if cfg == nil || !cfg.AutoWorkRouting {
		return LaneChat
	}
	maxLen := cfg.LightMaxLen
	if maxLen <= 0 {
		maxLen = defaultLightMaxLen
	}
	if utf8.RuneCountInString(t) > maxLen {
		return LaneWork
	}
	if re := cfg.HeavyPattern(); re != nil && re.MatchString(t) {
		return LaneWork
	}
	return LaneChat
}

// CommandPrefix mengembalikan awalan perintah yang aktif. Default "!".
func CommandPrefix(cfg *Config) string {
	if cfg != nil {
		if p := strings.TrimSpace(cfg.CommandPrefix); p != "" {
			return p
		}
	}
	return defaultCommandPrefix
}

// commandName memecah teks menjadi nama perintah (lowercase) bila diawali
// prefix. Boolean kedua menandakan teks memang berawalan prefix, termasuk
// saat tidak ada nama perintah setelahnya (mis. cuma "!").
func commandName(cfg *Config, text string) (string, bool) {
	fields := commandArgs(cfg, text)
	if len(fields) == 0 {
		return "", isPrefixed(cfg, text)
	}
	return strings.ToLower(fields[0]), true
}

// commandArgs mengembalikan argumen perintah tanpa prefix. Contoh:
// "!model 2" -> ["model", "2"].
func commandArgs(cfg *Config, text string) []string {
	prefix := CommandPrefix(cfg)
	t := strings.TrimSpace(text)
	if !strings.HasPrefix(t, prefix) {
		return nil
	}
	rest := strings.TrimSpace(strings.TrimPrefix(t, prefix))
	if rest == "" {
		return nil
	}
	return strings.Fields(rest)
}

func isPrefixed(cfg *Config, text string) bool {
	return strings.HasPrefix(strings.TrimSpace(text), CommandPrefix(cfg))
}

// isResetCommand melaporkan apakah teks adalah perintah untuk mereset sesi
// obrolan: "<prefix>reset" atau alias "<prefix>restart". "/reset" tetap
// diterima sebagai bentuk lama agar kebiasaan lama tidak langsung rusak.
func isResetCommand(cfg *Config, t string) bool {
	if name, ok := commandName(cfg, t); ok {
		return name == "reset" || name == "restart"
	}
	return strings.EqualFold(strings.TrimSpace(t), "/reset")
}

// isFastCommand melaporkan apakah teks adalah perintah fast-path (tanpa
// pemanggilan agent) yang ditangani langsung oleh handleMessage. Perintah
// dengan prefix yang tidak dikenal juga masuk fast-path supaya dibalas
// petunjuk, bukan dikirim ke agent.
func isFastCommand(cfg *Config, t string) bool {
	if _, ok := commandName(cfg, t); ok {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(t), "ping") || isResetCommand(cfg, t)
}

// sessionGate mengunci sesi berdasarkan kunci (id sesi, atau path store saat
// id masih kosong) sehingga tidak pernah ada dua `opencode run` pada sesi/id
// yang sama pada saat bersamaan. Kunci berbeda tetap bisa jalan paralel.
type sessionGate struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// lock mengambil kunci key dan mengembalikan fungsi unlock (aman dipanggil
// lebih dari sekali). Tidak mengunci apa pun bila key kosong.
func (g *sessionGate) lock(key string) func() {
	if key == "" {
		return func() {}
	}
	g.mu.Lock()
	if g.locks == nil {
		g.locks = make(map[string]*sync.Mutex)
	}
	m, ok := g.locks[key]
	if !ok {
		m = new(sync.Mutex)
		g.locks[key] = m
	}
	g.mu.Unlock()
	m.Lock()
	var once sync.Once
	return func() { once.Do(m.Unlock) }
}

package relay

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

type taskItem struct {
	ID   string `json:"id"`
	Text string `json:"text"`
	// Chat adalah target mark-read untuk pesan ini, sudah dihitung handleHook
	// lewat chatTarget (Chat -> SenderJID -> OwnerJID -> OwnerPhone).
	Chat string `json:"chat"`
	Lane Lane   `json:"lane"`
	// Media berisi metadata lampiran (foto/file) yang dikirim bos, atau nil
	// bila pesan murni teks. Lampirannya diunduh saat task dijalankan.
	Media *MediaInfo `json:"media,omitempty"`
}

// TaskRunner runs the agent for a prompt and returns raw stdout/stderr.
// sessionID, bila tidak kosong, melanjutkan sesi opencode yang sama.
type TaskRunner interface {
	Run(ctx context.Context, prompt, sessionID string) (stdout string, stderr string, err error)
}

type ExecRunner struct {
	Bin     string
	Workdir string
	BaseDir string
	Agent   string
	Model   string
	Timeout time.Duration

	modelMu sync.RWMutex
}

// args builds the `opencode run` argument list. Prompt TIDAK ikut di sini:
// prompt dikirim lewat stdin oleh Run supaya prompt raksasa (mis. rangkuman
// hasil build ratusan KB) tidak kena batas panjang satu argumen OS
// (MAX_ARG_STRLEN ~128KB) yang bikin fork/exec gagal dengan
// "argument list too long". Exposed for testing.
func (r *ExecRunner) args(sessionID string) []string {
	// OpenCode V2 menghapus flag --dir. Directory kerja sekarang ditentukan
	// lewat cwd proses (lihat Run), bukan argumen command.
	args := []string{"run", "--format", "json", "--title", "wa-relay"}
	if r.Agent != "" {
		args = append(args, "--agent", r.Agent)
	}
	if model := r.CurrentModel(); model != "" {
		args = append(args, "--model", model)
	}
	if sessionID != "" {
		args = append(args, "--session", sessionID)
	}
	return args
}

func (r *ExecRunner) workdir() string {
	if r.Workdir != "" {
		return r.Workdir
	}
	return r.BaseDir
}

// WorkdirPath mengembalikan working directory runner untuk ditampilkan pada
// status worker. Runner lain boleh tidak mengimplementasikan ini.
func (r *ExecRunner) WorkdirPath() string {
	return r.workdir()
}

// CurrentModel mengembalikan model yang sedang dipakai runner.
func (r *ExecRunner) CurrentModel() string {
	r.modelMu.RLock()
	defer r.modelMu.RUnlock()
	return r.Model
}

// SetModel mengganti model runner untuk pemanggilan berikutnya.
func (r *ExecRunner) SetModel(model string) {
	r.modelMu.Lock()
	r.Model = model
	r.modelMu.Unlock()
}

func (r *ExecRunner) Run(ctx context.Context, prompt, sessionID string) (string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.Bin, r.args(sessionID)...)
	cmd.Dir = r.workdir()
	// Prompt lewat stdin: `opencode run` membaca pesan dari stdin bila tidak ada
	// positional message, jadi prompt besar tidak dibatasi argumen OS.
	cmd.Stdin = strings.NewReader(prompt)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return stdout.String(), stderr.String(), context.DeadlineExceeded
	}
	return stdout.String(), stderr.String(), err
}

// Sender delivers a reply message to the owner.
type Sender interface {
	Send(text string) error
	// SendFile mengirim sebuah file/foto sebagai lampiran WhatsApp. caption
	// boleh kosong.
	SendFile(path, caption string) error
}

type WacliSender struct {
	Bin   string
	Phone string
}

func (s *WacliSender) Send(text string) error {
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	return runCommand(ctx, s.Bin, "send", "text", "--to", s.Phone, "--message", text)
}

func (s *WacliSender) SendFile(path, caption string) error {
	ctx, cancel := context.WithTimeout(context.Background(), sendFileTimeout)
	defer cancel()
	args := []string{"send", "file", "--to", s.Phone, "--file", path}
	if strings.TrimSpace(caption) != "" {
		args = append(args, "--caption", caption)
	}
	return runCommand(ctx, s.Bin, args...)
}

// MarkReader menandai satu chat sebagai sudah dibaca (read receipt).
type MarkReader interface {
	MarkRead(chat string) error
}

type WacliMarkReader struct {
	Bin string
}

// args builds the `wacli chats mark-read` argument list. Exposed for testing.
// Flag --receipts yang mengirim read receipt diserahkan sepenuhnya ke wacli,
// termasuk soal privasi read receipt milik user.
func (m *WacliMarkReader) args(chat string) []string {
	return []string{"chats", "mark-read", "--chat", chat, "--receipts"}
}

func (m *WacliMarkReader) MarkRead(chat string) error {
	ctx, cancel := context.WithTimeout(context.Background(), markReadTimeout)
	defer cancel()
	return runCommand(ctx, m.Bin, m.args(chat)...)
}

// runCommand menjalankan sebuah command dan, bila gagal, mengembalikan error
// yang memuat potongan output untuk diagnosis.
func runCommand(ctx context.Context, name string, args ...string) error {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err == nil {
		return nil
	}
	if detail := truncateRunes(strings.TrimSpace(string(out)), stderrMaxRune); detail != "" {
		return fmt.Errorf("%v: %s", err, detail)
	}
	return err
}

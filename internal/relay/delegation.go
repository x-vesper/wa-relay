package relay

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

// confirmationRe cocok balasan singkat yang berarti "setuju" dari bos.
var confirmationRe = regexp.MustCompile(`(?i)^\s*(ya|iya|setuju|oke|ok|gas|lanjut|proses|approve|yes|y)\b`)

// Delegation adalah tugas berat yang menunggu persetujuan bos sebelum
// didelegasikan ke agent teknis.
type Delegation struct {
	ID        string    `json:"id"`
	Text      string    `json:"text"`
	Chat      string    `json:"chat"`
	CreatedAt time.Time `json:"created_at"`
}

func (d *Delegation) expired(ttl time.Duration) bool {
	return d == nil || (ttl > 0 && time.Since(d.CreatedAt) > ttl)
}

// DelegationStore menyimpan satu delegasi pending per chat dalam file JSON
// agar survive restart.
type DelegationStore struct {
	mu   sync.Mutex
	path string
	ttl  time.Duration
	d    *Delegation
}

func LoadDelegationStore(path string, ttl time.Duration) *DelegationStore {
	s := &DelegationStore{path: path, ttl: ttl}
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		return s
	}
	var d Delegation
	if jerr := json.Unmarshal(b, &d); jerr != nil {
		return s
	}
	s.d = &d
	return s
}

func (s *DelegationStore) Get(chat string) (*Delegation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.d == nil || s.d.Chat != chat || s.d.expired(s.ttl) {
		return nil, false
	}
	return s.d, true
}

func (s *DelegationStore) Set(d *Delegation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.d = d
	s.persist()
}

func (s *DelegationStore) Clear(chat string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.d != nil && s.d.Chat == chat {
		s.d = nil
		s.persist()
	}
}

func (s *DelegationStore) persist() {
	if s.path == "" {
		return
	}
	var b []byte
	var err error
	if s.d != nil {
		b, err = json.MarshalIndent(s.d, "", "  ")
		if err != nil {
			return
		}
	}
	dir := filepath.Dir(s.path)
	_ = os.MkdirAll(dir, 0o700)
	_ = writeFileAtomic(s.path, b, 0o600)
}

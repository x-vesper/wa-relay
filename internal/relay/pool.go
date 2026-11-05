package relay

import "sync"

// runnerPool adalah kumpulan runner untuk lane work, masing-masing punya
// working directory sendiri. Slot dipegang selama satu build berjalan, jadi
// jumlah runner = jumlah build yang boleh paralel. Akuisisi bersifat
// non-blocking: kalau semua slot terpakai, pemanggil bisa langsung memberi tahu
// bos bahwa tim sedang sibuk (bukan menunggu diam-diam di antrean).
type runnerPool struct {
	mu      sync.Mutex
	runners []TaskRunner
	busy    []bool
}

func newRunnerPool(runners []TaskRunner) *runnerPool {
	if len(runners) == 0 {
		return nil
	}
	return &runnerPool{runners: runners, busy: make([]bool, len(runners))}
}

// acquire mengambil satu slot bebas. ok=false berarti semua slot terpakai.
// release mengembalikan slot ke pool (aman dipanggil lebih dari sekali).
func (p *runnerPool) acquire() (r TaskRunner, release func(), ok bool) {
	if p == nil {
		return nil, func() {}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.runners {
		if !p.busy[i] {
			p.busy[i] = true
			idx := i
			var once sync.Once
			return p.runners[idx], func() {
				once.Do(func() {
					p.mu.Lock()
					p.busy[idx] = false
					p.mu.Unlock()
				})
			}, true
		}
	}
	return nil, func() {}, false
}

// available melaporkan apakah masih ada slot bebas (tanpa memesan).
func (p *runnerPool) available() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, b := range p.busy {
		if !b {
			return true
		}
	}
	return false
}

// size mengembalikan jumlah slot.
func (p *runnerPool) size() int {
	if p == nil {
		return 0
	}
	return len(p.runners)
}

// poolSlot adalah snapshot satu slot build untuk ditampilkan ke bos.
type poolSlot struct {
	Workdir string
	Busy    bool
}

// status mengembalikan snapshot semua slot tanpa memesan atau mengubah state.
func (p *runnerPool) status() []poolSlot {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]poolSlot, len(p.runners))
	for i, runner := range p.runners {
		dir := ""
		if w, ok := runner.(interface{ WorkdirPath() string }); ok {
			dir = w.WorkdirPath()
		}
		out[i] = poolSlot{Workdir: dir, Busy: p.busy[i]}
	}
	return out
}

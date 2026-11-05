package relay

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRunnerPoolAcquireRelease(t *testing.T) {
	pool := newRunnerPool([]TaskRunner{&recordingRunner{}, &recordingRunner{}})
	if pool.size() != 2 {
		t.Fatalf("size = %d, want 2", pool.size())
	}

	_, release1, ok1 := pool.acquire()
	_, release2, ok2 := pool.acquire()
	if !ok1 || !ok2 {
		t.Fatalf("dua slot pertama harus bebas: ok1=%v ok2=%v", ok1, ok2)
	}
	if pool.available() {
		t.Fatal("pool penuh tapi available() = true")
	}
	if _, release3, ok3 := pool.acquire(); ok3 {
		release3()
		t.Fatal("akuisisi ketiga harus gagal saat dua slot terpakai")
	}

	release1()
	release1() // idempoten
	if !pool.available() {
		t.Fatal("slot yang dilepas harus tersedia lagi")
	}
	release2()
}

// Saat semua slot build terpakai, stepi (chat) yang mengabari bos, dan tugas
// TIDAK disimpan sebagai delegasi pending.
func TestRelayBusyWhenAllBuildSlotsUsed(t *testing.T) {
	cfg := testConfig(t)
	chatRunner := &stubRunner{stdout: `{"type":"text","part":{"text":"bos, tim lagi sibuk semua"}}`}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, chatRunner, sender)
	r.SetWorkPool([]TaskRunner{&recordingRunner{}})
	chat := "6281234567890@s.whatsapp.net"

	_, release, ok := r.acquireWork()
	if !ok {
		t.Fatal("slot pertama harus bebas")
	}
	defer release()

	r.handleMessage(context.Background(), taskItem{ID: "w1", Text: "coding berat", Lane: LaneWork, Chat: chat})

	got := sender.messages()
	if len(got) != 1 || !strings.Contains(got[0], "sibuk") {
		t.Fatalf("sent = %v, want pesan tim sibuk dari stepi", got)
	}
	if _, ok := r.delegations.Get(chat); ok {
		t.Fatal("delegasi tidak boleh disimpan saat tim sibuk")
	}
	if len(chatRunner.sessions) != 1 {
		t.Fatalf("pesan sibuk harus lewat stepi (chat runner), calls=%d", len(chatRunner.sessions))
	}
}

// Kalau slot terpakai tepat saat bos konfirmasi, build tidak dijalankan dan bos
// tetap dapat kabar sibuk.
func TestRelayBusyOnConfirmWhenSlotsTaken(t *testing.T) {
	cfg := testConfig(t)
	chatRunner := &stubRunner{stdout: `{"type":"text","part":{"text":"tim lagi sibuk semua bos"}}`}
	workRunner := &recordingRunner{}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, chatRunner, sender)
	r.SetWorkPool([]TaskRunner{workRunner})
	chat := "6281234567890@s.whatsapp.net"

	_, release, ok := r.acquireWork()
	if !ok {
		t.Fatal("slot harus bebas")
	}
	defer release()

	r.delegations.Set(&Delegation{ID: "w1", Text: "buat fitur", Chat: chat, CreatedAt: time.Now()})
	r.handleMessage(context.Background(), taskItem{ID: "c1", Text: "setuju", Lane: LaneChat, Chat: chat})

	deadline := time.After(2 * time.Second)
	for {
		if msgs := sender.messages(); len(msgs) > 0 {
			if !strings.Contains(msgs[len(msgs)-1], "sibuk") {
				t.Fatalf("sent = %v, want pesan sibuk", msgs)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("bos tidak dikabari saat slot build penuh")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if workRunner.count() != 0 {
		t.Fatalf("build tidak boleh jalan saat slot penuh, calls=%d", workRunner.count())
	}
}

// Build memakai slot pool lalu melepasnya, sehingga build berikutnya bisa jalan.
func TestRelayBuildUsesAndReleasesPoolSlot(t *testing.T) {
	cfg := testConfig(t)
	runner := &echoRunner{}
	sender := &stubSender{}
	r := newTestRelay(t, cfg, runner, sender)
	r.SetWorkPool([]TaskRunner{runner})
	chat := "6281234567890@s.whatsapp.net"

	ctx := context.Background()
	r.handleMessage(ctx, taskItem{ID: "w1", Text: "buat fitur X", Lane: LaneWork, Chat: chat})
	r.handleMessage(ctx, taskItem{ID: "c1", Text: "setuju", Lane: LaneChat, Chat: chat})

	deadline := time.After(2 * time.Second)
	for runner.nth(2) == "" {
		select {
		case <-deadline:
			t.Fatal("delegasi tidak selesai")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if got := runner.nth(1); !strings.Contains(got, "Selesaikan sebaik-baiknya") {
		t.Fatalf("panggilan build tidak lewat pool: %q", got)
	}
	if !r.workAvailable() {
		t.Fatal("slot build tidak dilepas setelah build selesai")
	}
}

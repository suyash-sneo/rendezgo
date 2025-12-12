package workdist

import (
	"context"
	"testing"
	"time"

	"github.com/you/workdist/internal/fakestore"
)

func TestCoordinatorAcquiresAndReleases(t *testing.T) {
	store := fakestore.New()
	provider := NewStaticParallelismProvider(map[string]int{"alpha": 1})
	factory := &recordingFactory{started: make(chan string, 2)}

	cfg := DefaultConfig()
	cfg.NodeHeartbeatPeriod = 10 * time.Millisecond
	cfg.NodeTTL = 500 * time.Millisecond
	cfg.LeaseTTL = 300 * time.Millisecond
	cfg.LeaseRenewPeriod = 80 * time.Millisecond
	cfg.ReconcilePeriod = 20 * time.Millisecond
	cfg.JoinWarmup = 0
	cfg.MinSlotRuntime = 0
	cfg.SlotMoveCooldown = 0
	cfg.ReleasePerMinute = 10

	c, err := NewCoordinator(cfg, store, provider, factory, staticNodeID("node-1"), NopLogger(), NopMetrics())
	if err != nil {
		t.Fatalf("new coordinator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	go func() {
		_ = c.Run(ctx)
	}()

	select {
	case <-factory.started:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("slot runner did not start in time")
	}

	owner, ok, err := store.GetLeaseOwner(context.Background(), "lease:alpha|0")
	if err != nil || !ok || owner != "node-1" {
		t.Fatalf("expected lease owned by node-1, got ok=%v owner=%s err=%v", ok, owner, err)
	}

	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)

	_, ok, err = store.GetLeaseOwner(context.Background(), "lease:alpha|0")
	if err != nil {
		t.Fatalf("get lease owner after stop err: %v", err)
	}
	if ok {
		t.Fatalf("expected lease to be released on shutdown")
	}
}

type recordingFactory struct {
	started chan string
}

func (f *recordingFactory) NewSlotRunner(slot Slot) (SlotRunner, error) {
	select {
	case f.started <- slot.Key():
	default:
	}
	return &testRunner{}, nil
}

type testRunner struct{}

func (r *testRunner) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (r *testRunner) Stop(ctx context.Context) error {
	_ = ctx
	return nil
}

type staticNodeID string

func (n staticNodeID) NodeID() (string, error) { return string(n), nil }

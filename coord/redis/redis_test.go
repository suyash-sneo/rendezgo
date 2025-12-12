package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestHeartbeatAndListNodes(t *testing.T) {
	store, mr := newStore(t)
	defer store.Close()

	ctx := context.Background()
	if err := store.HeartbeatNode(ctx, "node-1", 2*time.Second); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0] != "node-1" {
		t.Fatalf("unexpected nodes: %v", nodes)
	}

	mr.FastForward(3 * time.Second)
	nodes, err = store.ListNodes(ctx)
	if err != nil {
		t.Fatalf("list nodes after expiry: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected nodes to be filtered after expiry, got %v", nodes)
	}
}

func TestPruneDeadNodes(t *testing.T) {
	store, mr := newStore(t)
	defer store.Close()

	ctx := context.Background()
	if err := store.HeartbeatNode(ctx, "node-1", time.Second); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	mr.FastForward(2 * time.Second)

	removed, err := store.PruneDeadNodes(ctx)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
}

func TestLeaseAcquireRenewRelease(t *testing.T) {
	store, mr := newStore(t)
	defer store.Close()

	ctx := context.Background()
	ok, err := store.AcquireLease(ctx, "lease:slot1", "node-1", time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire failed: ok=%v err=%v", ok, err)
	}

	ok, err = store.AcquireLease(ctx, "lease:slot1", "node-2", time.Second)
	if err != nil {
		t.Fatalf("second acquire err: %v", err)
	}
	if ok {
		t.Fatalf("expected second acquire to fail while held")
	}

	ok, err = store.RenewLease(ctx, "lease:slot1", "node-1", 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("renew failed: ok=%v err=%v", ok, err)
	}

	ok, err = store.RenewLease(ctx, "lease:slot1", "node-2", time.Second)
	if err != nil {
		t.Fatalf("renew wrong owner err: %v", err)
	}
	if ok {
		t.Fatalf("expected renew with wrong owner to fail")
	}

	ok, err = store.ReleaseLease(ctx, "lease:slot1", "node-1")
	if err != nil {
		t.Fatalf("release err: %v", err)
	}
	if !ok {
		t.Fatalf("expected release to succeed")
	}

	ok, err = store.AcquireLease(ctx, "lease:slot1", "node-2", time.Second)
	if err != nil || !ok {
		t.Fatalf("acquire after release failed: ok=%v err=%v", ok, err)
	}

	mr.FastForward(1500 * time.Millisecond)
	owner, found, err := store.GetLeaseOwner(ctx, "lease:slot1")
	if err != nil {
		t.Fatalf("get owner err: %v", err)
	}
	if found {
		t.Fatalf("expected lease expired, found owner %q", owner)
	}
}

func TestConfigCache(t *testing.T) {
	store, mr := newStore(t)
	defer store.Close()

	ctx := context.Background()
	if err := store.PutConfig(ctx, "cfg:workA", []byte("12"), time.Second); err != nil {
		t.Fatalf("put config: %v", err)
	}

	val, ok, err := store.GetConfig(ctx, "cfg:workA")
	if err != nil || !ok || string(val) != "12" {
		t.Fatalf("unexpected config get: val=%s ok=%v err=%v", string(val), ok, err)
	}

	mr.FastForward(2 * time.Second)
	_, ok, err = store.GetConfig(ctx, "cfg:workA")
	if err != nil {
		t.Fatalf("get config after expiry err: %v", err)
	}
	if ok {
		t.Fatalf("expected config to expire")
	}
}

func newStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	store, err := New(Options{Addr: mr.Addr()})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return store, mr
}

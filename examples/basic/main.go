package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/suyash-sneo/rendezgo/pkg/rendezgo"
)

type demoRunner struct {
	slot rendezgo.Slot
}

func (r *demoRunner) Run(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Printf("[slot %s] doing work", r.slot.Key())
		}
	}
}
func (r *demoRunner) Stop(ctx context.Context) error { return nil }

type demoFactory struct{}

func (demoFactory) NewConsumer(slot rendezgo.Slot) (rendezgo.Consumer, error) {
	return &demoRunner{slot: slot}, nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	cfg := rendezgo.DefaultConfig()
	cfg.ClusterID = "example"
	cfg.NodeID = "basic"
	cfg.StaticWorkloads = []rendezgo.WorkloadConfig{
		{Name: "workload.A", Units: 2},
	}

	ctrl, err := rendezgo.NewController(cfg, client, demoFactory{}, rendezgo.NopLogger(), rendezgo.NopMetrics())
	if err != nil {
		log.Fatalf("controller: %v", err)
	}
	if err := ctrl.Start(ctx); err != nil && ctx.Err() == nil {
		log.Printf("controller exited: %v", err)
		os.Exit(1)
	}
}

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/suyash-sneo/rendez/pkg/rendez"
)

type demoRunner struct {
	slot rendez.Slot
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

func (demoFactory) NewConsumer(slot rendez.Slot) (rendez.Consumer, error) {
	return &demoRunner{slot: slot}, nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	cfg := rendez.DefaultConfig()
	cfg.ClusterID = "example"
	cfg.NodeID = "basic"
	cfg.StaticTopics = []rendez.TopicConfig{
		{Name: "topic.A", Partitions: 2},
	}

	ctrl, err := rendez.NewController(cfg, client, demoFactory{}, rendez.NopLogger(), rendez.NopMetrics())
	if err != nil {
		log.Fatalf("controller: %v", err)
	}
	if err := ctrl.Start(ctx); err != nil && ctx.Err() == nil {
		log.Printf("controller exited: %v", err)
		os.Exit(1)
	}
}

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/you/workdist"
	"github.com/you/workdist/coord/redis"
)

type demoRunner struct {
	slot workdist.Slot
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

func (f *demoFactory) NewSlotRunner(slot workdist.Slot) (workdist.SlotRunner, error) {
	return &demoRunner{slot: slot}, nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := redis.New(redis.Options{Addr: "127.0.0.1:6379"})
	if err != nil {
		log.Fatalf("redis store: %v", err)
	}
	provider := workdist.NewStaticParallelismProvider(map[string]int{
		"topic.A": 2,
	})

	cfg := workdist.DefaultConfig()
	coord, err := workdist.NewCoordinator(cfg, store, provider, &demoFactory{}, workdist.NewDefaultNodeIDProvider(), workdist.NopLogger(), workdist.NopMetrics())
	if err != nil {
		log.Fatalf("coordinator: %v", err)
	}
	if err := coord.Run(ctx); err != nil && ctx.Err() == nil {
		log.Printf("coordinator exited: %v", err)
		os.Exit(1)
	}
}

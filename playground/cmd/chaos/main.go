package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/you/workdist"
	store "github.com/you/workdist/coord/redis"
)

func main() {
	var (
		nodes     = flag.Int("nodes", 2, "number of local workers to run")
		workClass = flag.String("work-class", "demo.topic.A", "work class name")
		parallel  = flag.Int("parallelism", 4, "desired parallelism for the work class")
		redisAddr = flag.String("redis", "127.0.0.1:6379", "redis address")
		verbose   = flag.Bool("v", false, "enable verbose runner logging")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	storeClient, err := store.New(store.Options{Addr: *redisAddr})
	if err != nil {
		log.Fatalf("redis: %v", err)
	}

	provider := workdist.NewStaticParallelismProvider(map[string]int{*workClass: *parallel})
	cfg := workdist.DefaultConfig()
	cfg.JoinWarmup = 0

	var wg sync.WaitGroup
	for i := 0; i < *nodes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("node-%d", idx)
			coord, err := workdist.NewCoordinator(cfg, storeClient, provider, &playgroundFactory{verbose: *verbose}, fixedNodeID(nodeID), workdist.NopLogger(), workdist.NopMetrics())
			if err != nil {
				log.Printf("[%s] coordinator init failed: %v", nodeID, err)
				return
			}
			log.Printf("[%s] starting", nodeID)
			if err := coord.Run(ctx); err != nil && ctx.Err() == nil {
				log.Printf("[%s] coordinator exited: %v", nodeID, err)
			}
		}(i)
	}

	wg.Wait()
	log.Println("playground exit")
}

type playgroundFactory struct {
	verbose bool
}

func (f *playgroundFactory) NewSlotRunner(slot workdist.Slot) (workdist.SlotRunner, error) {
	return &playgroundRunner{
		slot:    slot,
		verbose: f.verbose,
	}, nil
}

type playgroundRunner struct {
	slot    workdist.Slot
	verbose bool
}

func (r *playgroundRunner) Run(ctx context.Context) error {
	ticker := time.NewTicker(750 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if r.verbose {
				log.Printf("[slot %s] tick", r.slot.Key())
			}
		}
	}
}

func (r *playgroundRunner) Stop(ctx context.Context) error {
	_ = ctx
	return nil
}

type fixedNodeID string

func (f fixedNodeID) NodeID() (string, error) { return string(f), nil }

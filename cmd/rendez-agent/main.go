package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/suyash-sneo/rendezgo/pkg/rendezgo"
)

func main() {
	var (
		redisAddr     string
		workloadsFlag string
		nodeID        string
		clusterID     string
		verbose       bool
	)

	flag.StringVar(&redisAddr, "redis", "127.0.0.1:6379", "redis address")
	flag.StringVar(&workloadsFlag, "workloads", "demo:4", "workload=units pairs (comma-separated)")
	flag.StringVar(&nodeID, "node", "", "node id suffix (defaults to hostname)")
	flag.StringVar(&clusterID, "cluster", "agent", "cluster id prefix")
	flag.BoolVar(&verbose, "v", false, "verbose logging")
	flag.Parse()

	workloads := parseWorkloads(workloadsFlag)
	if len(workloads) == 0 {
		workloads = []rendezgo.WorkloadConfig{{Name: "demo", Units: 4}}
	}

	cfg := rendezgo.DefaultConfig()
	cfg.ClusterID = clusterID
	cfg.NodeID = nodeID
	cfg.StaticWorkloads = workloads

	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	logger := stdLogger{verbose: verbose}
	factory := logConsumerFactory{logger: logger}

	ctrl, err := rendezgo.NewController(cfg, client, factory, logger, rendezgo.NopMetrics())
	if err != nil {
		log.Fatalf("controller: %v", err)
	}

	ctx, cancel := signalContext()
	defer cancel()
	if err := ctrl.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("controller exited: %v", err)
	}
}

func parseWorkloads(flagVal string) []rendezgo.WorkloadConfig {
	parts := strings.Split(flagVal, ",")
	var workloads []rendezgo.WorkloadConfig
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			continue
		}
		kv := strings.SplitN(p, ":", 2)
		if len(kv) != 2 {
			continue
		}
		var units int
		fmt.Sscanf(kv[1], "%d", &units)
		if units <= 0 {
			continue
		}
		workloads = append(workloads, rendezgo.WorkloadConfig{Name: kv[0], Units: units})
	}
	return workloads
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		cancel()
	}()
	return ctx, cancel
}

type stdLogger struct {
	verbose bool
}

func (l stdLogger) Debug(msg string, fields ...rendezgo.Field) {
	if l.verbose {
		log.Print(format(msg, fields...))
	}
}

func (l stdLogger) Info(msg string, fields ...rendezgo.Field) { log.Print(format(msg, fields...)) }
func (l stdLogger) Warn(msg string, fields ...rendezgo.Field) {
	log.Print("WARN: " + format(msg, fields...))
}
func (l stdLogger) Error(msg string, fields ...rendezgo.Field) {
	log.Print("ERROR: " + format(msg, fields...))
}

func format(msg string, fields ...rendezgo.Field) string {
	if len(fields) == 0 {
		return msg
	}
	parts := make([]string, 0, len(fields))
	for _, f := range fields {
		parts = append(parts, fmt.Sprintf("%s=%v", f.Key, f.Value))
	}
	return msg + " " + strings.Join(parts, " ")
}

type logConsumerFactory struct {
	logger rendezgo.Logger
}

func (f logConsumerFactory) NewConsumer(slot rendezgo.Slot) (rendezgo.Consumer, error) {
	f.logger.Info("starting consumer", rendezgo.Field{Key: "slot", Value: slot.Key()})
	return &logConsumer{logger: f.logger, slot: slot.Key()}, nil
}

type logConsumer struct {
	logger rendezgo.Logger
	slot   string
}

func (c *logConsumer) Run(ctx context.Context) error {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			c.logger.Debug("consuming", rendezgo.Field{Key: "slot", Value: c.slot})
		}
	}
}

func (c *logConsumer) Stop(ctx context.Context) error {
	c.logger.Info("stopping consumer", rendezgo.Field{Key: "slot", Value: c.slot})
	return nil
}

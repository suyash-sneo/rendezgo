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

	"github.com/suyash-sneo/rendez/pkg/rendez"
)

func main() {
	var (
		redisAddr  string
		topicsFlag string
		nodeID     string
		clusterID  string
		verbose    bool
	)

	flag.StringVar(&redisAddr, "redis", "127.0.0.1:6379", "redis address")
	flag.StringVar(&topicsFlag, "topics", "demo:4", "topic=partitions pairs (comma-separated)")
	flag.StringVar(&nodeID, "node", "", "node id suffix (defaults to hostname)")
	flag.StringVar(&clusterID, "cluster", "agent", "cluster id prefix")
	flag.BoolVar(&verbose, "v", false, "verbose logging")
	flag.Parse()

	topics := parseTopics(topicsFlag)
	if len(topics) == 0 {
		topics = []rendez.TopicConfig{{Name: "demo", Partitions: 4}}
	}

	cfg := rendez.DefaultConfig()
	cfg.ClusterID = clusterID
	cfg.NodeID = nodeID
	cfg.StaticTopics = topics

	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	logger := stdLogger{verbose: verbose}
	factory := logConsumerFactory{logger: logger}

	ctrl, err := rendez.NewController(cfg, client, factory, logger, rendez.NopMetrics())
	if err != nil {
		log.Fatalf("controller: %v", err)
	}

	ctx, cancel := signalContext()
	defer cancel()
	if err := ctrl.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("controller exited: %v", err)
	}
}

func parseTopics(flagVal string) []rendez.TopicConfig {
	parts := strings.Split(flagVal, ",")
	var topics []rendez.TopicConfig
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			continue
		}
		kv := strings.SplitN(p, ":", 2)
		if len(kv) != 2 {
			continue
		}
		var partitions int
		fmt.Sscanf(kv[1], "%d", &partitions)
		if partitions <= 0 {
			continue
		}
		topics = append(topics, rendez.TopicConfig{Name: kv[0], Partitions: partitions})
	}
	return topics
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

func (l stdLogger) Debug(msg string, fields ...rendez.Field) {
	if l.verbose {
		log.Print(format(msg, fields...))
	}
}

func (l stdLogger) Info(msg string, fields ...rendez.Field) { log.Print(format(msg, fields...)) }
func (l stdLogger) Warn(msg string, fields ...rendez.Field) {
	log.Print("WARN: " + format(msg, fields...))
}
func (l stdLogger) Error(msg string, fields ...rendez.Field) {
	log.Print("ERROR: " + format(msg, fields...))
}

func format(msg string, fields ...rendez.Field) string {
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
	logger rendez.Logger
}

func (f logConsumerFactory) NewConsumer(slot rendez.Slot) (rendez.Consumer, error) {
	f.logger.Info("starting consumer", rendez.Field{Key: "slot", Value: slot.Key()})
	return &logConsumer{logger: f.logger, slot: slot.Key()}, nil
}

type logConsumer struct {
	logger rendez.Logger
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
			c.logger.Debug("consuming", rendez.Field{Key: "slot", Value: c.slot})
		}
	}
}

func (c *logConsumer) Stop(ctx context.Context) error {
	c.logger.Info("stopping consumer", rendez.Field{Key: "slot", Value: c.slot})
	return nil
}

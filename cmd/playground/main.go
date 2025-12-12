package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"

	"github.com/suyash-sneo/rendez/pkg/rendez"
)

func main() {
	var (
		mode         string
		redisAddr    string
		topicsFlag   string
		initialNodes int
		scenario     string
		scenarioFile string
	)
	flag.StringVar(&mode, "mode", "simulated", "simulated or real")
	flag.StringVar(&redisAddr, "redis", "127.0.0.1:6379", "redis address for real mode")
	flag.StringVar(&topicsFlag, "topics", "demo:8", "topics as topic:partitions,...")
	flag.IntVar(&initialNodes, "nodes", 3, "initial node count")
	flag.StringVar(&scenario, "scenario", "", "built-in scenario to run")
	flag.StringVar(&scenarioFile, "scenario-file", "", "path to YAML/JSON scenario file")
	flag.Parse()

	topics := parseTopics(topicsFlag)
	if len(topics) == 0 {
		topics = []rendez.TopicConfig{{Name: "demo", Partitions: 8}}
	}

	ctx, cancel := signalContext()
	defer cancel()

	sim, err := newSimulation(ctx, mode, redisAddr, topics)
	if err != nil {
		fmt.Fprintf(os.Stderr, "playground init: %v\n", err)
		os.Exit(1)
	}
	for i := 0; i < initialNodes; i++ {
		sim.addNode(1.0)
	}

	if scenario != "" {
		go sim.runScenario(ctx, scenario)
	}
	if scenarioFile != "" {
		go sim.runScenarioFile(ctx, scenarioFile)
	}

	go sim.dashboard(ctx)
	fmt.Println("Chaos playground ready. Type 'help' for commands.")
	sim.repl(ctx)
}

type simulation struct {
	template rendez.Config
	topics   []rendez.TopicConfig

	mu        sync.Mutex
	nodes     map[string]*simNode
	weights   *weightTable
	admin     *redis.Client
	redisAddr string
	server    *miniredis.Miniredis

	owners map[string]string
	churn  []time.Time
}

type simNode struct {
	id      string
	weight  float64
	cancel  context.CancelFunc
	ctrl    *rendez.Controller
	client  *redis.Client
	hook    *chaosHook
	health  *healthGate
	healthy bool
}

func newSimulation(ctx context.Context, mode, redisAddr string, topics []rendez.TopicConfig) (*simulation, error) {
	cfg := rendez.DefaultConfig()
	cfg.ClusterID = "sim"
	cfg.ReconcileInterval = 2 * time.Second
	cfg.ReconcileJitter = 0.1
	cfg.LeaseTTL = 10 * time.Second
	cfg.LeaseRenewInterval = 3 * time.Second
	cfg.HeartbeatInterval = 2 * time.Second
	cfg.HeartbeatTTL = 12 * time.Second
	cfg.MinConsumerRuntime = 2 * time.Second
	cfg.SlotMoveCooldown = 8 * time.Second
	cfg.ConfigRefreshInterval = 10 * time.Second
	cfg.StaticTopics = topics
	cfg.AcquireLimit = 200

	sim := &simulation{
		template: cfg,
		topics:   topics,
		nodes:    map[string]*simNode{},
		weights:  newWeightTable(),
		owners:   map[string]string{},
	}
	if mode == "real" {
		sim.redisAddr = redisAddr
		sim.admin = redis.NewClient(&redis.Options{Addr: redisAddr})
	} else {
		server, err := miniredis.Run()
		if err != nil {
			return nil, err
		}
		sim.server = server
		sim.redisAddr = server.Addr()
		sim.admin = redis.NewClient(&redis.Options{Addr: server.Addr()})
	}
	go func() {
		<-ctx.Done()
		if sim.server != nil {
			sim.server.Close()
		}
	}()
	return sim, nil
}

func (s *simulation) addNode(weight float64) string {
	return s.spawnNode("", weight)
}

func (s *simulation) spawnNode(customID string, weight float64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := customID
	if id == "" {
		id = fmt.Sprintf("n-%d", len(s.nodes)+1)
	}
	cfg := s.template
	cfg.NodeID = id
	cfg.StaticWeights = nil
	client := redis.NewClient(&redis.Options{Addr: s.redisAddr})
	hook := &chaosHook{}
	client.AddHook(hook)
	health := &healthGate{healthy: true}
	factory := &playConsumerFactory{}
	ctrl, err := rendez.NewController(cfg, client, factory, rendez.NopLogger(), rendez.NopMetrics(), rendez.WithWeightProvider(s.weights), rendez.WithHealthChecker(health))
	if err != nil {
		fmt.Printf("add node: %v\n", err)
		return ""
	}
	nodeID := cfg.ClusterID + ":" + cfg.NodeID
	s.weights.Set(nodeID, weight)
	ctx, cancel := context.WithCancel(context.Background())
	go ctrl.Start(ctx)
	node := &simNode{
		id:      nodeID,
		weight:  weight,
		cancel:  cancel,
		ctrl:    ctrl,
		client:  client,
		hook:    hook,
		health:  health,
		healthy: true,
	}
	s.nodes[nodeID] = node
	return nodeID
}

func (s *simulation) removeNode(id string, graceful bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		fmt.Printf("node %s not found\n", id)
		return
	}
	node.ctrl.SetReleaseOnShutdown(graceful)
	node.cancel()
	delete(s.nodes, id)
	s.weights.Delete(id)
}

func (s *simulation) adjustWeight(id string, weight float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		fmt.Printf("node %s not found\n", id)
		return
	}
	node.weight = weight
	s.weights.Set(id, weight)
}

func (s *simulation) restartNode(id string) {
	s.mu.Lock()
	node, ok := s.nodes[id]
	weight := 1.0
	if ok {
		weight = node.weight
	}
	s.mu.Unlock()
	if !ok {
		fmt.Printf("node %s not found\n", id)
		return
	}
	s.removeNode(id, true)
	base := id
	if strings.Contains(id, ":") {
		parts := strings.Split(id, ":")
		base = parts[len(parts)-1]
	}
	newID := s.spawnNode(base, weight)
	fmt.Printf("restarted %s -> %s\n", id, newID)
}

func (s *simulation) restartAll() {
	for _, id := range s.nodeIDs() {
		s.restartNode(id)
	}
}

func (s *simulation) setRedisFault(id string, fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		fmt.Printf("node %s not found\n", id)
		return
	}
	node.hook.fail.Store(fail)
}

func (s *simulation) setHealth(id string, healthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	node, ok := s.nodes[id]
	if !ok {
		fmt.Printf("node %s not found\n", id)
		return
	}
	node.health.Set(healthy)
	node.healthy = healthy
}

func (s *simulation) dashboard(ctx context.Context) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.printDashboard()
		}
	}
}

func (s *simulation) printDashboard() {
	s.mu.Lock()
	nodes := make(map[string]*simNode, len(s.nodes))
	for k, v := range s.nodes {
		nodes[k] = v
	}
	s.mu.Unlock()

	owners := s.snapshotOwners()
	churn := s.updateChurn(owners)

	fmt.Print("\033[H\033[2J")
	fmt.Printf("redis: %s | nodes: %d | churn/min: %d\n", s.redisAddr, len(nodes), churn)
	fmt.Println("Nodes:")
	for _, n := range nodes {
		owned := len(n.ctrl.OwnedSlots())
		state := "ok"
		if n.ctrl.BackoffActive() {
			state = "backoff"
		}
		if !n.healthy {
			state = "unhealthy"
		}
		fmt.Printf("  %-10s weight=%.1f owned=%d state=%s\n", n.id, n.weight, owned, state)
	}
	fmt.Printf("Caps: max/pod=%d max/topic=%d\n", s.template.MaxConsumersPerPod, s.template.MaxConsumersPerTopicPerPod)

	fmt.Println("\nSlots:")
	total := 0
	matches := 0
	weights := s.weights.Snapshot()
	nodesList := make([]rendez.NodeWeight, 0, len(weights))
	for id, w := range weights {
		nodesList = append(nodesList, rendez.NodeWeight{ID: id, Weight: w})
	}
	for _, topic := range s.topics {
		fmt.Printf("  %s: ", topic.Name)
		for i := 0; i < topic.Partitions; i++ {
			slot := rendez.Slot{Topic: topic.Name, Index: i}
			key := fmt.Sprintf("lease:%s:%d", topic.Name, i)
			owner := owners[key]
			total++
			if expected, ok := rendez.RendezvousOwner(slot, nodesList); ok && expected == owner {
				matches++
			}
			fmt.Printf("%d[%s] ", i, shortOwner(owner))
		}
		fmt.Println()
	}
	progress := 0.0
	if total > 0 {
		progress = (float64(matches) / float64(total)) * 100
	}
	fmt.Printf("\nConvergence: %.1f%% aligned | Commands: help, add, remove, kill, weight, fail, health, scenario, load\n", progress)
}

func (s *simulation) snapshotOwners() map[string]string {
	owners := map[string]string{}
	for _, t := range s.topics {
		for i := 0; i < t.Partitions; i++ {
			key := fmt.Sprintf("lease:%s:%d", t.Name, i)
			val, err := s.admin.Get(context.Background(), key).Result()
			if err == nil {
				owners[key] = val
			}
		}
	}
	return owners
}

func (s *simulation) updateChurn(current map[string]string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, v := range current {
		if prev, ok := s.owners[k]; !ok || prev != v {
			s.churn = append(s.churn, now)
		}
	}
	cutoff := now.Add(-time.Minute)
	i := 0
	for ; i < len(s.churn); i++ {
		if s.churn[i].After(cutoff) {
			break
		}
	}
	if i > 0 {
		s.churn = s.churn[i:]
	}
	s.owners = current
	return len(s.churn)
}

func (s *simulation) repl(ctx context.Context) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		switch strings.ToLower(line) {
		case "help":
			fmt.Println("commands: add [n] [weight], remove <id>, restart <id>, kill <id>, weight <id> <w>, fail <id> on|off, health <id> on|off, shedding on|off, release <n>, scenario <name>, load <file>, quit")
			continue
		case "quit", "exit":
			return
		}
		s.handleCommand(ctx, line)
	}
}

func (s *simulation) handleCommand(ctx context.Context, line string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return
	}
	switch parts[0] {
	case "add":
		count := 1
		weight := 1.0
		if len(parts) > 1 {
			if v, err := strconv.Atoi(parts[1]); err == nil {
				count = v
			}
		}
		if len(parts) > 2 {
			if v, err := strconv.ParseFloat(parts[2], 64); err == nil {
				weight = v
			}
		}
		for i := 0; i < count; i++ {
			id := s.addNode(weight)
			fmt.Printf("added node %s\n", id)
		}
	case "remove":
		if len(parts) < 2 {
			fmt.Println("usage: remove <id>")
			return
		}
		s.removeNode(parts[1], true)
	case "restart":
		if len(parts) < 2 {
			fmt.Println("usage: restart <id>")
			return
		}
		s.restartNode(parts[1])
	case "kill":
		if len(parts) < 2 {
			fmt.Println("usage: kill <id>")
			return
		}
		s.removeNode(parts[1], false)
	case "weight":
		if len(parts) < 3 {
			fmt.Println("usage: weight <id> <weight>")
			return
		}
		w, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			fmt.Println("invalid weight")
			return
		}
		s.adjustWeight(parts[1], w)
	case "fail":
		if len(parts) < 3 {
			fmt.Println("usage: fail <id> on|off")
			return
		}
		s.setRedisFault(parts[1], parts[2] == "on")
	case "health":
		if len(parts) < 3 {
			fmt.Println("usage: health <id> on|off")
			return
		}
		s.setHealth(parts[1], parts[2] == "on")
	case "scenario":
		if len(parts) < 2 {
			fmt.Println("usage: scenario <name>")
			return
		}
		go s.runScenario(ctx, parts[1])
	case "shedding":
		if len(parts) < 2 {
			fmt.Println("usage: shedding on|off")
			return
		}
		s.template.SheddingEnabled = parts[1] == "on"
		s.restartAll()
	case "release":
		if len(parts) < 2 {
			fmt.Println("usage: release <per-interval>")
			return
		}
		if v, err := strconv.Atoi(parts[1]); err == nil {
			s.template.SheddingRelease = v
			s.restartAll()
		}
	case "load":
		if len(parts) < 2 {
			fmt.Println("usage: load <file>")
			return
		}
		go s.runScenarioFile(ctx, parts[1])
	default:
		fmt.Println("unknown command")
	}
}

func (s *simulation) runScenario(ctx context.Context, name string) {
	fmt.Printf("running scenario %s\n", name)
	switch name {
	case "scale-out":
		s.addNode(1)
		time.Sleep(1 * time.Second)
		for i := 0; i < 5; i++ {
			s.addNode(1)
		}
		time.Sleep(2 * time.Second)
		for i := 0; i < 10; i++ {
			s.addNode(1)
		}
		s.logSummary(name)
	case "pod-death":
		nodes := s.nodeIDs()
		if len(nodes) == 0 {
			return
		}
		target := len(nodes) / 2
		rand.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
		for i := 0; i < target; i++ {
			s.removeNode(nodes[i], false)
		}
		s.logSummary(name)
	case "redis-instability":
		nodes := s.nodeIDs()
		for i, id := range nodes {
			if i%2 == 0 {
				s.setRedisFault(id, true)
			}
		}
		time.Sleep(3 * time.Second)
		for _, id := range nodes {
			s.setRedisFault(id, false)
		}
		s.logSummary(name)
	case "weight-skew":
		nodes := s.nodeIDs()
		for i, id := range nodes {
			s.adjustWeight(id, float64(i+1))
		}
		s.logSummary(name)
	case "cross-cluster":
		cfg := s.template
		cfg.ClusterID = "other"
		cfg.NodeID = uuid.NewString()
		client := redis.NewClient(&redis.Options{Addr: s.redisAddr})
		hook := &chaosHook{}
		client.AddHook(hook)
		health := &healthGate{healthy: true}
		ctrl, _ := rendez.NewController(cfg, client, &playConsumerFactory{}, rendez.NopLogger(), rendez.NopMetrics(), rendez.WithWeightProvider(s.weights), rendez.WithHealthChecker(health))
		s.weights.Set(cfg.ClusterID+":"+cfg.NodeID, 1)
		nodeCtx, cancel := context.WithCancel(ctx)
		go ctrl.Start(nodeCtx)
		s.mu.Lock()
		s.nodes[cfg.ClusterID+":"+cfg.NodeID] = &simNode{id: cfg.ClusterID + ":" + cfg.NodeID, weight: 1, cancel: cancel, ctrl: ctrl, client: client, hook: hook, health: health, healthy: true}
		s.mu.Unlock()
		s.logSummary(name)
	case "lease-flapping":
		nodes := s.nodeIDs()
		if len(nodes) == 0 {
			return
		}
		id := nodes[0]
		for i := 0; i < 3; i++ {
			s.setRedisFault(id, true)
			time.Sleep(500 * time.Millisecond)
			s.setRedisFault(id, false)
			time.Sleep(700 * time.Millisecond)
		}
		s.logSummary(name)
	default:
		fmt.Println("unknown scenario")
	}
}

func (s *simulation) runScenarioFile(ctx context.Context, path string) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		fmt.Printf("read scenario: %v\n", err)
		return
	}
	var file scenarioFile
	if strings.HasSuffix(path, ".json") {
		if err := json.Unmarshal(data, &file.Events); err != nil {
			fmt.Printf("parse scenario: %v\n", err)
			return
		}
	} else {
		if err := yaml.Unmarshal(data, &file.Events); err != nil {
			fmt.Printf("parse scenario: %v\n", err)
			return
		}
	}
	start := time.Now()
	for _, evt := range file.Events {
		delay, err := time.ParseDuration(evt.At)
		if err != nil {
			continue
		}
		wait := delay - time.Since(start)
		if wait < 0 {
			wait = 0
		}
		go func(ev scenarioEvent) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
				s.handleCommand(ctx, ev.Command)
			}
		}(evt)
	}
}

func (s *simulation) nodeIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.nodes))
	for id := range s.nodes {
		ids = append(ids, id)
	}
	return ids
}

func (s *simulation) logSummary(label string) {
	owners := s.snapshotOwners()
	weights := s.weights.Snapshot()
	nodesList := make([]rendez.NodeWeight, 0, len(weights))
	for id, w := range weights {
		nodesList = append(nodesList, rendez.NodeWeight{ID: id, Weight: w})
	}
	total := 0
	matches := 0
	dist := map[string]int{}
	for _, t := range s.topics {
		for i := 0; i < t.Partitions; i++ {
			slot := rendez.Slot{Topic: t.Name, Index: i}
			key := fmt.Sprintf("lease:%s:%d", t.Name, i)
			owner := owners[key]
			if owner != "" {
				dist[owner]++
			}
			if expected, ok := rendez.RendezvousOwner(slot, nodesList); ok && expected == owner {
				matches++
			}
			total++
		}
	}
	convergence := 0.0
	if total > 0 {
		convergence = (float64(matches) / float64(total)) * 100
	}
	fmt.Printf("Scenario %s summary: convergence=%.1f%% churn/min=%d ownership=%v\n", label, convergence, len(s.churn), dist)
}

type playConsumerFactory struct{}

func (playConsumerFactory) NewConsumer(slot rendez.Slot) (rendez.Consumer, error) {
	return &playConsumer{slot: slot.Key()}, nil
}

type playConsumer struct {
	slot string
}

func (p *playConsumer) Run(ctx context.Context) error {
	t := time.NewTicker(750 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (p *playConsumer) Stop(ctx context.Context) error {
	select {
	case <-ctx.Done():
	case <-time.After(50 * time.Millisecond):
	}
	return nil
}

type weightTable struct {
	mu      sync.RWMutex
	weights map[string]float64
}

func newWeightTable() *weightTable {
	return &weightTable{weights: map[string]float64{}}
}

func (w *weightTable) Weight(id string) (float64, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	val, ok := w.weights[id]
	return val, ok
}

func (w *weightTable) Set(id string, weight float64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.weights[id] = weight
}

func (w *weightTable) Snapshot() map[string]float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	cp := make(map[string]float64, len(w.weights))
	for k, v := range w.weights {
		cp[k] = v
	}
	return cp
}

func (w *weightTable) Delete(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.weights, id)
}

type healthGate struct {
	mu      sync.RWMutex
	healthy bool
}

func (h *healthGate) Healthy(ctx context.Context) bool {
	_ = ctx
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.healthy
}

func (h *healthGate) Set(v bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.healthy = v
}

type chaosHook struct {
	fail atomic.Bool
}

func (h *chaosHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *chaosHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.fail.Load() {
			return fmt.Errorf("chaos redis failure")
		}
		return next(ctx, cmd)
	}
}

func (h *chaosHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.fail.Load() {
			return fmt.Errorf("chaos redis pipeline failure")
		}
		return next(ctx, cmds)
	}
}

type scenarioFile struct {
	Events []scenarioEvent `json:"events" yaml:"events"`
}

type scenarioEvent struct {
	At      string `json:"at" yaml:"at"`
	Command string `json:"command" yaml:"command"`
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

func shortOwner(owner string) string {
	if owner == "" {
		return "-"
	}
	parts := strings.Split(owner, ":")
	return parts[len(parts)-1]
}

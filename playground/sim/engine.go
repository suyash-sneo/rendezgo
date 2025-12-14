package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"

	"github.com/suyash-sneo/rendezgo/pkg/rendezgo"
)

type Mode string

const (
	ModeSimulated Mode = "simulated"
	ModeReal      Mode = "real"
)

type Options struct {
	Mode         Mode
	RedisAddr    string
	Workloads    []rendezgo.WorkloadConfig
	InitialNodes int
	TopK         int
	Scenario     string
	ScenarioFile string
}

type Engine struct {
	mu           sync.Mutex
	mode         Mode
	redisAddr    string
	server       *miniredis.Miniredis
	admin        *redis.Client
	template     rendezgo.Config
	weights      *weightTable
	nodes        map[string]*simNode
	events       *RingBuffer
	owners       map[string]string
	churn        []time.Time
	focus        string
	topK         int
	ctx          context.Context
	cancel       context.CancelFunc
	scenario     string
	scenarioFile string
	initNodes    int
}

type simNode struct {
	id      string
	weight  float64
	cancel  context.CancelFunc
	ctrl    *rendezgo.Controller
	client  *redis.Client
	hook    *chaosHook
	health  *healthGate
	healthy bool
}

type CommandResult struct {
	Message string
}

func NewEngine(opts Options) (*Engine, error) {
	if opts.Mode == "" {
		opts.Mode = ModeSimulated
	}
	if opts.RedisAddr == "" {
		opts.RedisAddr = "127.0.0.1:6379"
	}
	if opts.TopK <= 0 {
		opts.TopK = 3
	}
	cfg := rendezgo.DefaultConfig()
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
	cfg.AcquireLimit = 200
	cfg.StaticWorkloads = opts.Workloads

	eng := &Engine{
		mode:         opts.Mode,
		redisAddr:    opts.RedisAddr,
		template:     cfg,
		weights:      newWeightTable(),
		nodes:        map[string]*simNode{},
		events:       NewRingBuffer(2000),
		owners:       map[string]string{},
		focus:        "",
		topK:         opts.TopK,
		scenario:     opts.Scenario,
		scenarioFile: opts.ScenarioFile,
		initNodes:    opts.InitialNodes,
	}
	return eng, nil
}

func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.ctx != nil {
		e.mu.Unlock()
		return nil
	}
	if e.mode == ModeSimulated {
		server, err := miniredis.Run()
		if err != nil {
			e.mu.Unlock()
			return err
		}
		e.server = server
		e.redisAddr = server.Addr()
	}
	e.admin = redis.NewClient(&redis.Options{Addr: e.redisAddr})
	e.ctx, e.cancel = context.WithCancel(ctx)
	initNodes := e.initNodes
	if initNodes <= 0 {
		initNodes = 3
	}
	e.mu.Unlock()

	for i := 0; i < initNodes; i++ {
		e.addNode(1.0, "")
	}

	if e.scenario != "" {
		go e.runScenario(e.ctx, e.scenario)
	}
	if e.scenarioFile != "" {
		go e.runScenarioFile(e.ctx, e.scenarioFile)
	}
	return nil
}

func (e *Engine) Stop() {
	e.mu.Lock()
	cancel := e.cancel
	e.cancel = nil
	nodes := e.nodes
	e.nodes = map[string]*simNode{}
	server := e.server
	admin := e.admin
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, n := range nodes {
		n.ctrl.SetReleaseOnShutdown(true)
		n.cancel()
		_ = n.client.Close()
	}
	if server != nil {
		server.Close()
	}
	if admin != nil {
		_ = admin.Close()
	}
}

func (e *Engine) emit(t EventType, msg string, fields map[string]interface{}) {
	e.events.Append(Event{At: time.Now(), Type: t, Message: msg, Fields: fields})
}

func (e *Engine) ExecCommand(ctx context.Context, line string) (CommandResult, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return CommandResult{}, nil
	}
	e.emit(EventCommand, line, map[string]interface{}{"command": line})
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return CommandResult{}, nil
	}
	cmd := strings.ToLower(fields[0])
	args := fields[1:]
	switch cmd {
	case "help":
		msg := "commands: add [n] [weight], remove <id>, restart <id>, kill <id>, weight <id> <w>, fail <id> on|off, health <id> on|off, shedding on|off, release <n>, scenario <name>, load <file>, focus <workload|none>, predict down <node>, explain <workload> <unit>"
		e.emit(EventInfo, msg, nil)
		return CommandResult{Message: msg}, nil
	case "add":
		count := 1
		weight := 1.0
		if len(args) > 0 {
			if v, err := strconv.Atoi(args[0]); err == nil {
				count = v
			}
		}
		if len(args) > 1 {
			if v, err := strconv.ParseFloat(args[1], 64); err == nil {
				weight = v
			}
		}
		for i := 0; i < count; i++ {
			e.addNode(weight, "")
		}
		return CommandResult{Message: "added"}, nil
	case "remove":
		if len(args) < 1 {
			err := fmt.Errorf("usage: remove <id>")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		e.removeNode(args[0], true)
		return CommandResult{Message: "removed"}, nil
	case "restart":
		if len(args) < 1 {
			err := fmt.Errorf("usage: restart <id>")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		e.restartNode(args[0])
		return CommandResult{Message: "restarted"}, nil
	case "kill":
		if len(args) < 1 {
			err := fmt.Errorf("usage: kill <id>")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		e.removeNode(args[0], false)
		return CommandResult{Message: "killed"}, nil
	case "weight":
		if len(args) < 2 {
			err := fmt.Errorf("usage: weight <id> <weight>")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		w, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			err = fmt.Errorf("invalid weight")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		e.adjustWeight(args[0], w)
		return CommandResult{Message: "weight set"}, nil
	case "fail":
		if len(args) < 2 {
			err := fmt.Errorf("usage: fail <id> on|off")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		e.setRedisFault(args[0], args[1] == "on")
		return CommandResult{Message: "fail set"}, nil
	case "health":
		if len(args) < 2 {
			err := fmt.Errorf("usage: health <id> on|off")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		e.setHealth(args[0], args[1] == "on")
		return CommandResult{Message: "health set"}, nil
	case "shedding":
		if len(args) < 1 {
			err := fmt.Errorf("usage: shedding on|off")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		e.setShedding(args[0] == "on")
		return CommandResult{Message: "shedding"}, nil
	case "release":
		if len(args) < 1 {
			err := fmt.Errorf("usage: release <per-interval>")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		if v, err := strconv.Atoi(args[0]); err == nil {
			e.setRelease(v)
		}
		return CommandResult{Message: "release"}, nil
	case "scenario":
		if len(args) < 1 {
			err := fmt.Errorf("usage: scenario <name>")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		go e.runScenario(ctx, args[0])
		return CommandResult{Message: "scenario"}, nil
	case "load":
		if len(args) < 1 {
			err := fmt.Errorf("usage: load <file>")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		go e.runScenarioFile(ctx, args[0])
		return CommandResult{Message: "load"}, nil
	case "focus":
		if len(args) < 1 {
			err := fmt.Errorf("usage: focus <workload|none>")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		e.setFocus(args[0])
		return CommandResult{Message: "focus"}, nil
	case "predict":
		if len(args) == 2 && args[0] == "down" {
			e.predictDown(args[1])
			return CommandResult{Message: "predict"}, nil
		}
		err := fmt.Errorf("usage: predict down <nodeID>")
		e.emit(EventError, err.Error(), nil)
		return CommandResult{}, err
	case "explain":
		if len(args) < 2 {
			err := fmt.Errorf("usage: explain <workload> <unit>")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		unit, err := strconv.Atoi(args[1])
		if err != nil {
			err = fmt.Errorf("unit must be integer")
			e.emit(EventError, err.Error(), nil)
			return CommandResult{}, err
		}
		e.explain(args[0], unit)
		return CommandResult{Message: "explain"}, nil
	default:
		err := fmt.Errorf("unknown command")
		e.emit(EventError, err.Error(), nil)
		return CommandResult{}, err
	}
}

func (e *Engine) addNode(weight float64, customID string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := customID
	if id == "" {
		id = fmt.Sprintf("n-%d", len(e.nodes)+1)
	}
	cfg := e.template
	cfg.NodeID = id
	cfg.StaticWeights = nil
	client := redis.NewClient(&redis.Options{Addr: e.redisAddr})
	hook := &chaosHook{}
	client.AddHook(hook)
	health := &healthGate{healthy: true}
	ctrl, err := rendezgo.NewController(cfg, client, &playConsumerFactory{}, rendezgo.NopLogger(), rendezgo.NopMetrics(), rendezgo.WithWeightProvider(e.weights), rendezgo.WithHealthChecker(health))
	if err != nil {
		e.emit(EventError, fmt.Sprintf("add node %s: %v", id, err), nil)
		return ""
	}
	nodeID := cfg.ClusterID + ":" + cfg.NodeID
	e.weights.Set(nodeID, weight)
	nodeCtx, cancel := context.WithCancel(context.Background())
	go ctrl.Start(nodeCtx)
	n := &simNode{id: nodeID, weight: weight, cancel: cancel, ctrl: ctrl, client: client, hook: hook, health: health, healthy: true}
	e.nodes[nodeID] = n
	e.emit(EventInfo, fmt.Sprintf("added node %s (weight %.2f)", nodeID, weight), map[string]interface{}{"node": nodeID, "weight": weight})
	return nodeID
}

func (e *Engine) removeNode(id string, graceful bool) {
	e.mu.Lock()
	node, ok := e.nodes[id]
	if ok {
		delete(e.nodes, id)
		e.weights.Delete(id)
	}
	e.mu.Unlock()
	if !ok {
		e.emit(EventError, fmt.Sprintf("node %s not found", id), nil)
		return
	}
	node.ctrl.SetReleaseOnShutdown(graceful)
	node.cancel()
	_ = node.client.Close()
	e.emit(EventInfo, fmt.Sprintf("removed node %s", id), map[string]interface{}{"node": id, "graceful": graceful})
}

func (e *Engine) restartNode(id string) {
	e.mu.Lock()
	node, ok := e.nodes[id]
	weight := 1.0
	if ok {
		weight = node.weight
	}
	e.mu.Unlock()
	if !ok {
		e.emit(EventError, fmt.Sprintf("node %s not found", id), nil)
		return
	}
	e.removeNode(id, true)
	base := id
	if strings.Contains(id, ":") {
		parts := strings.Split(id, ":")
		base = parts[len(parts)-1]
	}
	newID := e.addNode(weight, base)
	e.emit(EventInfo, fmt.Sprintf("restarted %s -> %s", id, newID), map[string]interface{}{"from": id, "to": newID})
}

func (e *Engine) adjustWeight(id string, weight float64) {
	e.mu.Lock()
	node, ok := e.nodes[id]
	if ok {
		node.weight = weight
	}
	e.mu.Unlock()
	if !ok {
		e.emit(EventError, fmt.Sprintf("node %s not found", id), nil)
		return
	}
	e.weights.Set(id, weight)
	e.emit(EventInfo, fmt.Sprintf("weight %s=%.2f", id, weight), map[string]interface{}{"node": id, "weight": weight})
}

func (e *Engine) setRedisFault(id string, fail bool) {
	e.mu.Lock()
	node, ok := e.nodes[id]
	e.mu.Unlock()
	if !ok {
		e.emit(EventError, fmt.Sprintf("node %s not found", id), nil)
		return
	}
	node.hook.fail.Store(fail)
	e.emit(EventInfo, fmt.Sprintf("fail %s=%v", id, fail), map[string]interface{}{"node": id, "fail": fail})
}

func (e *Engine) setHealth(id string, healthy bool) {
	e.mu.Lock()
	node, ok := e.nodes[id]
	e.mu.Unlock()
	if !ok {
		e.emit(EventError, fmt.Sprintf("node %s not found", id), nil)
		return
	}
	node.health.Set(healthy)
	node.healthy = healthy
	e.emit(EventInfo, fmt.Sprintf("health %s=%v", id, healthy), map[string]interface{}{"node": id, "healthy": healthy})
}

func (e *Engine) setShedding(on bool) {
	e.mu.Lock()
	e.template.SheddingEnabled = on
	e.mu.Unlock()
	e.restartAll()
	e.emit(EventInfo, fmt.Sprintf("shedding %v", on), map[string]interface{}{"enabled": on})
}

func (e *Engine) setRelease(n int) {
	e.mu.Lock()
	e.template.SheddingRelease = n
	e.mu.Unlock()
	e.restartAll()
	e.emit(EventInfo, fmt.Sprintf("shedding release=%d", n), map[string]interface{}{"release": n})
}

func (e *Engine) setFocus(focus string) {
	if focus == "none" {
		focus = ""
	}
	e.mu.Lock()
	e.focus = focus
	e.mu.Unlock()
	e.emit(EventInfo, fmt.Sprintf("focus set to %s", focus), map[string]interface{}{"focus": focus})
}

func (e *Engine) restartAll() {
	ids := e.nodeIDs()
	for _, id := range ids {
		e.restartNode(id)
	}
}

func (e *Engine) nodeIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	ids := make([]string, 0, len(e.nodes))
	for id := range e.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (e *Engine) Snapshot() Snapshot {
	e.mu.Lock()
	workloads := append([]rendezgo.WorkloadConfig{}, e.template.StaticWorkloads...)
	focus := e.focus
	caps := CapsSummary{
		MaxPerNode:      e.template.MaxConsumersPerNode,
		MaxPerWorkload:  e.template.MaxConsumersPerWorkloadPerNode,
		MinPerNode:      e.template.MinConsumersPerNode,
		SheddingEnabled: e.template.SheddingEnabled,
		SheddingRelease: e.template.SheddingRelease,
	}
	mode := e.mode
	redisAddr := e.redisAddr
	topK := e.topK
	e.mu.Unlock()

	owners, leaseTTLs, cooldownTTLs := e.redisState(workloads)
	desired := rendezgo.DesiredOwners(workloads, e.liveWeights(""))
	e.updateChurn(owners)

	nodes := e.buildNodeSnapshots(workloads, desired, owners)
	units := e.buildUnitSnapshots(workloads, owners, desired, leaseTTLs, cooldownTTLs, topK, focus)

	convergence := 0.0
	if len(units) > 0 {
		aligned := 0
		for _, u := range units {
			if u.Aligned {
				aligned++
			}
		}
		convergence = (float64(aligned) / float64(len(units))) * 100
	}

	wlSummaries := make([]WorkloadSummary, 0, len(workloads))
	for _, wl := range workloads {
		wlSummaries = append(wlSummaries, WorkloadSummary{Name: wl.Name, Units: wl.Units})
	}

	return Snapshot{
		Now:            time.Now(),
		RedisAddr:      redisAddr,
		Mode:           string(mode),
		ChurnPerMinute: len(e.churn),
		Convergence:    convergence,
		Focus:          focus,
		TopK:           topK,
		Caps:           caps,
		Workloads:      wlSummaries,
		Nodes:          nodes,
		Units:          units,
	}
}

func (e *Engine) EventsSince(seq uint64) ([]Event, uint64) {
	return e.events.Since(seq)
}

func (e *Engine) redisState(workloads []rendezgo.WorkloadConfig) (map[string]string, map[string]time.Duration, map[string]time.Duration) {
	owners := map[string]string{}
	leaseTTLs := map[string]time.Duration{}
	cooldownTTLs := map[string]time.Duration{}
	ctx := context.Background()
	pipe := e.admin.Pipeline()
	leaseCmds := map[string]*redis.StringCmd{}
	ttlCmds := map[string]*redis.DurationCmd{}
	coolCmds := map[string]*redis.DurationCmd{}
	for _, wl := range workloads {
		for unit := 0; unit < wl.Units; unit++ {
			slot := rendezgo.Slot{Workload: wl.Name, Unit: unit}
			key := e.leaseKey(slot)
			leaseCmds[key] = pipe.Get(ctx, key)
			ttlCmds[key] = pipe.PTTL(ctx, key)
			coolCmds[key] = pipe.PTTL(ctx, e.cooldownKey(slot))
		}
	}
	if len(leaseCmds) > 0 {
		_, _ = pipe.Exec(ctx)
	}
	for key, cmd := range leaseCmds {
		if val, err := cmd.Result(); err == nil {
			owners[key] = val
		}
	}
	for key, cmd := range ttlCmds {
		if ttl, err := cmd.Result(); err == nil && ttl > 0 {
			leaseTTLs[key] = ttl
		}
	}
	for key, cmd := range coolCmds {
		if ttl, err := cmd.Result(); err == nil && ttl > 0 {
			cooldownTTLs[key] = ttl
		}
	}
	return owners, leaseTTLs, cooldownTTLs
}

func (e *Engine) buildNodeSnapshots(workloads []rendezgo.WorkloadConfig, desired map[string]string, owners map[string]string) []NodeSnapshot {
	e.mu.Lock()
	nodes := make(map[string]*simNode, len(e.nodes))
	for id, n := range e.nodes {
		nodes[id] = n
	}
	e.mu.Unlock()
	weightSnap := e.weights.Snapshot()
	nodeSnaps := map[string]*NodeSnapshot{}
	for id := range nodes {
		nodeSnaps[id] = &NodeSnapshot{ID: id, Weight: weightSnap[id], Workload: map[string]WorkloadCount{}}
	}
	for key, owner := range owners {
		if owner == "" {
			continue
		}
		slot, err := e.slotFromLeaseKey(key)
		if err != nil {
			continue
		}
		ns, ok := nodeSnaps[owner]
		if !ok {
			ns = &NodeSnapshot{ID: owner, Weight: weightSnap[owner], Workload: map[string]WorkloadCount{}}
			nodeSnaps[owner] = ns
		}
		ns.Owned++
		wc := ns.Workload[slot.Workload]
		wc.Owned++
		ns.Workload[slot.Workload] = wc
	}
	for key, want := range desired {
		slot, err := e.slotFromLeaseKey(key)
		if err != nil || want == "" {
			continue
		}
		ns, ok := nodeSnaps[want]
		if !ok {
			ns = &NodeSnapshot{ID: want, Weight: weightSnap[want], Workload: map[string]WorkloadCount{}}
			nodeSnaps[want] = ns
		}
		ns.Desired++
		wc := ns.Workload[slot.Workload]
		wc.Desired++
		ns.Workload[slot.Workload] = wc
	}
	out := make([]NodeSnapshot, 0, len(nodeSnaps))
	for _, ns := range nodeSnaps {
		if ns.Desired > ns.Owned {
			ns.Missing = ns.Desired - ns.Owned
		}
		if ns.Owned > ns.Desired {
			ns.Extra = ns.Owned - ns.Desired
		}
		ns.State = e.nodeState(nodes[ns.ID])
		out = append(out, *ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (e *Engine) buildUnitSnapshots(workloads []rendezgo.WorkloadConfig, owners, desired map[string]string, leaseTTLs, cooldownTTLs map[string]time.Duration, topK int, focus string) []UnitSnapshot {
	weights := e.liveWeights("")
	units := make([]UnitSnapshot, 0)
	for _, wl := range workloads {
		if focus != "" && focus != wl.Name {
			continue
		}
		for unit := 0; unit < wl.Units; unit++ {
			slot := rendezgo.Slot{Workload: wl.Name, Unit: unit}
			key := e.leaseKey(slot)
			owner := owners[key]
			want := desired[key]
			ranking := rendezgo.RendezvousRanking(slot, weights)
			top := ranking
			if topK > 0 && len(ranking) > topK {
				top = ranking[:topK]
			}
			cs := make([]CandidateScore, 0, len(top))
			for _, r := range top {
				cs = append(cs, CandidateScore{ID: r.ID, Score: r.Score, Weight: r.Weight})
			}
			units = append(units, UnitSnapshot{
				Workload:     wl.Name,
				Unit:         unit,
				CurrentOwner: owner,
				DesiredOwner: want,
				LeaseTTL:     leaseTTLs[key],
				CooldownTTL:  cooldownTTLs[e.cooldownKey(slot)],
				Ranking:      cs,
				Aligned:      owner != "" && owner == want,
				Cooldown:     cooldownTTLs[e.cooldownKey(slot)] > 0,
				Unowned:      owner == "",
			})
		}
	}
	sort.Slice(units, func(i, j int) bool {
		if units[i].Workload == units[j].Workload {
			return units[i].Unit < units[j].Unit
		}
		return units[i].Workload < units[j].Workload
	})
	return units
}

func (e *Engine) updateChurn(current map[string]string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	for k, v := range current {
		if prev, ok := e.owners[k]; !ok || prev != v {
			e.churn = append(e.churn, now)
		}
	}
	cutoff := now.Add(-time.Minute)
	idx := 0
	for idx < len(e.churn) && !e.churn[idx].After(cutoff) {
		idx++
	}
	if idx > 0 {
		e.churn = e.churn[idx:]
	}
	e.owners = current
}

func (e *Engine) predictDown(target string) {
	full := e.liveWeights("")
	remaining := e.liveWeights(target)
	currentDesired := rendezgo.DesiredOwners(e.template.StaticWorkloads, full)
	predicted := rendezgo.DesiredOwners(e.template.StaticWorkloads, remaining)
	moved := []string{}
	for key, newOwner := range predicted {
		if currentDesired[key] != newOwner {
			moved = append(moved, key)
		}
	}
	sort.Strings(moved)
	for _, key := range moved {
		slot, err := e.slotFromLeaseKey(key)
		if err != nil {
			continue
		}
		ranking := rendezgo.RendezvousRanking(slot, remaining)
		top := ranking
		if e.topK > 0 && len(ranking) > e.topK {
			top = ranking[:e.topK]
		}
		e.emit(EventPrediction, fmt.Sprintf("predict %s[%d]: %s -> %s", slot.Workload, slot.Unit, shortOwner(currentDesired[key]), shortOwner(predicted[key])), map[string]interface{}{"slot": slot.Key(), "candidates": top})
	}
}

func (e *Engine) explain(workload string, unit int) {
	weights := e.liveWeights("")
	slot := rendezgo.Slot{Workload: workload, Unit: unit}
	ranking := rendezgo.RendezvousRanking(slot, weights)
	e.emit(EventInfo, fmt.Sprintf("explain %s[%d]", workload, unit), map[string]interface{}{"slot": slot.Key(), "ranking": ranking})
}

func (e *Engine) liveWeights(skip string) []rendezgo.NodeWeight {
	e.mu.Lock()
	ids := make([]string, 0, len(e.nodes))
	for id := range e.nodes {
		if id == skip {
			continue
		}
		ids = append(ids, id)
	}
	e.mu.Unlock()
	sort.Strings(ids)
	snap := e.weights.Snapshot()
	out := make([]rendezgo.NodeWeight, 0, len(ids))
	for _, id := range ids {
		w := snap[id]
		if w == 0 {
			w = 1
		}
		out = append(out, rendezgo.NodeWeight{ID: id, Weight: w})
	}
	return out
}

func (e *Engine) runScenario(ctx context.Context, name string) {
	e.emit(EventScenario, fmt.Sprintf("scenario %s start", name), map[string]interface{}{"scenario": name})
	switch name {
	case "scale-out":
		e.addNode(1, "")
		time.Sleep(time.Second)
		for i := 0; i < 5; i++ {
			e.addNode(1, "")
		}
		time.Sleep(2 * time.Second)
		for i := 0; i < 10; i++ {
			e.addNode(1, "")
		}
	case "pod-death":
		nodes := e.nodeIDs()
		target := len(nodes) / 2
		rand.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
		for i := 0; i < target; i++ {
			e.removeNode(nodes[i], false)
		}
	case "redis-instability":
		nodes := e.nodeIDs()
		for i, id := range nodes {
			if i%2 == 0 {
				e.setRedisFault(id, true)
			}
		}
		time.Sleep(3 * time.Second)
		for _, id := range nodes {
			e.setRedisFault(id, false)
		}
	case "weight-skew":
		nodes := e.nodeIDs()
		for i, id := range nodes {
			e.adjustWeight(id, float64(i+1))
		}
	case "cross-cluster":
		e.spawnOtherCluster()
	case "lease-flapping":
		nodes := e.nodeIDs()
		if len(nodes) == 0 {
			break
		}
		id := nodes[0]
		for i := 0; i < 3; i++ {
			e.setRedisFault(id, true)
			time.Sleep(500 * time.Millisecond)
			e.setRedisFault(id, false)
			time.Sleep(700 * time.Millisecond)
		}
	default:
		e.emit(EventError, "unknown scenario", map[string]interface{}{"scenario": name})
	}
	e.emit(EventScenario, fmt.Sprintf("scenario %s done", name), map[string]interface{}{"scenario": name})
}

func (e *Engine) runScenarioFile(ctx context.Context, path string) {
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		e.emit(EventError, fmt.Sprintf("read scenario: %v", err), map[string]interface{}{"file": path})
		return
	}
	var file scenarioFile
	if strings.HasSuffix(path, ".json") {
		if err := json.Unmarshal(data, &file.Events); err != nil {
			e.emit(EventError, fmt.Sprintf("parse scenario: %v", err), map[string]interface{}{"file": path})
			return
		}
	} else {
		if err := yaml.Unmarshal(data, &file.Events); err != nil {
			e.emit(EventError, fmt.Sprintf("parse scenario: %v", err), map[string]interface{}{"file": path})
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
				e.ExecCommand(context.Background(), ev.Command)
			}
		}(evt)
	}
	e.emit(EventScenario, fmt.Sprintf("scenario file %s scheduled", path), map[string]interface{}{"file": path})
}

func (e *Engine) spawnOtherCluster() {
	cfg := e.template
	cfg.ClusterID = "other"
	cfg.NodeID = uuid.NewString()
	client := redis.NewClient(&redis.Options{Addr: e.redisAddr})
	hook := &chaosHook{}
	client.AddHook(hook)
	health := &healthGate{healthy: true}
	ctrl, err := rendezgo.NewController(cfg, client, &playConsumerFactory{}, rendezgo.NopLogger(), rendezgo.NopMetrics(), rendezgo.WithWeightProvider(e.weights), rendezgo.WithHealthChecker(health))
	if err != nil {
		e.emit(EventError, fmt.Sprintf("spawn other cluster: %v", err), nil)
		return
	}
	nodeID := cfg.ClusterID + ":" + cfg.NodeID
	e.weights.Set(nodeID, 1)
	nodeCtx, cancel := context.WithCancel(context.Background())
	go ctrl.Start(nodeCtx)
	e.mu.Lock()
	e.nodes[nodeID] = &simNode{id: nodeID, weight: 1, cancel: cancel, ctrl: ctrl, client: client, hook: hook, health: health, healthy: true}
	e.mu.Unlock()
	e.emit(EventInfo, fmt.Sprintf("added other cluster node %s", nodeID), map[string]interface{}{"node": nodeID})
}

// helpers
func (e *Engine) leaseKey(slot rendezgo.Slot) string {
	return fmt.Sprintf("lease:%s:%d", slot.Workload, slot.Unit)
}
func (e *Engine) cooldownKey(slot rendezgo.Slot) string {
	return fmt.Sprintf("moved:%s:%d", slot.Workload, slot.Unit)
}

func (e *Engine) slotFromLeaseKey(key string) (rendezgo.Slot, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 3 {
		return rendezgo.Slot{}, fmt.Errorf("invalid lease key")
	}
	unit, err := strconv.Atoi(parts[2])
	if err != nil {
		return rendezgo.Slot{}, err
	}
	return rendezgo.Slot{Workload: parts[1], Unit: unit}, nil
}

func (e *Engine) nodeState(n *simNode) string {
	if n == nil {
		return "unknown"
	}
	if !n.healthy {
		return "unhealthy"
	}
	if n.ctrl != nil && n.ctrl.BackoffActive() {
		return "backoff"
	}
	return "ok"
}

func shortOwner(owner string) string {
	if owner == "" {
		return "-"
	}
	parts := strings.Split(owner, ":")
	return parts[len(parts)-1]
}

// play consumer used for simulation

type playConsumerFactory struct{}

func (playConsumerFactory) NewConsumer(slot rendezgo.Slot) (rendezgo.Consumer, error) {
	return &playConsumer{slot: slot.Key()}, nil
}

type playConsumer struct{ slot string }

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

// weight table and gates

type weightTable struct {
	mu      sync.RWMutex
	weights map[string]float64
}

func newWeightTable() *weightTable { return &weightTable{weights: map[string]float64{}} }

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

func (h *chaosHook) DialHook(next redis.DialHook) redis.DialHook { return next }

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

// scenario file structures

type scenarioFile struct {
	Events []scenarioEvent `json:"events" yaml:"events"`
}

type scenarioEvent struct {
	At      string `json:"at" yaml:"at"`
	Command string `json:"command" yaml:"command"`
}

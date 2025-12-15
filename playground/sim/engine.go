package sim

import (
	"context"
	"encoding/json"
	"errors"
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

// EngineOptions configures a simulation engine.
type EngineOptions struct {
	Mode             string
	RedisAddr        string
	InitialNodes     int
	Workloads        []rendezgo.WorkloadConfig
	TopK             int
	SnapshotInterval time.Duration
	ScenarioName     string
	ScenarioFilePath string
}

// Engine drives a cluster of rendezgo controllers and exposes snapshots/events.
type Engine struct {
	mu             sync.Mutex
	opts           EngineOptions
	template       rendezgo.Config
	weights        *weightTable
	nodes          map[string]*simNode
	events         *RingBuffer
	owners         map[string]string
	churn          []time.Time
	focus          string
	server         *miniredis.Miniredis
	timeDriverStop chan struct{}
	admin          *redis.Client
	ctx            context.Context
	cancel         context.CancelFunc
	lastSnapshot   Snapshot
	lastSnapshotAt time.Time
	redisAddr      string
}

type simNode struct {
	id        string
	weight    float64
	cancel    context.CancelFunc
	done      chan struct{}
	ctrl      *rendezgo.Controller
	client    *redis.Client
	hook      *chaosHook
	health    *healthGate
	healthy   bool
	redisFail bool
}

// NewEngine builds an engine with sensible defaults for simulation.
func NewEngine(opts EngineOptions) (*Engine, error) {
	if opts.Mode == "" {
		opts.Mode = "simulated"
	}
	if opts.RedisAddr == "" {
		opts.RedisAddr = "127.0.0.1:6379"
	}
	if opts.TopK <= 0 {
		opts.TopK = 3
	}
	if opts.SnapshotInterval <= 0 {
		opts.SnapshotInterval = time.Second
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

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &Engine{
		opts:         opts,
		template:     cfg,
		weights:      newWeightTable(),
		nodes:        map[string]*simNode{},
		events:       NewRingBuffer(2000),
		owners:       map[string]string{},
		redisAddr:    opts.RedisAddr,
		lastSnapshot: Snapshot{},
	}, nil
}

// Start prepares Redis and spins up initial nodes.
func (e *Engine) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.ctx != nil {
		e.mu.Unlock()
		return nil
	}
	if e.opts.Mode == "simulated" {
		server, err := miniredis.Run()
		if err != nil {
			e.mu.Unlock()
			return err
		}
		e.server = server
		e.redisAddr = server.Addr()
		e.timeDriverStop = make(chan struct{})
	}
	e.admin = redis.NewClient(&redis.Options{Addr: e.redisAddr})
	e.ctx, e.cancel = context.WithCancel(ctx)
	initialNodes := e.opts.InitialNodes
	if initialNodes <= 0 {
		initialNodes = 3
	}
	scenario := e.opts.ScenarioName
	scriptPath := e.opts.ScenarioFilePath
	e.mu.Unlock()

	e.recordEvent(EventEngineStart, fmt.Sprintf("engine start mode=%s redis=%s", e.opts.Mode, e.redisAddr), map[string]interface{}{
		"mode":  e.opts.Mode,
		"redis": e.redisAddr,
	})

	if e.opts.Mode == "simulated" && e.server != nil {
		go e.driveSimulatedTime(e.ctx, e.server, e.timeDriverStop)
	}

	for i := 0; i < initialNodes; i++ {
		if _, err := e.addNode(1.0, ""); err != nil {
			return err
		}
	}

	if scenario != "" {
		go e.RunScenario(scenario)
	}
	if scriptPath != "" {
		data, err := os.ReadFile(filepath.Clean(scriptPath))
		if err != nil {
			e.recordError(fmt.Sprintf("scenario file %s: %v", scriptPath, err), map[string]interface{}{"file": scriptPath})
		} else {
			go e.RunScenarioFile(scriptPath, data)
		}
	}
	return nil
}

// Close stops all controllers and Redis resources. Idempotent.
func (e *Engine) Close() error {
	e.mu.Lock()
	cancel := e.cancel
	ctx := e.ctx
	nodes := e.nodes
	e.nodes = map[string]*simNode{}
	e.cancel = nil
	e.ctx = nil
	server := e.server
	e.server = nil
	stopTime := e.timeDriverStop
	e.timeDriverStop = nil
	admin := e.admin
	e.admin = nil
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if ctx != nil {
		<-ctx.Done()
	}
	if stopTime != nil {
		close(stopTime)
	}
	for _, n := range nodes {
		n.ctrl.SetReleaseOnShutdown(true)
		n.cancel()
		if n.done != nil {
			select {
			case <-n.done:
			case <-time.After(2 * time.Second):
			}
		}
		_ = n.client.Close()
	}
	if server != nil {
		server.Close()
	}
	if admin != nil {
		_ = admin.Close()
	}
	e.recordEvent(EventEngineStop, "engine stopped", nil)
	return nil
}

// Snapshot returns the most recent snapshot; expensive rebuilds are throttled by SnapshotInterval.
func (e *Engine) Snapshot() Snapshot {
	now := time.Now()
	e.mu.Lock()
	last := e.lastSnapshot
	lastAt := e.lastSnapshotAt
	interval := e.opts.SnapshotInterval
	e.mu.Unlock()

	if !lastAt.IsZero() && interval > 0 && now.Sub(lastAt) < interval {
		return interpolateSnapshot(last, now.Sub(lastAt))
	}

	snap := e.buildSnapshot(now)
	e.mu.Lock()
	e.lastSnapshot = snap
	e.lastSnapshotAt = now
	e.mu.Unlock()
	return snap
}

// EventsSince returns all events after the provided seq and the latest delivered seq.
func (e *Engine) EventsSince(seq uint64) ([]Event, uint64) {
	return e.events.Since(seq)
}

// AddNodes adds N new nodes with the supplied weight.
func (e *Engine) AddNodes(count int, weight float64) error {
	if count <= 0 {
		count = 1
	}
	for i := 0; i < count; i++ {
		if _, err := e.addNode(weight, ""); err != nil {
			return err
		}
	}
	return nil
}

// RemoveNode removes a node, optionally releasing leases.
func (e *Engine) RemoveNode(id string, graceful bool) error {
	return e.removeNode(id, graceful, EventNodeRemove)
}

// RestartNode restarts a node preserving its weight.
func (e *Engine) RestartNode(id string) error {
	e.mu.Lock()
	node, ok := e.nodes[id]
	weight := 1.0
	if ok {
		weight = node.weight
	}
	e.mu.Unlock()
	if !ok {
		return e.recordError(fmt.Sprintf("node %s not found", id), map[string]interface{}{"nodeID": id})
	}
	base := id
	if strings.Contains(id, ":") {
		parts := strings.Split(id, ":")
		base = parts[len(parts)-1]
	}
	if err := e.removeNode(id, true, EventNodeRestart); err != nil {
		return err
	}
	newID, err := e.addNode(weight, base)
	if err != nil {
		return err
	}
	e.recordEvent(EventNodeRestart, fmt.Sprintf("restarted %s -> %s", id, newID), map[string]interface{}{"from": id, "to": newID})
	return nil
}

// KillNode forcefully removes a node without graceful release.
func (e *Engine) KillNode(id string) error {
	return e.removeNode(id, false, EventNodeKill)
}

// SetWeight adjusts a node weight.
func (e *Engine) SetWeight(id string, weight float64) error {
	e.mu.Lock()
	node, ok := e.nodes[id]
	if ok {
		node.weight = weight
	}
	e.mu.Unlock()
	if !ok {
		return e.recordError(fmt.Sprintf("node %s not found", id), map[string]interface{}{"nodeID": id})
	}
	e.weights.Set(id, weight)
	e.recordEvent(EventNodeWeight, fmt.Sprintf("weight %s=%.2f", id, weight), map[string]interface{}{"nodeID": id, "weight": weight})
	return nil
}

// SetRedisFault toggles forced Redis errors for a node.
func (e *Engine) SetRedisFault(id string, fail bool) error {
	e.mu.Lock()
	node, ok := e.nodes[id]
	e.mu.Unlock()
	if !ok {
		return e.recordError(fmt.Sprintf("node %s not found", id), map[string]interface{}{"nodeID": id})
	}
	node.hook.fail.Store(fail)
	node.redisFail = fail
	e.recordEvent(EventNodeRedisFail, fmt.Sprintf("redis fault %s=%v", id, fail), map[string]interface{}{"nodeID": id, "fail": fail})
	return nil
}

// SetHealth toggles node health gate.
func (e *Engine) SetHealth(id string, healthy bool) error {
	e.mu.Lock()
	node, ok := e.nodes[id]
	e.mu.Unlock()
	if !ok {
		return e.recordError(fmt.Sprintf("node %s not found", id), map[string]interface{}{"nodeID": id})
	}
	node.health.Set(healthy)
	node.healthy = healthy
	e.recordEvent(EventNodeHealth, fmt.Sprintf("health %s=%v", id, healthy), map[string]interface{}{"nodeID": id, "healthy": healthy})
	return nil
}

// SetShedding toggles shedding feature and restarts controllers to pick up config.
func (e *Engine) SetShedding(enabled bool) error {
	e.mu.Lock()
	e.template.SheddingEnabled = enabled
	e.mu.Unlock()
	e.restartAll()
	e.recordEvent(EventShedding, fmt.Sprintf("shedding enabled=%v", enabled), map[string]interface{}{"enabled": enabled})
	return nil
}

// SetSheddingRelease adjusts the release rate and restarts controllers.
func (e *Engine) SetSheddingRelease(perInterval int) error {
	e.mu.Lock()
	e.template.SheddingRelease = perInterval
	e.mu.Unlock()
	e.restartAll()
	e.recordEvent(EventSheddingRate, fmt.Sprintf("shedding release=%d", perInterval), map[string]interface{}{"perInterval": perInterval})
	return nil
}

// SetFocus records the workload focus; snapshot includes the hint.
func (e *Engine) SetFocus(workload string) error {
	e.mu.Lock()
	e.focus = workload
	e.mu.Unlock()
	e.recordEvent(EventFocus, fmt.Sprintf("focus set to %s", workload), map[string]interface{}{"workload": workload})
	return nil
}

// PredictDown forecasts slot movements if nodeID disappears.
func (e *Engine) PredictDown(nodeID string) (PredictDownResult, error) {
	if nodeID == "" {
		return PredictDownResult{}, e.recordError("nodeID required", map[string]interface{}{"endpoint": "predict.down"})
	}
	if e.ctx == nil {
		return PredictDownResult{}, e.recordError("engine not started", map[string]interface{}{"endpoint": "predict.down"})
	}
	e.mu.Lock()
	workloads := append([]rendezgo.WorkloadConfig{}, e.template.StaticWorkloads...)
	topK := e.opts.TopK
	e.mu.Unlock()

	full := e.liveWeights("")
	remaining := e.liveWeights(nodeID)
	removed := len(full) != len(remaining)
	nowMs := time.Now().UnixMilli()
	currentDesired := rendezgo.DesiredOwners(workloads, full)
	predicted := rendezgo.DesiredOwners(workloads, remaining)

	moves := []PredictedMove{}
	summary := map[string]int{}
	for _, wl := range workloads {
		for unit := 0; unit < wl.Units; unit++ {
			slot := rendezgo.Slot{Workload: wl.Name, Unit: unit}
			key := e.leaseKey(slot)
			cur := currentDesired[key]
			next := predicted[key]
			if cur == next {
				continue
			}
			reason := "unknown"
			if cur == "" {
				reason = "wasUnowned"
			} else {
				reason = "desiredOwnerChanged"
			}
			var ranking []rendezgo.NodeScore
			if topK > 0 {
				ranking = rendezgo.RendezvousTopK(slot, remaining, topK)
			} else {
				ranking = rendezgo.RendezvousRanking(slot, remaining)
			}
			candidates := make([]HRWCandidate, 0, len(ranking))
			for _, r := range ranking {
				candidates = append(candidates, HRWCandidate{
					NodeID:  r.ID,
					ShortID: shortID(r.ID),
					Weight:  r.Weight,
					Score:   r.Score,
				})
			}
			if next != "" {
				summary[next]++
			}
			moves = append(moves, PredictedMove{
				Workload:   slot.Workload,
				Unit:       slot.Unit,
				FromOwner:  cur,
				ToOwner:    next,
				Reason:     reason,
				Candidates: candidates,
			})
		}
	}

	sort.Slice(moves, func(i, j int) bool {
		if moves[i].Workload == moves[j].Workload {
			return moves[i].Unit < moves[j].Unit
		}
		return moves[i].Workload < moves[j].Workload
	})

	summaryList := make([]PredictDownSummaryOwner, 0, len(summary))
	for node, count := range summary {
		summaryList = append(summaryList, PredictDownSummaryOwner{NodeID: node, Count: count})
	}
	sort.Slice(summaryList, func(i, j int) bool { return summaryList[i].NodeID < summaryList[j].NodeID })

	result := PredictDownResult{
		NodeID:             nodeID,
		RemovedFromLiveSet: removed,
		GeneratedAtUnixMs:  nowMs,
		Moves:              moves,
		Summary: PredictDownSummary{
			UnitsImpacted: len(moves),
			ByToOwner:     summaryList,
		},
	}
	e.recordEvent(EventPredictDown, fmt.Sprintf("predict down %s moves=%d", nodeID, len(moves)), map[string]interface{}{"nodeID": nodeID, "moves": len(moves)})
	return result, nil
}

// ExplainUnit returns an HRW breakdown for a unit.
func (e *Engine) ExplainUnit(workload string, unit int) (ExplainUnitResult, error) {
	if workload == "" || unit < 0 {
		return ExplainUnitResult{}, e.recordError("workload and unit required", map[string]interface{}{"endpoint": "explain.unit"})
	}
	if e.admin == nil {
		return ExplainUnitResult{}, e.recordError("engine not started", map[string]interface{}{"endpoint": "explain.unit"})
	}
	e.mu.Lock()
	workloads := append([]rendezgo.WorkloadConfig{}, e.template.StaticWorkloads...)
	e.mu.Unlock()

	var match *rendezgo.WorkloadConfig
	for i := range workloads {
		if workloads[i].Name == workload {
			match = &workloads[i]
			break
		}
	}
	if match == nil || unit >= match.Units {
		return ExplainUnitResult{}, e.recordError("unknown workload/unit", map[string]interface{}{"endpoint": "explain.unit", "workload": workload, "unit": unit})
	}

	slot := rendezgo.Slot{Workload: workload, Unit: unit}
	now := time.Now()
	weights := e.liveWeights("")
	ranking := rendezgo.RendezvousRanking(slot, weights)
	ctx := context.Background()
	key := e.leaseKey(slot)
	owner, _ := e.admin.Get(ctx, key).Result()
	ttl, _ := e.admin.PTTL(ctx, key).Result()
	coolKey := e.cooldownKey(slot)
	coolTTL, _ := e.admin.PTTL(ctx, coolKey).Result()

	liveNodes := make([]ExplainLiveNode, 0, len(weights))
	for _, w := range weights {
		liveNodes = append(liveNodes, ExplainLiveNode{NodeID: w.ID, ShortID: shortID(w.ID), Weight: w.Weight})
	}
	sort.Slice(liveNodes, func(i, j int) bool { return liveNodes[i].NodeID < liveNodes[j].NodeID })

	explainRanking := make([]ExplainRanking, 0, len(ranking))
	for i := 0; i < len(ranking); i++ {
		explainRanking = append(explainRanking, ExplainRanking{
			NodeID:  ranking[i].ID,
			ShortID: shortID(ranking[i].ID),
			Weight:  ranking[i].Weight,
			Score:   ranking[i].Score,
			Rank:    i + 1,
		})
	}

	res := ExplainUnitResult{
		Workload:          workload,
		Unit:              unit,
		SlotKey:           slot.Key(),
		GeneratedAtUnixMs: now.UnixMilli(),
		LiveNodes:         liveNodes,
		Ranking:           explainRanking,
		CurrentLease: ExplainLease{
			Key:   key,
			Owner: owner,
			TTLMs: maxTTL(ttl),
		},
		Cooldown: ExplainCooldown{
			Key:    coolKey,
			Active: coolTTL > 0,
			TTLMs:  maxTTL(coolTTL),
		},
	}
	e.recordEvent(EventExplainUnit, fmt.Sprintf("explain %s[%d]", workload, unit), map[string]interface{}{"workload": workload, "unit": unit})
	return res, nil
}

// RunScenario triggers a built-in scenario asynchronously.
func (e *Engine) RunScenario(name string) error {
	if name == "" {
		return e.recordError("scenario name required", map[string]interface{}{"endpoint": "scenario.run"})
	}
	go e.runScenario(e.ctx, name)
	return nil
}

// RunScenarioFile schedules events from a YAML/JSON scenario description.
func (e *Engine) RunScenarioFile(filename string, data []byte) error {
	if len(data) == 0 {
		return e.recordError("scenario file empty", map[string]interface{}{"endpoint": "scenario.upload"})
	}
	var file scenarioFile
	if strings.HasSuffix(strings.ToLower(filename), ".json") {
		if err := json.Unmarshal(data, &file.Events); err != nil {
			return e.recordError(fmt.Sprintf("parse scenario json: %v", err), map[string]interface{}{"endpoint": "scenario.upload", "file": filename})
		}
	} else {
		if err := yaml.Unmarshal(data, &file.Events); err != nil {
			return e.recordError(fmt.Sprintf("parse scenario yaml: %v", err), map[string]interface{}{"endpoint": "scenario.upload", "file": filename})
		}
	}
	start := time.Now()
	e.recordEvent(EventScenarioStart, fmt.Sprintf("scenario file %s start", filepath.Base(filename)), map[string]interface{}{"file": filename})
	for _, evt := range file.Events {
		delay, err := time.ParseDuration(evt.At)
		if err != nil {
			e.recordEvent(EventScenarioError, fmt.Sprintf("bad scenario delay %s", evt.At), map[string]interface{}{"file": filename})
			continue
		}
		wait := delay - time.Since(start)
		if wait < 0 {
			wait = 0
		}
		go func(ev scenarioEvent) {
			select {
			case <-e.ctx.Done():
				return
			case <-time.After(wait):
				e.recordEvent(EventScenarioStep, ev.Command, map[string]interface{}{"file": filename})
				if err := e.execScenarioCommand(context.Background(), ev.Command); err != nil {
					e.recordEvent(EventScenarioError, err.Error(), map[string]interface{}{"file": filename})
				}
			}
		}(evt)
	}
	go func() {
		select {
		case <-e.ctx.Done():
		case <-time.After(time.Since(start) + 2*time.Second):
			e.recordEvent(EventScenarioDone, fmt.Sprintf("scenario file %s done", filepath.Base(filename)), map[string]interface{}{"file": filename})
		}
	}()
	return nil
}

// internal helpers

func (e *Engine) addNode(weight float64, customID string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.ctx == nil {
		return "", errors.New("engine not started")
	}
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
		return "", e.recordError(fmt.Sprintf("add node %s: %v", id, err), map[string]interface{}{"nodeID": id})
	}
	nodeID := cfg.ClusterID + ":" + cfg.NodeID
	e.weights.Set(nodeID, weight)
	nodeCtx, cancel := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ctrl.Start(nodeCtx)
	}()
	n := &simNode{id: nodeID, weight: weight, cancel: cancel, done: done, ctrl: ctrl, client: client, hook: hook, health: health, healthy: true}
	e.nodes[nodeID] = n
	e.recordEvent(EventNodeAdd, fmt.Sprintf("added node %s weight=%.2f", nodeID, weight), map[string]interface{}{"nodeID": nodeID, "weight": weight})
	return nodeID, nil
}

func (e *Engine) removeNode(id string, graceful bool, eventType EventType) error {
	e.mu.Lock()
	node, ok := e.nodes[id]
	if ok {
		delete(e.nodes, id)
		e.weights.Delete(id)
	}
	e.mu.Unlock()
	if !ok {
		return e.recordError(fmt.Sprintf("node %s not found", id), map[string]interface{}{"nodeID": id})
	}
	node.ctrl.SetReleaseOnShutdown(graceful)
	node.cancel()
	if node.done != nil {
		select {
		case <-node.done:
		case <-time.After(2 * time.Second):
		}
	}
	_ = node.client.Close()
	e.recordEvent(eventType, fmt.Sprintf("removed node %s graceful=%v", id, graceful), map[string]interface{}{"nodeID": id, "graceful": graceful})
	return nil
}

func (e *Engine) restartAll() {
	ids := e.nodeIDs()
	for _, id := range ids {
		_ = e.RestartNode(id)
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

func (e *Engine) buildSnapshot(now time.Time) Snapshot {
	e.mu.Lock()
	workloads := append([]rendezgo.WorkloadConfig{}, e.template.StaticWorkloads...)
	focus := e.focus
	clusterID := e.template.ClusterID
	topK := e.opts.TopK
	cfg := e.template
	mode := e.opts.Mode
	redisAddr := e.redisAddr
	e.mu.Unlock()

	owners, leaseTTLs, cooldownTTLs := e.redisState(workloads)
	desired := rendezgo.DesiredOwners(workloads, e.liveWeights(""))
	e.updateChurn(owners)
	e.mu.Lock()
	churnCount := len(e.churn)
	e.mu.Unlock()

	nodes := e.buildNodeSnapshots(workloads, desired, owners)
	units, totals := e.buildUnitSnapshots(workloads, owners, desired, leaseTTLs, cooldownTTLs, topK)

	convergence := 0.0
	totalUnits := totals.desiredUnits
	if totalUnits > 0 {
		convergence = (float64(totalUnits-totals.misalignedUnits) / float64(totalUnits)) * 100
	}

	wlSummaries := make([]SnapshotWorkload, 0, len(workloads))
	for _, wl := range workloads {
		wlSummaries = append(wlSummaries, SnapshotWorkload{Name: wl.Name, Units: wl.Units})
	}
	sort.Slice(wlSummaries, func(i, j int) bool { return wlSummaries[i].Name < wlSummaries[j].Name })

	return Snapshot{
		NowUnixMs:     now.UnixMilli(),
		Mode:          mode,
		RedisAddr:     redisAddr,
		ClusterID:     clusterID,
		FocusWorkload: focus,
		TopK:          topK,
		Config: SnapshotConfig{
			LeaseTTLms:                     cfg.LeaseTTL.Milliseconds(),
			LeaseRenewIntervalms:           cfg.LeaseRenewInterval.Milliseconds(),
			HeartbeatTTLms:                 cfg.HeartbeatTTL.Milliseconds(),
			HeartbeatIntervalms:            cfg.HeartbeatInterval.Milliseconds(),
			ReconcileIntervalms:            cfg.ReconcileInterval.Milliseconds(),
			ReconcileJitter:                cfg.ReconcileJitter,
			MinConsumerRuntimems:           cfg.MinConsumerRuntime.Milliseconds(),
			SlotMoveCooldownms:             cfg.SlotMoveCooldown.Milliseconds(),
			RedisBackoffms:                 cfg.RedisBackoff.Milliseconds(),
			AcquireLimit:                   cfg.AcquireLimit,
			MaxConsumersPerNode:            cfg.MaxConsumersPerNode,
			MaxConsumersPerWorkloadPerNode: cfg.MaxConsumersPerWorkloadPerNode,
			MinConsumersPerNode:            cfg.MinConsumersPerNode,
			SheddingEnabled:                cfg.SheddingEnabled,
			SheddingRelease:                cfg.SheddingRelease,
		},
		Metrics: SnapshotMetrics{
			ChurnPerMinute:       churnCount,
			ConvergencePct:       convergence,
			OwnedUnitsTotal:      totals.ownedUnits,
			DesiredUnitsTotal:    totals.desiredUnits,
			MisalignedUnitsTotal: totals.misalignedUnits,
		},
		Workloads: wlSummaries,
		Nodes:     nodes,
		Units:     units,
	}
}

type unitTotals struct {
	ownedUnits      int
	desiredUnits    int
	misalignedUnits int
}

func (e *Engine) redisState(workloads []rendezgo.WorkloadConfig) (map[string]string, map[string]time.Duration, map[string]time.Duration) {
	owners := map[string]string{}
	leaseTTLs := map[string]time.Duration{}
	cooldownTTLs := map[string]time.Duration{}
	if e.admin == nil {
		return owners, leaseTTLs, cooldownTTLs
	}
	ctx := context.Background()
	pipe := e.admin.Pipeline()
	leaseCmds := map[string]*redis.StringCmd{}
	ttlCmds := map[string]*redis.DurationCmd{}
	coolCmds := map[string]*redis.DurationCmd{}
	for _, wl := range workloads {
		for unit := 0; unit < wl.Units; unit++ {
			slot := rendezgo.Slot{Workload: wl.Name, Unit: unit}
			leaseKey := e.leaseKey(slot)
			cooldownKey := e.cooldownKey(slot)
			leaseCmds[leaseKey] = pipe.Get(ctx, leaseKey)
			ttlCmds[leaseKey] = pipe.PTTL(ctx, leaseKey)
			coolCmds[cooldownKey] = pipe.PTTL(ctx, cooldownKey)
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

func (e *Engine) driveSimulatedTime(ctx context.Context, server *miniredis.Miniredis, stop <-chan struct{}) {
	now := time.Now()
	server.SetTime(now)
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	last := now
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-t.C:
			cur := time.Now()
			delta := cur.Sub(last)
			last = cur
			if delta > 0 {
				server.FastForward(delta)
			}
		}
	}
}

func (e *Engine) buildNodeSnapshots(workloads []rendezgo.WorkloadConfig, desired map[string]string, owners map[string]string) []SnapshotNode {
	e.mu.Lock()
	nodes := make(map[string]*simNode, len(e.nodes))
	for id, n := range e.nodes {
		nodes[id] = n
	}
	e.mu.Unlock()
	weightSnap := e.weights.Snapshot()
	nodeSnaps := map[string]*SnapshotNode{}
	for id := range nodes {
		nodeSnaps[id] = &SnapshotNode{
			ID:            id,
			ShortID:       shortID(id),
			Weight:        weightSnap[id],
			State:         e.nodeState(nodes[id]),
			Healthy:       nodes[id].healthy,
			RedisFault:    nodes[id].redisFail,
			BackoffActive: nodes[id].ctrl != nil && nodes[id].ctrl.BackoffActive(),
			Workloads:     []SnapshotNodeWorkload{},
		}
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
			ns = &SnapshotNode{ID: owner, ShortID: shortID(owner), Weight: weightSnap[owner], State: e.nodeState(nil), Workloads: []SnapshotNodeWorkload{}}
			nodeSnaps[owner] = ns
		}
		ns.OwnedUnits++
		ns.Workloads = ensureWL(ns.Workloads, slot.Workload)
		for i := range ns.Workloads {
			if ns.Workloads[i].Name == slot.Workload {
				ns.Workloads[i].OwnedUnits++
				break
			}
		}
	}

	for key, want := range desired {
		slot, err := e.slotFromLeaseKey(key)
		if err != nil || want == "" {
			continue
		}
		ns, ok := nodeSnaps[want]
		if !ok {
			ns = &SnapshotNode{ID: want, ShortID: shortID(want), Weight: weightSnap[want], State: e.nodeState(nil), Workloads: []SnapshotNodeWorkload{}}
			nodeSnaps[want] = ns
		}
		ns.DesiredUnits++
		ns.Workloads = ensureWL(ns.Workloads, slot.Workload)
		for i := range ns.Workloads {
			if ns.Workloads[i].Name == slot.Workload {
				ns.Workloads[i].DesiredUnits++
				break
			}
		}
	}

	out := make([]SnapshotNode, 0, len(nodeSnaps))
	for _, ns := range nodeSnaps {
		if ns.DesiredUnits > ns.OwnedUnits {
			ns.MissingDesiredUnits = ns.DesiredUnits - ns.OwnedUnits
		}
		if ns.OwnedUnits > ns.DesiredUnits {
			ns.ExtraOwnedUnits = ns.OwnedUnits - ns.DesiredUnits
		}
		sort.Slice(ns.Workloads, func(i, j int) bool { return ns.Workloads[i].Name < ns.Workloads[j].Name })
		out = append(out, *ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func ensureWL(list []SnapshotNodeWorkload, name string) []SnapshotNodeWorkload {
	for _, wl := range list {
		if wl.Name == name {
			return list
		}
	}
	return append(list, SnapshotNodeWorkload{Name: name})
}

func (e *Engine) buildUnitSnapshots(workloads []rendezgo.WorkloadConfig, owners, desired map[string]string, leaseTTLs, cooldownTTLs map[string]time.Duration, topK int) ([]SnapshotUnit, unitTotals) {
	weights := e.liveWeights("")
	units := make([]SnapshotUnit, 0)
	totals := unitTotals{}
	for _, wl := range workloads {
		for unit := 0; unit < wl.Units; unit++ {
			slot := rendezgo.Slot{Workload: wl.Name, Unit: unit}
			key := e.leaseKey(slot)
			owner := owners[key]
			want := desired[key]
			var ranking []rendezgo.NodeScore
			if topK > 0 {
				ranking = rendezgo.RendezvousTopK(slot, weights, topK)
			} else {
				ranking = rendezgo.RendezvousRanking(slot, weights)
			}
			candidates := make([]HRWCandidate, 0, len(ranking))
			for _, r := range ranking {
				candidates = append(candidates, HRWCandidate{
					NodeID:  r.ID,
					ShortID: shortID(r.ID),
					Weight:  r.Weight,
					Score:   r.Score,
				})
			}
			leaseTTL := leaseTTLs[key]
			coolKey := e.cooldownKey(slot)
			coolTTL := cooldownTTLs[coolKey]
			aligned := want != "" && owner == want
			if owner != "" {
				totals.ownedUnits++
			}
			totals.desiredUnits++
			if !aligned {
				totals.misalignedUnits++
			}
			units = append(units, SnapshotUnit{
				Workload: wl.Name,
				Unit:     unit,
				Lease: LeaseInfo{
					Key:   key,
					Owner: owner,
					TTLMs: leaseTTL.Milliseconds(),
				},
				Cooldown: CooldownInfo{
					Key:    coolKey,
					Active: coolTTL > 0,
					TTLMs:  coolTTL.Milliseconds(),
				},
				DesiredOwner: want,
				Aligned:      aligned,
				HRWTopK:      candidates,
			})
		}
	}
	sort.Slice(units, func(i, j int) bool {
		if units[i].Workload == units[j].Workload {
			return units[i].Unit < units[j].Unit
		}
		return units[i].Workload < units[j].Workload
	})
	return units, totals
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
	if ctx != nil {
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
	e.recordEvent(EventScenarioStart, fmt.Sprintf("scenario %s start", name), map[string]interface{}{"scenario": name})
	switch name {
	case "scale-out":
		e.runScenarioStep("add node burst", func() { _ = e.AddNodes(1, 1) })
		time.Sleep(time.Second)
		e.runScenarioStep("add +5", func() { _ = e.AddNodes(5, 1) })
		time.Sleep(2 * time.Second)
		e.runScenarioStep("add +10", func() { _ = e.AddNodes(10, 1) })
	case "pod-death":
		nodes := e.nodeIDs()
		target := len(nodes) / 2
		rand.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
		for i := 0; i < target; i++ {
			id := nodes[i]
			e.runScenarioStep("kill "+id, func() { _ = e.KillNode(id) })
		}
	case "redis-instability":
		nodes := e.nodeIDs()
		for i, id := range nodes {
			if i%2 == 0 {
				id := id
				e.runScenarioStep("fault "+id, func() { _ = e.SetRedisFault(id, true) })
			}
		}
		time.Sleep(3 * time.Second)
		for _, id := range nodes {
			id := id
			e.runScenarioStep("heal "+id, func() { _ = e.SetRedisFault(id, false) })
		}
	case "weight-skew":
		nodes := e.nodeIDs()
		for i, id := range nodes {
			id := id
			weight := float64(i + 1)
			e.runScenarioStep(fmt.Sprintf("weight %s %.2f", id, weight), func() { _ = e.SetWeight(id, weight) })
		}
	case "cross-cluster":
		e.runScenarioStep("spawn other cluster", e.spawnOtherCluster)
	case "lease-flapping":
		nodes := e.nodeIDs()
		if len(nodes) > 0 {
			id := nodes[0]
			for i := 0; i < 3; i++ {
				e.runScenarioStep("fault "+id, func() { _ = e.SetRedisFault(id, true) })
				time.Sleep(500 * time.Millisecond)
				e.runScenarioStep("heal "+id, func() { _ = e.SetRedisFault(id, false) })
				time.Sleep(700 * time.Millisecond)
			}
		}
	default:
		e.recordEvent(EventScenarioError, "unknown scenario", map[string]interface{}{"scenario": name})
		return
	}
	e.recordEvent(EventScenarioDone, fmt.Sprintf("scenario %s done", name), map[string]interface{}{"scenario": name})
	_ = ctx
}

func (e *Engine) runScenarioStep(label string, fn func()) {
	e.recordEvent(EventScenarioStep, label, nil)
	fn()
}

func (e *Engine) execScenarioCommand(ctx context.Context, line string) error {
	_ = ctx
	line = strings.TrimSpace(line)
	if line == "" {
		return nil
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}
	cmd := strings.ToLower(fields[0])
	args := fields[1:]
	switch cmd {
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
		return e.AddNodes(count, weight)
	case "remove":
		if len(args) < 1 {
			return fmt.Errorf("usage: remove <id>")
		}
		return e.RemoveNode(args[0], true)
	case "restart":
		if len(args) < 1 {
			return fmt.Errorf("usage: restart <id>")
		}
		return e.RestartNode(args[0])
	case "kill":
		if len(args) < 1 {
			return fmt.Errorf("usage: kill <id>")
		}
		return e.KillNode(args[0])
	case "weight":
		if len(args) < 2 {
			return fmt.Errorf("usage: weight <id> <weight>")
		}
		w, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			return err
		}
		return e.SetWeight(args[0], w)
	case "fail":
		if len(args) < 2 {
			return fmt.Errorf("usage: fail <id> on|off")
		}
		return e.SetRedisFault(args[0], args[1] == "on")
	case "health":
		if len(args) < 2 {
			return fmt.Errorf("usage: health <id> on|off")
		}
		return e.SetHealth(args[0], args[1] == "on")
	case "shedding":
		if len(args) < 1 {
			return fmt.Errorf("usage: shedding on|off")
		}
		return e.SetShedding(args[0] == "on")
	case "release":
		if len(args) < 1 {
			return fmt.Errorf("usage: release <per-interval>")
		}
		if v, err := strconv.Atoi(args[0]); err == nil {
			return e.SetSheddingRelease(v)
		}
		return fmt.Errorf("invalid release value")
	case "scenario":
		if len(args) < 1 {
			return fmt.Errorf("usage: scenario <name>")
		}
		return e.RunScenario(args[0])
	case "focus":
		if len(args) < 1 {
			return fmt.Errorf("usage: focus <workload|none>")
		}
		if args[0] == "none" {
			return e.SetFocus("")
		}
		return e.SetFocus(args[0])
	case "predict":
		if len(args) == 2 && args[0] == "down" {
			_, err := e.PredictDown(args[1])
			return err
		}
		return fmt.Errorf("usage: predict down <nodeID>")
	case "explain":
		if len(args) < 2 {
			return fmt.Errorf("usage: explain <workload> <unit>")
		}
		unit, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("unit must be integer")
		}
		_, err = e.ExplainUnit(args[0], unit)
		return err
	default:
		return fmt.Errorf("unknown command")
	}
}

func (e *Engine) spawnOtherCluster() {
	e.mu.Lock()
	cfg := e.template
	parentCtx := e.ctx
	e.mu.Unlock()
	cfg.ClusterID = "other"
	cfg.NodeID = uuid.NewString()
	client := redis.NewClient(&redis.Options{Addr: e.redisAddr})
	hook := &chaosHook{}
	client.AddHook(hook)
	health := &healthGate{healthy: true}
	ctrl, err := rendezgo.NewController(cfg, client, &playConsumerFactory{}, rendezgo.NopLogger(), rendezgo.NopMetrics(), rendezgo.WithWeightProvider(e.weights), rendezgo.WithHealthChecker(health))
	if err != nil {
		e.recordError(fmt.Sprintf("spawn other cluster: %v", err), nil)
		return
	}
	nodeID := cfg.ClusterID + ":" + cfg.NodeID
	e.weights.Set(nodeID, 1)
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	nodeCtx, cancel := context.WithCancel(parentCtx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = ctrl.Start(nodeCtx)
	}()
	e.mu.Lock()
	e.nodes[nodeID] = &simNode{id: nodeID, weight: 1, cancel: cancel, done: done, ctrl: ctrl, client: client, hook: hook, health: health, healthy: true}
	e.mu.Unlock()
	e.recordEvent(EventNodeAdd, fmt.Sprintf("added other cluster node %s", nodeID), map[string]interface{}{"nodeID": nodeID})
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

func shortID(owner string) string {
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

func (e *Engine) recordEvent(t EventType, msg string, fields map[string]interface{}) {
	e.events.Append(Event{AtUnixMs: time.Now().UnixMilli(), Type: t, Message: msg, Fields: fields})
}

func (e *Engine) recordError(msg string, fields map[string]interface{}) error {
	e.recordEvent(EventError, msg, fields)
	return errors.New(msg)
}

// EmitEvent allows external callers (HTTP layer) to push events.
func (e *Engine) EmitEvent(t EventType, msg string, fields map[string]interface{}) {
	e.recordEvent(t, msg, fields)
}

// EmitErrorEvent records an error event and returns it as an error for convenience.
func (e *Engine) EmitErrorEvent(msg string, fields map[string]interface{}) error {
	return e.recordError(msg, fields)
}

func interpolateSnapshot(s Snapshot, drift time.Duration) Snapshot {
	cp := deepCopySnapshot(s)
	cp.NowUnixMs = cp.NowUnixMs + drift.Milliseconds()
	for i := range cp.Units {
		if cp.Units[i].Lease.TTLMs > 0 {
			cp.Units[i].Lease.TTLMs = max64(cp.Units[i].Lease.TTLMs-drift.Milliseconds(), 0)
		}
		if cp.Units[i].Cooldown.TTLMs > 0 {
			cp.Units[i].Cooldown.TTLMs = max64(cp.Units[i].Cooldown.TTLMs-drift.Milliseconds(), 0)
			cp.Units[i].Cooldown.Active = cp.Units[i].Cooldown.TTLMs > 0
		}
	}
	return cp
}

func deepCopySnapshot(s Snapshot) Snapshot {
	cp := s
	cp.Workloads = append([]SnapshotWorkload{}, s.Workloads...)
	cp.Nodes = append([]SnapshotNode{}, s.Nodes...)
	for i := range cp.Nodes {
		cp.Nodes[i].Workloads = append([]SnapshotNodeWorkload{}, s.Nodes[i].Workloads...)
	}
	cp.Units = append([]SnapshotUnit{}, s.Units...)
	for i := range cp.Units {
		cp.Units[i].HRWTopK = append([]HRWCandidate{}, s.Units[i].HRWTopK...)
	}
	return cp
}

func maxTTL(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return ttl.Milliseconds()
}

func max64(v int64, floor int64) int64 {
	if v < floor {
		return floor
	}
	return v
}

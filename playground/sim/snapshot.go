package sim

// Snapshot represents a point-in-time view of the simulation state.
type Snapshot struct {
	NowUnixMs     int64              `json:"nowUnixMs"`
	Mode          string             `json:"mode"`
	RedisAddr     string             `json:"redisAddr"`
	ClusterID     string             `json:"clusterID"`
	FocusWorkload string             `json:"focusWorkload"`
	TopK          int                `json:"topK"`
	Config        SnapshotConfig     `json:"config"`
	Metrics       SnapshotMetrics    `json:"metrics"`
	Workloads     []SnapshotWorkload `json:"workloads"`
	Nodes         []SnapshotNode     `json:"nodes"`
	Units         []SnapshotUnit     `json:"units"`
}

// SnapshotConfig exposes the controller configuration in milliseconds.
type SnapshotConfig struct {
	LeaseTTLms                     int64   `json:"leaseTTLms"`
	LeaseRenewIntervalms           int64   `json:"leaseRenewIntervalms"`
	HeartbeatTTLms                 int64   `json:"heartbeatTTLms"`
	HeartbeatIntervalms            int64   `json:"heartbeatIntervalms"`
	ReconcileIntervalms            int64   `json:"reconcileIntervalms"`
	ReconcileJitter                float64 `json:"reconcileJitter"`
	MinConsumerRuntimems           int64   `json:"minConsumerRuntimems"`
	SlotMoveCooldownms             int64   `json:"slotMoveCooldownms"`
	RedisBackoffms                 int64   `json:"redisBackoffms"`
	AcquireLimit                   int     `json:"acquireLimit"`
	MaxConsumersPerNode            int     `json:"maxConsumersPerNode"`
	MaxConsumersPerWorkloadPerNode int     `json:"maxConsumersPerWorkloadPerNode"`
	MinConsumersPerNode            int     `json:"minConsumersPerNode"`
	SheddingEnabled                bool    `json:"sheddingEnabled"`
	SheddingRelease                int     `json:"sheddingRelease"`
}

// SnapshotMetrics includes smoothed convergence and churn figures.
type SnapshotMetrics struct {
	ChurnPerMinute       int     `json:"churnPerMinute"`
	ConvergencePct       float64 `json:"convergencePct"`
	OwnedUnitsTotal      int     `json:"ownedUnitsTotal"`
	DesiredUnitsTotal    int     `json:"desiredUnitsTotal"`
	MisalignedUnitsTotal int     `json:"misalignedUnitsTotal"`
}

// SnapshotWorkload lists workloads and unit counts.
type SnapshotWorkload struct {
	Name  string `json:"name"`
	Units int    `json:"units"`
}

// SnapshotNode captures per-node ownership and health.
type SnapshotNode struct {
	ID                  string                 `json:"id"`
	ShortID             string                 `json:"shortID"`
	Weight              float64                `json:"weight"`
	State               string                 `json:"state"`
	BackoffActive       bool                   `json:"backoffActive"`
	Healthy             bool                   `json:"healthy"`
	RedisFault          bool                   `json:"redisFault"`
	OwnedUnits          int                    `json:"ownedUnits"`
	DesiredUnits        int                    `json:"desiredUnits"`
	MissingDesiredUnits int                    `json:"missingDesiredUnits"`
	ExtraOwnedUnits     int                    `json:"extraOwnedUnits"`
	Workloads           []SnapshotNodeWorkload `json:"workloads"`
}

// SnapshotNodeWorkload shows ownership per workload on a node.
type SnapshotNodeWorkload struct {
	Name         string `json:"name"`
	OwnedUnits   int    `json:"ownedUnits"`
	DesiredUnits int    `json:"desiredUnits"`
}

// SnapshotUnit shows lease, cooldown, desired owner, and HRW ranking for a unit.
type SnapshotUnit struct {
	Workload     string         `json:"workload"`
	Unit         int            `json:"unit"`
	Lease        LeaseInfo      `json:"lease"`
	Cooldown     CooldownInfo   `json:"cooldown"`
	DesiredOwner string         `json:"desiredOwner"`
	Aligned      bool           `json:"aligned"`
	HRWTopK      []HRWCandidate `json:"hrwTopK"`
}

// LeaseInfo reports the current lease state.
type LeaseInfo struct {
	Key   string `json:"key"`
	Owner string `json:"owner"`
	TTLMs int64  `json:"ttlMs"`
}

// CooldownInfo reports the current cooldown state.
type CooldownInfo struct {
	Key    string `json:"key"`
	Active bool   `json:"active"`
	TTLMs  int64  `json:"ttlMs"`
}

// HRWCandidate captures ranked HRW scores.
type HRWCandidate struct {
	NodeID  string  `json:"nodeID"`
	ShortID string  `json:"shortID"`
	Weight  float64 `json:"weight"`
	Score   float64 `json:"score"`
}

// PredictDownResult expresses expected movements if a node disappears.
type PredictDownResult struct {
	NodeID             string             `json:"nodeID"`
	RemovedFromLiveSet bool               `json:"removedFromLiveSet"`
	GeneratedAtUnixMs  int64              `json:"generatedAtUnixMs"`
	Moves              []PredictedMove    `json:"moves"`
	Summary            PredictDownSummary `json:"summary"`
}

// PredictedMove shows a single slot movement forecast.
type PredictedMove struct {
	Workload   string         `json:"workload"`
	Unit       int            `json:"unit"`
	FromOwner  string         `json:"fromOwner"`
	ToOwner    string         `json:"toOwner"`
	Reason     string         `json:"reason"`
	Candidates []HRWCandidate `json:"candidates"`
}

// PredictDownSummary aggregates move counts.
type PredictDownSummary struct {
	UnitsImpacted int                       `json:"unitsImpacted"`
	ByToOwner     []PredictDownSummaryOwner `json:"byToOwner"`
}

// PredictDownSummaryOwner aggregates counts per destination owner.
type PredictDownSummaryOwner struct {
	NodeID string `json:"nodeID"`
	Count  int    `json:"count"`
}

// ExplainUnitResult provides a detailed HRW breakdown for a specific unit.
type ExplainUnitResult struct {
	Workload          string            `json:"workload"`
	Unit              int               `json:"unit"`
	SlotKey           string            `json:"slotKey"`
	GeneratedAtUnixMs int64             `json:"generatedAtUnixMs"`
	LiveNodes         []ExplainLiveNode `json:"liveNodes"`
	Ranking           []ExplainRanking  `json:"ranking"`
	CurrentLease      ExplainLease      `json:"currentLease"`
	Cooldown          ExplainCooldown   `json:"cooldown"`
}

// ExplainLiveNode lists live node weights.
type ExplainLiveNode struct {
	NodeID  string  `json:"nodeID"`
	ShortID string  `json:"shortID"`
	Weight  float64 `json:"weight"`
}

// ExplainRanking shows HRW score and rank.
type ExplainRanking struct {
	NodeID  string  `json:"nodeID"`
	ShortID string  `json:"shortID"`
	Weight  float64 `json:"weight"`
	Score   float64 `json:"score"`
	Rank    int     `json:"rank"`
}

// ExplainLease captures the current lease state.
type ExplainLease struct {
	Key   string `json:"key"`
	Owner string `json:"owner"`
	TTLMs int64  `json:"ttlMs"`
}

// ExplainCooldown captures cooldown information.
type ExplainCooldown struct {
	Key    string `json:"key"`
	Active bool   `json:"active"`
	TTLMs  int64  `json:"ttlMs"`
}

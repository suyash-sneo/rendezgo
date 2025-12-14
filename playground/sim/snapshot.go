package sim

import "time"

type Snapshot struct {
	Now            time.Time         `json:"now"`
	RedisAddr      string            `json:"redisAddr"`
	Mode           string            `json:"mode"`
	ChurnPerMinute int               `json:"churnPerMinute"`
	Convergence    float64           `json:"convergence"`
	Focus          string            `json:"focus"`
	TopK           int               `json:"topK"`
	Caps           CapsSummary       `json:"caps"`
	Workloads      []WorkloadSummary `json:"workloads"`
	Nodes          []NodeSnapshot    `json:"nodes"`
	Units          []UnitSnapshot    `json:"units"`
}

type CapsSummary struct {
	MaxPerNode      int  `json:"maxPerNode"`
	MaxPerWorkload  int  `json:"maxPerWorkload"`
	MinPerNode      int  `json:"minPerNode"`
	SheddingEnabled bool `json:"sheddingEnabled"`
	SheddingRelease int  `json:"sheddingRelease"`
}

type WorkloadSummary struct {
	Name  string `json:"name"`
	Units int    `json:"units"`
}

type NodeSnapshot struct {
	ID       string                   `json:"id"`
	Weight   float64                  `json:"weight"`
	State    string                   `json:"state"`
	Owned    int                      `json:"owned"`
	Desired  int                      `json:"desired"`
	Missing  int                      `json:"missing"`
	Extra    int                      `json:"extra"`
	Workload map[string]WorkloadCount `json:"workload"`
}

type WorkloadCount struct {
	Owned   int `json:"owned"`
	Desired int `json:"desired"`
}

type UnitSnapshot struct {
	Workload     string           `json:"workload"`
	Unit         int              `json:"unit"`
	CurrentOwner string           `json:"currentOwner"`
	DesiredOwner string           `json:"desiredOwner"`
	LeaseTTL     time.Duration    `json:"leaseTTL"`
	CooldownTTL  time.Duration    `json:"cooldownTTL"`
	Ranking      []CandidateScore `json:"ranking"`
	Aligned      bool             `json:"aligned"`
	Cooldown     bool             `json:"cooldown"`
	Unowned      bool             `json:"unowned"`
}

type CandidateScore struct {
	ID     string  `json:"id"`
	Score  float64 `json:"score"`
	Weight float64 `json:"weight"`
}

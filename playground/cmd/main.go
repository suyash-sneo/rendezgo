package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/suyash-sneo/rendezgo/playground/sim"
	"github.com/suyash-sneo/rendezgo/playground/ui/tui"
)

func main() {
	var (
		mode         string
		redisAddr    string
		workloadsVal string
		initialNodes int
		scenario     string
		scenarioFile string
		topK         int
	)
	flag.StringVar(&mode, "mode", "simulated", "simulated or real")
	flag.StringVar(&redisAddr, "redis", "127.0.0.1:6379", "redis address for real mode")
	flag.StringVar(&workloadsVal, "workloads", "demo:8", "workloads as workload:units,...")
	flag.IntVar(&initialNodes, "nodes", 3, "initial node count")
	flag.StringVar(&scenario, "scenario", "", "built-in scenario to run")
	flag.StringVar(&scenarioFile, "scenario-file", "", "path to YAML/JSON scenario file")
	flag.IntVar(&topK, "topk", 3, "top K HRW candidates to display")
	flag.Parse()

	workloads := parseWorkloads(workloadsVal)
	if len(workloads) == 0 {
		fmt.Fprintln(os.Stderr, "no workloads provided")
		os.Exit(1)
	}

	eng, err := sim.NewEngine(sim.Options{
		Mode:         sim.Mode(mode),
		RedisAddr:    redisAddr,
		Workloads:    workloads,
		InitialNodes: initialNodes,
		TopK:         topK,
		Scenario:     scenario,
		ScenarioFile: scenarioFile,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "engine init: %v\n", err)
		os.Exit(1)
	}
	if err := eng.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "engine start: %v\n", err)
		os.Exit(1)
	}
	defer eng.Stop()

	if err := tui.Run(eng); err != nil {
		fmt.Fprintf(os.Stderr, "ui error: %v\n", err)
		os.Exit(1)
	}
}

func parseWorkloads(val string) []sim.WorkloadConfig {
	parts := strings.Split(val, ",")
	var out []sim.WorkloadConfig
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
		out = append(out, sim.WorkloadConfig{Name: kv[0], Units: units})
	}
	return out
}

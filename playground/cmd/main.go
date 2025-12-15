package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/suyash-sneo/rendezgo/playground/server"
	"github.com/suyash-sneo/rendezgo/playground/sim"
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
		listen       string
		autoOpen     bool
	)
	flag.StringVar(&mode, "mode", "simulated", "simulated or real")
	flag.StringVar(&redisAddr, "redis", "127.0.0.1:6379", "redis address for real mode")
	flag.StringVar(&workloadsVal, "workloads", "demo:8", "workloads as workload:units,...")
	flag.IntVar(&initialNodes, "nodes", 3, "initial node count")
	flag.StringVar(&scenario, "scenario", "", "built-in scenario to run")
	flag.StringVar(&scenarioFile, "scenario-file", "", "path to YAML/JSON scenario file")
	flag.IntVar(&topK, "topk", 3, "top K HRW candidates to display")
	flag.StringVar(&listen, "listen", "127.0.0.1:8080", "listen address")
	flag.BoolVar(&autoOpen, "open", true, "open browser automatically")
	flag.Parse()

	workloads := parseWorkloads(workloadsVal)
	if len(workloads) == 0 {
		fmt.Fprintln(os.Stderr, "no workloads provided")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng, err := sim.NewEngine(sim.EngineOptions{
		Mode:             mode,
		RedisAddr:        redisAddr,
		Workloads:        workloads,
		InitialNodes:     initialNodes,
		TopK:             topK,
		SnapshotInterval: time.Second,
		ScenarioName:     scenario,
		ScenarioFilePath: scenarioFile,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "engine init: %v\n", err)
		os.Exit(1)
	}
	if err := eng.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "engine start: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close()

	srv, err := server.New(eng)
	if err != nil {
		fmt.Fprintf(os.Stderr, "server init: %v\n", err)
		os.Exit(1)
	}

	url := fmt.Sprintf("http://%s", listen)
	fmt.Printf("rendezgo playground listening on %s\n", url)
	if autoOpen {
		go openBrowser(url)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(ctx, listen)
	}()

	select {
	case <-ctx.Done():
		<-errCh
	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
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

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url) //nolint:gosec
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url) //nolint:gosec
	default:
		cmd = exec.Command("xdg-open", url) //nolint:gosec
	}
	_ = cmd.Start()
}

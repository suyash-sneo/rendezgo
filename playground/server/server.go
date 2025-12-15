package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/suyash-sneo/rendezgo/playground/sim"
	"github.com/suyash-sneo/rendezgo/playground/web"
)

// Server exposes HTTP API + static UI for the playground.
type Server struct {
	eng    *sim.Engine
	mux    *http.ServeMux
	assets fs.FS
	srv    *http.Server
}

// New constructs a server bound to an engine.
func New(eng *sim.Engine) (*Server, error) {
	assetFS, err := fs.Sub(web.Assets, ".")
	if err != nil {
		return nil, err
	}
	return &Server{
		eng:    eng,
		mux:    http.NewServeMux(),
		assets: assetFS,
	}, nil
}

// Start begins serving on the provided address.
func (s *Server) Start(ctx context.Context, listen string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.registerRoutes()
	s.srv = &http.Server{Addr: listen, Handler: s.mux}

	go func() {
		<-ctx.Done()
		_ = s.srv.Shutdown(context.Background())
	}()

	err := s.srv.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/", s.handleIndex)
	s.mux.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(s.mustSub("assets")))))

	s.mux.HandleFunc("/api/v1/snapshot", s.handleSnapshot)
	s.mux.HandleFunc("/api/v1/events", s.handleEvents)

	s.mux.HandleFunc("/api/v1/nodes/add", s.handleAddNodes)
	s.mux.HandleFunc("/api/v1/nodes/remove", s.handleRemoveNode)
	s.mux.HandleFunc("/api/v1/nodes/restart", s.handleRestartNode)
	s.mux.HandleFunc("/api/v1/nodes/kill", s.handleKillNode)
	s.mux.HandleFunc("/api/v1/nodes/weight", s.handleWeight)
	s.mux.HandleFunc("/api/v1/nodes/health", s.handleHealth)
	s.mux.HandleFunc("/api/v1/nodes/redisFault", s.handleRedisFault)

	s.mux.HandleFunc("/api/v1/shedding/toggle", s.handleShedding)
	s.mux.HandleFunc("/api/v1/shedding/release", s.handleSheddingRelease)
	s.mux.HandleFunc("/api/v1/focus", s.handleFocus)

	s.mux.HandleFunc("/api/v1/scenario/run", s.handleScenario)
	s.mux.HandleFunc("/api/v1/scenario/upload", s.handleScenarioUpload)

	s.mux.HandleFunc("/api/v1/predict/down", s.handlePredictDown)
	s.mux.HandleFunc("/api/v1/explain/unit", s.handleExplainUnit)
}

func (s *Server) mustSub(path string) fs.FS {
	sub, err := fs.Sub(s.assets, path)
	if err != nil {
		panic(err)
	}
	return sub
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	data, err := fs.ReadFile(s.assets, "index.html")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "index missing")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "bad_request", "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, s.eng.Snapshot())
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "bad_request", "method not allowed")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal_error", "streaming unsupported")
		return
	}
	sinceParam := r.URL.Query().Get("since")
	var since uint64
	if sinceParam != "" {
		val, err := strconv.ParseUint(sinceParam, 10, 64)
		if err != nil {
			s.eng.EmitErrorEvent("invalid since param", map[string]interface{}{"endpoint": "events", "since": sinceParam})
			writeError(w, http.StatusBadRequest, "bad_request", "invalid since param")
			return
		}
		since = val
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	send := func(events []sim.Event) {
		for _, ev := range events {
			payload, _ := json.Marshal(ev)
			fmt.Fprintf(w, "event: %s\n", ev.Type)
			fmt.Fprintf(w, "data: %s\n\n", payload)
		}
		flusher.Flush()
	}

	evs, latest := s.eng.EventsSince(since)
	if len(evs) > 0 {
		send(evs)
		since = latest
	}

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
			evs, latest = s.eng.EventsSince(since)
			if len(evs) == 0 {
				continue
			}
			send(evs)
			since = latest
		}
	}
}

func (s *Server) handleAddNodes(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Count  int     `json:"count"`
		Weight float64 `json:"weight"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "nodes.add"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.Count == 0 {
		req.Count = 1
	}
	if req.Weight == 0 {
		req.Weight = 1
	}
	if err := s.eng.AddNodes(req.Count, req.Weight); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleRemoveNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID   string `json:"nodeID"`
		Graceful bool   `json:"graceful"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "nodes.remove"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.NodeID == "" {
		msg := "nodeID required"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "nodes.remove"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	if err := s.eng.RemoveNode(req.NodeID, req.Graceful); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleRestartNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID string `json:"nodeID"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "nodes.restart"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.NodeID == "" {
		msg := "nodeID required"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "nodes.restart"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	if err := s.eng.RestartNode(req.NodeID); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleKillNode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID string `json:"nodeID"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "nodes.kill"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.NodeID == "" {
		msg := "nodeID required"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "nodes.kill"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	if err := s.eng.KillNode(req.NodeID); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleWeight(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID string  `json:"nodeID"`
		Weight float64 `json:"weight"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "nodes.weight"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.NodeID == "" {
		msg := "nodeID required"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "nodes.weight"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	if req.Weight <= 0 {
		msg := "weight must be >0"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "nodes.weight"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	if err := s.eng.SetWeight(req.NodeID, req.Weight); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID  string `json:"nodeID"`
		Healthy bool   `json:"healthy"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "nodes.health"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.NodeID == "" {
		msg := "nodeID required"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "nodes.health"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	if err := s.eng.SetHealth(req.NodeID, req.Healthy); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleRedisFault(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID string `json:"nodeID"`
		Fail   bool   `json:"fail"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "nodes.redisFault"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.NodeID == "" {
		msg := "nodeID required"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "nodes.redisFault"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	if err := s.eng.SetRedisFault(req.NodeID, req.Fail); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleShedding(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "shedding.toggle"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := s.eng.SetShedding(req.Enabled); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleSheddingRelease(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PerInterval int `json:"perInterval"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "shedding.release"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.PerInterval < 0 {
		msg := "perInterval must be >=0"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "shedding.release"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	if err := s.eng.SetSheddingRelease(req.PerInterval); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleFocus(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Workload string `json:"workload"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "focus"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if err := s.eng.SetFocus(strings.TrimSpace(req.Workload)); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleScenario(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "scenario.run"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.Name == "" {
		msg := "scenario name required"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "scenario.run"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	if err := s.eng.RunScenario(req.Name); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleScenarioUpload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(4 << 20); err != nil {
		s.eng.EmitErrorEvent("parse form failed", map[string]interface{}{"endpoint": "scenario.upload"})
		writeError(w, http.StatusBadRequest, "bad_request", "invalid form")
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		s.eng.EmitErrorEvent("missing file", map[string]interface{}{"endpoint": "scenario.upload"})
		writeError(w, http.StatusBadRequest, "bad_request", "file required")
		return
	}
	defer file.Close()
	if !isAllowedScenarioExt(header) {
		msg := "unsupported file type"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "scenario.upload"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	data, err := io.ReadAll(file)
	if err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "scenario.upload"})
		writeError(w, http.StatusBadRequest, "bad_request", "read error")
		return
	}
	if err := s.eng.RunScenarioFile(header.Filename, data); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func isAllowedScenarioExt(h *multipart.FileHeader) bool {
	ext := strings.ToLower(filepath.Ext(h.Filename))
	return ext == ".yaml" || ext == ".yml" || ext == ".json"
}

func (s *Server) handlePredictDown(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NodeID string `json:"nodeID"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "predict.down"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.NodeID == "" {
		msg := "nodeID required"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "predict.down"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	res, err := s.eng.PredictDown(req.NodeID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleExplainUnit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Workload string `json:"workload"`
		Unit     int    `json:"unit"`
		TopN     int    `json:"topN"`
	}
	if err := decodeJSON(r, &req); err != nil {
		s.eng.EmitErrorEvent(err.Error(), map[string]interface{}{"endpoint": "explain.unit"})
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.Workload == "" {
		msg := "workload required"
		s.eng.EmitErrorEvent(msg, map[string]interface{}{"endpoint": "explain.unit"})
		writeError(w, http.StatusBadRequest, "bad_request", msg)
		return
	}
	res, err := s.eng.ExplainUnit(req.Workload, req.Unit)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if req.TopN > 0 && req.TopN < len(res.Ranking) {
		res.Ranking = res.Ranking[:req.TopN]
	}
	writeJSON(w, http.StatusOK, res)
}

func decodeJSON(r *http.Request, dst interface{}) error {
	if r.Method != http.MethodPost {
		return fmt.Errorf("method not allowed")
	}
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]interface{}{
		"error": map[string]string{
			"code":    code,
			"message": msg,
		},
	})
}

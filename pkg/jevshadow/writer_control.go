package jevshadow

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrPatchWriterPaused = errors.New("executor patch writer is paused")

type patchIntervalKey struct{}
type patchBudgetDeferred struct{ next time.Time }

func (e *patchBudgetDeferred) Error() string { return "executor patch budget not yet available" }
func (e *patchBudgetDeferred) retrySeconds() int {
	seconds := int(math.Ceil(time.Until(e.next).Seconds()))
	if seconds < 1 {
		return 1
	}
	return seconds
}

type patchWriterState struct {
	Version       int                  `json:"version"`
	Revision      uint64               `json:"revision"`
	Paused        bool                 `json:"paused"`
	NextAttemptAt time.Time            `json:"next_attempt_at"`
	NextByMessage map[string]time.Time `json:"next_by_message"`
	LastActor     string               `json:"last_actor,omitempty"`
	LastEvidence  string               `json:"last_evidence,omitempty"`
	ControlledAt  time.Time            `json:"controlled_at,omitempty"`
}
type patchWriterGate struct {
	mu       sync.Mutex
	path     string
	interval time.Duration
	state    patchWriterState
}

func newPatchWriterGate(dir string, interval time.Duration) (*patchWriterGate, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	g := &patchWriterGate{path: filepath.Join(dir, "writer-control.json"), interval: interval,
		state: patchWriterState{Version: 1, Revision: 1, NextByMessage: map[string]time.Time{}}}
	raw, err := os.ReadFile(g.path)
	if errors.Is(err, os.ErrNotExist) {
		return g, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) > 8<<20 {
		return nil, errors.New("executor writer control exceeds size limit")
	}
	if err := json.Unmarshal(raw, &g.state); err != nil {
		return nil, err
	}
	if g.state.Version != 1 || g.state.Revision < 1 || g.state.NextByMessage == nil {
		return nil, errors.New("invalid executor writer control state")
	}
	return g, nil
}

// The persisted gate covers all scopes and every real PATCH attempt including
// explicit platform rejections and retries. A budget denial never creates a
// pending external intent and returns immediately; Jev retains the task.
func (g *patchWriterGate) withAttempt(ctx context.Context, messageID string, send func() error) error {
	if !g.mu.TryLock() {
		return ErrCardWriterBusy
	}
	defer g.mu.Unlock()
	if g.state.Paused {
		return ErrPatchWriterPaused
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	min := g.interval
	if min > 0 && min < 15*time.Second {
		min = 15 * time.Second
	}
	if header, ok := ctx.Value(patchIntervalKey{}).(time.Duration); ok && header > min {
		min = header
	}
	now := time.Now().UTC()
	key := cardStateKey(messageID)
	next := g.state.NextAttemptAt
	if perCard := g.state.NextByMessage[key]; perCard.After(next) {
		next = perCard
	}
	if now.Before(next) {
		return &patchBudgetDeferred{next: next}
	}
	if g.interval > 0 || min > 0 {
		before := g.state
		g.state.NextByMessage = make(map[string]time.Time, len(before.NextByMessage)+1)
		for k, value := range before.NextByMessage {
			if value.After(now) {
				g.state.NextByMessage[k] = value
			}
		}
		g.state.NextAttemptAt = now.Add(g.interval)
		g.state.NextByMessage[key] = now.Add(min)
		if err := g.persist(); err != nil {
			g.state = before
			return fmt.Errorf("%w: persist PATCH budget: %v", ErrCardWriterUnavailable, err)
		}
	}
	return send()
}

func (g *patchWriterGate) persist() error {
	temp, err := os.CreateTemp(filepath.Dir(g.path), ".writer-control-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	if err := temp.Chmod(0600); err != nil {
		return err
	}
	if err := json.NewEncoder(temp).Encode(g.state); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp.Name(), g.path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(g.path))
}

// This control credential is independent from receipt, feedback and annotation
// writers. It stops PATCH for the whole regional executor, including historical
// scopes, while preserving successful-delivery intake and all durable receipts.
func (s *Service) HandleWriterControl(token string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authorization := []byte(r.Header.Get("Authorization"))
		control := len(token) >= 32 && subtle.ConstantTimeCompare(authorization, []byte("Bearer "+token)) == 1
		readToken := s.config.CutoverReadToken
		readonly := r.Method == http.MethodGet && len(readToken) >= 32 && subtle.ConstantTimeCompare(authorization, []byte("Bearer "+readToken)) == 1
		if !control && !readonly {
			w.WriteHeader(401)
			return
		}
		if s.writerGate == nil {
			w.WriteHeader(503)
			return
		}
		g := s.writerGate
		if r.Method == http.MethodGet {
			g.mu.Lock()
			defer g.mu.Unlock()
			writeJSON(w, 200, map[string]interface{}{"state": g.state, "patch_interval_seconds": g.interval.Seconds(), "permission_domain": s.config.PermissionDomain, "execution_domain": s.config.ExecutionDomain, "formal_disabled": s.config.FormalWriterDisabled})
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(405)
			return
		}
		var request struct {
			Action           string `json:"action"`
			ExpectedRevision uint64 `json:"expected_revision"`
			PermissionDomain string `json:"permission_domain"`
			Actor            string `json:"actor"`
			Evidence         string `json:"evidence"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&request); err != nil {
			w.WriteHeader(400)
			return
		}
		if ensureEOF(dec) != nil || request.Actor == "" || len(request.Actor) > 128 || request.Evidence == "" || len(request.Evidence) > 512 || request.PermissionDomain != s.config.PermissionDomain {
			w.WriteHeader(400)
			return
		}
		if request.Action != "pause" && request.Action != "resume" {
			w.WriteHeader(400)
			return
		}
		// Returning success is a drain barrier: a bounded PATCH already in flight
		// finishes first; no subsequent request crosses the paused gate.
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.state.Revision != request.ExpectedRevision {
			w.WriteHeader(409)
			return
		}
		before := g.state
		g.state.Paused = request.Action == "pause"
		g.state.Revision++
		g.state.LastActor, g.state.LastEvidence, g.state.ControlledAt = request.Actor, request.Evidence, time.Now().UTC()
		if err := g.persist(); err != nil {
			g.state = before
			w.WriteHeader(503)
			return
		}
		writeJSON(w, 200, g.state)
	}
}

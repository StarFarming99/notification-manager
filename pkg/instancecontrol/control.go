// Package instancecontrol exposes the opt-in local-ledger release protocol.
// Its credentials and state are independent of notification delivery profiles.
package instancecontrol

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
)

const maxTransferBytes = 64 << 20

type Manager struct {
	store                   *spool.Store
	readToken, controlToken string
	syncKey                 []byte
	syncPeer                *SyncPeer
	syncClient              *http.Client
}

// New configures the persisted instance identity before any admission or send.
// The protocol key authenticates cross-ledger exports, retirement proofs and
// the narrowly scoped single-hop synchronous peer route. General HTTP read and
// control authority still require their independent credentials.
func New(db *spool.Store, id, mode, readToken, controlToken string, proofKey []byte) (*Manager, error) {
	if db == nil {
		return nil, errors.New("instance release requires the local durable notification store")
	}
	if len(readToken) < 32 || len(controlToken) < 32 || len(proofKey) < 32 || readToken == controlToken || readToken == string(proofKey) || controlToken == string(proofKey) {
		return nil, errors.New("three independent instance read, control and proof credentials are required")
	}
	if err := db.ConfigureRelease(id, mode, proofKey); err != nil {
		return nil, err
	}
	return &Manager{store: db, readToken: readToken, controlToken: controlToken, syncKey: append([]byte(nil), proofKey...)}, nil
}

func credentialFile(name string) (string, error) {
	path := os.Getenv(name)
	if !filepath.IsAbs(path) {
		return "", errors.New("instance credential file must be an absolute path")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", errors.New("instance credential file is unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("instance credential must be a regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil || len(raw) > 4096 {
		return "", errors.New("instance credential file exceeds the bounded size")
	}
	value := strings.TrimSpace(string(raw))
	if len(value) < 32 || strings.ContainsAny(value, "\r\n\t ") {
		return "", errors.New("instance credential must contain at least 32 non-whitespace bytes")
	}
	return value, nil
}

func FromEnv(db *spool.Store) (*Manager, error) {
	id := os.Getenv("NM_INSTANCE_ID")
	if id == "" {
		for _, name := range []string{"NM_INSTANCE_INITIAL_MODE", "NM_INSTANCE_READ_TOKEN_FILE", "NM_INSTANCE_CONTROL_TOKEN_FILE", "NM_INSTANCE_PROTOCOL_KEY_FILE", "NM_INSTANCE_SYNC_PEER_ID", "NM_INSTANCE_SYNC_PEER_URL", "NM_INSTANCE_SYNC_ALLOW_HTTP"} {
			if os.Getenv(name) != "" {
				return nil, errors.New("instance release configuration is incomplete")
			}
		}
		if db != nil {
			state, err := db.ReleaseStatus()
			if err != nil {
				return nil, err
			}
			if state.InstanceID != "" {
				return nil, errors.New("persisted release instance requires its configured identity and credentials")
			}
		}
		return nil, nil
	}
	values := make([]string, 0, 3)
	for _, name := range []string{"NM_INSTANCE_READ_TOKEN_FILE", "NM_INSTANCE_CONTROL_TOKEN_FILE", "NM_INSTANCE_PROTOCOL_KEY_FILE"} {
		value, err := credentialFile(name)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	manager, err := New(db, id, os.Getenv("NM_INSTANCE_INITIAL_MODE"), values[0], values[1], []byte(values[2]))
	if err != nil {
		return nil, err
	}
	peerURL, peerID := os.Getenv("NM_INSTANCE_SYNC_PEER_URL"), os.Getenv("NM_INSTANCE_SYNC_PEER_ID")
	if peerURL != "" || peerID != "" {
		if err := manager.ConfigureSynchronousPeer(SyncPeer{ID: peerID, URL: peerURL, AllowHTTP: os.Getenv("NM_INSTANCE_SYNC_ALLOW_HTTP") == "true"}, nil); err != nil {
			return nil, err
		}
	}
	return manager, nil
}

func (m *Manager) guard(token string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized instance protocol request", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (m *Manager) ReadGuard(next http.HandlerFunc) http.HandlerFunc {
	return m.guard(m.readToken, next)
}
func (m *Manager) ControlGuard(next http.HandlerFunc) http.HandlerFunc {
	return m.guard(m.controlToken, next)
}

func decode(w http.ResponseWriter, r *http.Request, limit int64, dst interface{}) bool {
	defer r.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil || int64(len(raw)) > limit {
		http.Error(w, "instance protocol request exceeds its bounded size", http.StatusRequestEntityTooLarge)
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		http.Error(w, "invalid instance protocol request", http.StatusBadRequest)
		return false
	}
	if err := dec.Decode(new(interface{})); err != io.EOF {
		http.Error(w, "one instance protocol object required", http.StatusBadRequest)
		return false
	}
	return true
}

func reply(w http.ResponseWriter, value interface{}, err error) {
	if err != nil {
		// Never echo transfer rows, payloads, credentials, or caller evidence.
		code := "state_or_manifest_invalid"
		switch {
		case errors.Is(err, spool.ErrReleaseDeadline):
			code = "deadline_exceeded"
		case errors.Is(err, spool.ErrReleaseConflict):
			code = "revision_or_identity_conflict"
		case errors.Is(err, spool.ErrReleaseGate):
			code = "gate_closed"
		case errors.Is(err, spool.ErrAmbiguousGeneration):
			code = "generation_ambiguous"
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"error": "instance_protocol_rejected", "code": code, "reconcile": "authenticated GET /internal/instance"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (m *Manager) Status(w http.ResponseWriter, r *http.Request) {
	state, err := m.store.ReleaseStatus()
	if err != nil {
		reply(w, nil, err)
		return
	}
	status, err := m.store.Status()
	if err != nil {
		reply(w, nil, err)
		return
	}
	transferBytes, err := m.store.EstimateReleaseTransferBytes()
	if err != nil {
		reply(w, nil, err)
		return
	}
	attempts, err := m.store.ForwardAttempts()
	if err != nil {
		reply(w, nil, err)
		return
	}
	inFlight, unknown := 0, 0
	for _, attempt := range attempts {
		if attempt.State == spool.Sending {
			inFlight++
		}
		if attempt.State == spool.Unknown {
			unknown++
		}
	}
	peerID, peerURL := "", ""
	if m.syncPeer != nil {
		peerID, peerURL = m.syncPeer.ID, m.syncPeer.URL
	}
	reply(w, map[string]interface{}{"state": state, "spool": status, "capabilities": map[string]string{"single_active_release": "nm-local-bluegreen-continuity-v2", "synchronous_peer": "fixed-peer-single-hop-v1"}, "delivery_profile_independent": true, "transfer_bytes": transferBytes, "transfer_limit_bytes": spool.MaxReleaseTransferBytes, "synchronous_forwarding": map[string]interface{}{"configured": m.syncPeer != nil, "peer_id": peerID, "peer_url": peerURL, "single_hop": true, "retry_after_not_sent_millis": 100, "in_flight": inFlight, "unknown": unknown, "attempts": attempts}}, nil)
}

func (m *Manager) Control(w http.ResponseWriter, r *http.Request) {
	var command spool.ReleaseCommand
	if !decode(w, r, 32<<10, &command) {
		return
	}
	state, err := m.store.ControlReleaseContext(r.Context(), command)
	reply(w, state, err)
}

func (m *Manager) Export(w http.ResponseWriter, r *http.Request) {
	var request struct {
		OperationID      string `json:"operation_id"`
		ExpectedRevision uint64 `json:"expected_revision"`
		Final            bool   `json:"final"`
	}
	if !decode(w, r, 32<<10, &request) {
		return
	}
	manifest, err := m.store.ExportReleaseContext(r.Context(), request.OperationID, request.ExpectedRevision, request.Final)
	reply(w, manifest, err)
}

// ReadExport recovers the exact persisted export after a lost response. It is
// intentionally protected by control authority, never the read-only probe.
func (m *Manager) ReadExport(w http.ResponseWriter, r *http.Request) {
	manifest, err := m.store.ReadReleaseExport(r.URL.Query().Get("operation_id"), r.URL.Query().Get("checksum"))
	reply(w, manifest, err)
}

func (m *Manager) Import(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Command  spool.ReleaseCommand  `json:"command"`
		Manifest spool.ReleaseManifest `json:"manifest"`
	}
	if !decode(w, r, maxTransferBytes, &request) {
		return
	}
	state, err := m.store.ImportReleaseContext(r.Context(), request.Manifest, request.Command)
	reply(w, state, err)
}

func (m *Manager) SynchronousAttempts(w http.ResponseWriter, r *http.Request) {
	rows, err := m.store.SynchronousAttempts()
	reply(w, map[string]interface{}{"attempts": rows, "automatic_retry": false}, err)
}

func (m *Manager) ResolveSynchronous(w http.ResponseWriter, r *http.Request) {
	var request struct {
		AttemptID string `json:"attempt_id"`
		Actor     string `json:"actor"`
		Evidence  string `json:"evidence"`
		Delivered *bool  `json:"delivered"`
	}
	if !decode(w, r, 32<<10, &request) {
		return
	}
	if request.Delivered == nil {
		http.Error(w, "explicit externally verified delivery decision required", http.StatusBadRequest)
		return
	}
	err := m.store.ResolveSynchronousAttemptContext(r.Context(), request.AttemptID, request.Actor, request.Evidence, *request.Delivered)
	reply(w, map[string]interface{}{"attempt_id": request.AttemptID, "reconciled": err == nil, "automatic_retry": false}, err)
}

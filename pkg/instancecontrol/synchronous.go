package instancecontrol

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
)

const syncClaimsHeader = "X-NM-Release-Synchronous"
const syncMACHeader = "X-NM-Release-Signature"
const syncNotSentHeader = "X-NM-Release-Not-Sent"
const maxSynchronousBody = 2 << 20

// SyncPeer is an explicit per-generation origin, never the stable Service.
// HTTP requires an explicit opt-in for an independently isolated network;
// signed claims authenticate the request but do not encrypt its contents.
type SyncPeer struct {
	ID        string
	URL       string
	AllowHTTP bool
}

type syncClaims struct {
	RequestID        string                 `json:"request_id"`
	Operation        string                 `json:"operation_id"`
	SourceID         string                 `json:"source_id"`
	TargetID         string                 `json:"target_id"`
	Path             string                 `json:"path"`
	BodyHash         string                 `json:"body_hash"`
	AuthHash         string                 `json:"auth_hash"`
	OriginalSource   string                 `json:"original_source"`
	Compatibility    bool                   `json:"compatibility"`
	Deadline         time.Time              `json:"deadline"`
	PredecessorProof *spool.RetirementProof `json:"predecessor_proof,omitempty"`
}

func (m *Manager) ConfigureSynchronousPeer(peer SyncPeer, client *http.Client) error {
	u, err := url.Parse(peer.URL)
	if err != nil || peer.ID == "" || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(peer.AllowHTTP && u.Scheme == "http")) {
		return errors.New("explicit synchronous peer identity and secure generation origin required")
	}
	st, err := m.store.ReleaseStatus()
	if err != nil {
		return err
	}
	if peer.ID == st.InstanceID {
		return errors.New("synchronous peer cannot be the local instance")
	}
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil // A fixed generation peer must not use an ambient proxy.
		client = &http.Client{Transport: transport}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	m.syncPeer, m.syncClient = &peer, &copyClient
	return nil
}

func hashSynchronous(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func (m *Manager) syncMAC(raw []byte) string {
	mac := hmac.New(sha256.New, m.syncKey)
	_, _ = mac.Write([]byte("nm-synchronous-forward-v1\x00"))
	_, _ = mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil))
}

// Synchronous preserves a single caller budget across forwarding and handoff.
// It only retries a routing decision that durably never entered the platform;
// transport errors, response loss and platform errors are returned once.
func (m *Manager) Synchronous(workerBudget time.Duration, compatibility bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(syncClaimsHeader) != "" || r.Header.Get(syncMACHeader) != "" {
			http.Error(w, "forwarded requests require the internal single-hop endpoint", http.StatusForbidden)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), workerBudget)
		defer cancel()
		raw, err := io.ReadAll(io.LimitReader(r.Body, maxSynchronousBody+1))
		_ = r.Body.Close()
		if err != nil || len(raw) > maxSynchronousBody {
			http.Error(w, "invalid or oversized synchronous request", 413)
			return
		}
		forwarded := false
		var nextPeerCheck time.Time
		for {
			if err = m.store.RefreshReleaseSafety(ctx); err != nil {
				syncTimeout(w, err)
				return
			}
			err = m.store.WithSynchronousIngress(ctx, "", "", false, func(permit context.Context) error {
				local := r.Clone(permit)
				local.Body = io.NopCloser(bytes.NewReader(raw))
				next(w, local)
				return nil
			})
			if err == nil {
				return
			}
			if ctx.Err() != nil {
				syncTimeout(w, ctx.Err())
				return
			}
			if !errors.Is(err, spool.ErrReleaseGate) {
				http.Error(w, "synchronous release state unavailable", 503)
				return
			}
			st, statusErr := m.store.ReleaseStatus()
			if statusErr != nil {
				http.Error(w, "synchronous release state unavailable", 503)
				return
			}
			if st.Phase == "frozen" {
				deadline, _ := ctx.Deadline()
				resumed, resumeErr := m.store.ResumeUnretiredSender(ctx, st.OperationID, st.PeerID, deadline)
				if resumeErr != nil {
					syncTimeout(w, resumeErr)
					return
				}
				if resumed {
					continue
				}
			}
			if st.OperationID == "" || m.syncPeer == nil || st.PeerID != m.syncPeer.ID {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "instance synchronous peer is not configured", 503)
				return
			}
			if (!forwarded || !time.Now().Before(nextPeerCheck)) && (st.Phase == "buffering" || st.Phase == "returning" || st.Phase == "buffer-closed" || st.Phase == "frozen" || st.Phase == "retired") {
				forwarded = true
				neverSent, err := m.forwardSynchronous(ctx, w, r, raw, compatibility, st)
				if err != nil {
					syncTimeout(w, err)
					return
				}
				if !neverSent {
					return
				}
				// Only an authenticated never-entered reply permits another
				// routing decision. Recheck slowly enough to bound ledger growth;
				// the same request can then observe a later retirement proof if
				// the coordinator disappears between freeze and activation.
				nextPeerCheck = time.Now().Add(100 * time.Millisecond)
			}
			select {
			case <-ctx.Done():
				syncTimeout(w, ctx.Err())
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
}

func syncTimeout(w http.ResponseWriter, err error) {
	// Match the legacy synchronous API's failure status, not accepted/queued.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"Status": 400, "Message": err.Error()})
}

func (m *Manager) forwardSynchronous(ctx context.Context, w http.ResponseWriter, original *http.Request, raw []byte, compatibility bool, st spool.ReleaseState) (bool, error) {
	deadline, _ := ctx.Deadline()
	attempt, err := m.store.BeginForwardAttempt(spool.ForwardAttempt{OperationID: st.OperationID, SourceID: st.InstanceID, PeerID: st.PeerID, Path: original.URL.Path, Deadline: deadline})
	if err != nil {
		return false, err
	}
	claims := syncClaims{RequestID: attempt.ID, Operation: st.OperationID, SourceID: st.InstanceID, TargetID: st.PeerID, Path: original.URL.Path, BodyHash: hashSynchronous(raw), AuthHash: hashSynchronous([]byte(original.Header.Get("Authorization"))), OriginalSource: original.RemoteAddr, Compatibility: compatibility, Deadline: deadline, PredecessorProof: st.RetirementProof}
	encoded, _ := json.Marshal(claims)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(m.syncPeer.URL, "/")+"/internal/instance/synchronous/forward", bytes.NewReader(raw))
	if err != nil {
		_ = m.store.FinishForwardAttempt(attempt, "not_sent", "invalid_peer_request")
		return false, err
	}
	req.GetBody = nil // A POST must never be silently retried by the transport.
	req.Header.Set("Content-Type", original.Header.Get("Content-Type"))
	req.Header.Set("Authorization", original.Header.Get("Authorization"))
	req.Header.Set(syncClaimsHeader, base64.RawURLEncoding.EncodeToString(encoded))
	req.Header.Set(syncMACHeader, m.syncMAC(encoded))
	response, err := m.syncClient.Do(req)
	if err != nil {
		_ = m.store.FinishForwardAttempt(attempt, spool.Unknown, "peer_transport_result_unknown")
		return false, errors.New("synchronous peer result unknown: " + err.Error())
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusConflict && hmac.Equal([]byte(response.Header.Get(syncNotSentHeader)), []byte(m.syncMAC([]byte("not-sent:"+attempt.ID)))) {
		_ = m.store.FinishForwardAttempt(attempt, "not_sent", "peer_send_fence_closed")
		var recovery struct {
			Proof *spool.RetirementProof `json:"retirement_proof"`
		}
		// The proof has its own exact operation/snapshot/fence MAC. No proof or
		// stale/consumed proof can promote an instance through this assist.
		if json.NewDecoder(io.LimitReader(response.Body, 8192)).Decode(&recovery) == nil && recovery.Proof != nil {
			current, statusErr := m.store.ReleaseStatus()
			if statusErr == nil && current.OperationID == claims.Operation && current.PeerID == claims.TargetID && current.ImportedChecksum == recovery.Proof.SnapshotChecksum {
				_, _ = m.store.ControlReleaseContext(ctx, spool.ReleaseCommand{OperationID: current.OperationID, ExpectedRevision: current.Revision, Action: "activate", PeerID: current.PeerID, Proof: recovery.Proof, Actor: "synchronous-handoff-assist", Evidence: "authenticated peer never executed this request; exact final snapshot and durable retirement proof"})
			}
		}
		return true, nil
	}
	// Bound and validate the complete response before announcing success.
	// An oversized/incomplete response is unknown, never a truncated 200.
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxSynchronousBody+1))
	if readErr != nil || len(responseBody) > maxSynchronousBody {
		_ = m.store.FinishForwardAttempt(attempt, spool.Unknown, "peer_response_incomplete_or_oversized")
		return false, errors.New("synchronous peer result unknown: incomplete or oversized response")
	}
	// Relay the actual API status/body. A platform 400/timeout is not retried.
	if v := response.Header.Get("Content-Type"); v != "" {
		w.Header().Set("Content-Type", v)
	}
	if v := response.Header.Get("Retry-After"); v != "" {
		w.Header().Set("Retry-After", v)
	}
	w.WriteHeader(response.StatusCode)
	_, copyErr := w.Write(responseBody)
	state, outcome := "completed", "peer_result_returned"
	if copyErr != nil {
		state, outcome = spool.Unknown, "peer_response_incomplete"
	}
	_ = m.store.FinishForwardAttempt(attempt, state, outcome)
	return false, nil
}

// ForwardSynchronous authenticates one fixed peer and never forwards again.
// formal is selected by the local router, never by arbitrary caller paths.
func (m *Manager) ForwardSynchronous(regular, compatibility map[string]http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		encoded, err := base64.RawURLEncoding.DecodeString(r.Header.Get(syncClaimsHeader))
		if err != nil || len(encoded) > 8192 || !hmac.Equal([]byte(r.Header.Get(syncMACHeader)), []byte(m.syncMAC(encoded))) {
			http.Error(w, "unauthorized synchronous peer", 401)
			return
		}
		var claims syncClaims
		if err = json.Unmarshal(encoded, &claims); err != nil {
			http.Error(w, "invalid synchronous claims", 400)
			return
		}
		st, err := m.store.ReleaseStatus()
		if err != nil || m.syncPeer == nil || claims.SourceID != m.syncPeer.ID || claims.SourceID != st.PeerID || claims.TargetID != st.InstanceID || claims.Operation != st.OperationID || claims.RequestID == "" || claims.Deadline.IsZero() || claims.Deadline.After(time.Now().Add(24*time.Hour)) || claims.AuthHash != hashSynchronous([]byte(r.Header.Get("Authorization"))) {
			http.Error(w, "synchronous peer binding mismatch", 403)
			return
		}
		next := regular[claims.Path]
		if claims.Compatibility {
			next = compatibility[claims.Path]
		}
		if next == nil {
			http.Error(w, "unsupported synchronous route", 403)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(r.Body, maxSynchronousBody+1))
		_ = r.Body.Close()
		if err != nil || len(raw) > maxSynchronousBody || hashSynchronous(raw) != claims.BodyHash {
			http.Error(w, "synchronous body binding mismatch", 400)
			return
		}
		ctx, cancel := context.WithDeadline(r.Context(), claims.Deadline)
		defer cancel()
		if err = m.store.RefreshReleaseSafety(ctx); err != nil {
			syncTimeout(w, err)
			return
		}
		if _, err = m.store.ResumeUnretiredSender(ctx, claims.Operation, claims.SourceID, claims.Deadline); err != nil {
			syncTimeout(w, err)
			return
		}
		if proof := claims.PredecessorProof; proof != nil {
			current, statusErr := m.store.ReleaseStatus()
			if statusErr == nil && current.OperationID == claims.Operation && current.PeerID == claims.SourceID && current.ImportedChecksum == proof.SnapshotChecksum {
				_, _ = m.store.ControlReleaseContext(ctx, spool.ReleaseCommand{OperationID: current.OperationID, ExpectedRevision: current.Revision, Action: "activate", PeerID: current.PeerID, Proof: proof, Actor: "synchronous-handoff-assist", Evidence: "authenticated predecessor supplied exact final snapshot durable retirement proof"})
			}
		}
		err = m.store.WithSynchronousIngress(ctx, claims.Operation, claims.SourceID, true, func(permit context.Context) error {
			attempt, err := m.store.ClaimForwardRequest(spool.ForwardAttempt{ID: claims.RequestID, OperationID: claims.Operation, SourceID: claims.SourceID, PeerID: claims.TargetID, Path: claims.Path, Deadline: claims.Deadline})
			if err != nil {
				return err
			}
			local := r.Clone(permit)
			local.URL.Path = claims.Path
			local.RemoteAddr = claims.OriginalSource
			local.Body = io.NopCloser(bytes.NewReader(raw))
			local.Header.Del(syncClaimsHeader)
			local.Header.Del(syncMACHeader)
			next(w, local)
			_ = m.store.FinishForwardAttempt(attempt, "completed", "peer_api_returned")
			return nil
		})
		if errors.Is(err, spool.ErrReleaseGate) {
			w.Header().Set(syncNotSentHeader, m.syncMAC([]byte("not-sent:"+claims.RequestID)))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(409)
			latest, _ := m.store.ReleaseStatus()
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"error": "peer_has_not_entered_platform", "retirement_proof": latest.RetirementProof})
		} else if err != nil {
			http.Error(w, "synchronous peer request unresolved", 409)
		}
	}
}

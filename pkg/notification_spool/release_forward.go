package notification_spool

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

type synchronousIngressKey struct{}

// HasSynchronousIngress is only true inside this store's held release read
// permit. HTTP headers and caller context values cannot manufacture it.
func HasSynchronousIngress(ctx context.Context, s *Store) bool {
	return s != nil && ctx.Value(synchronousIngressKey{}) == s
}

// WithSynchronousIngress fences the whole synchronous request, including
// rendering, against freeze/retirement. No permit is held while a successor
// waits for activation or forwards to the still authoritative sender.
func (s *Store) WithSynchronousIngress(ctx context.Context, operation, peer string, forwarded bool, handle func(context.Context) error) error {
	for !s.releaseMu.TryRLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer s.releaseMu.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := releaseRead(tx)
		if err != nil {
			return err
		}
		if forwarded && (operation == "" || st.OperationID != operation || st.PeerID != peer) {
			return ErrReleaseConflict
		}
		switch st.Phase {
		case "", "active", "releasing", "draining", "cancelling":
			return nil
		default:
			return ErrReleaseGate
		}
	}); err != nil {
		return err
	}
	return handle(context.WithValue(ctx, synchronousIngressKey{}, s))
}

// WithSynchronousContextPermit preserves the original profile boundary and
// durable platform attempt without recursively taking the release RWMutex.
func (s *Store) WithSynchronousContextPermit(ctx context.Context, profile *ProfileSnapshot, send func() error) error {
	if !HasSynchronousIngress(ctx, s) {
		return errors.New("synchronous ingress permit required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	id := ""
	if profile != nil {
		id = profile.ID
		mu := s.profileSendMutex(id)
		mu.RLock()
		defer mu.RUnlock()
		if id == "formal" && s.formalDisabled {
			return ErrProfilePaused
		}
		if err := s.db.View(func(tx *bolt.Tx) error { return checkProfileAdmission(tx, profile) }); err != nil {
			return err
		}
	}
	return s.synchronousAttempt(id, send)
}

func (s *Store) WithForwardedSynchronousPermit(ctx context.Context, operation, peer string, profile *ProfileSnapshot, send func() error) error {
	return s.WithSynchronousIngress(ctx, operation, peer, true, func(ctx context.Context) error {
		return s.WithSynchronousContextPermit(ctx, profile, send)
	})
}

// ForwardAttempt is a reconciliation reference, never a replayable queue. A
// transport failure remains unknown and must not trigger another platform call.
type ForwardAttempt struct {
	ID                string     `json:"id"`
	OperationID       string     `json:"operation_id"`
	SourceID          string     `json:"source_id"`
	PeerID            string     `json:"peer_id"`
	Path              string     `json:"path"`
	Deadline          time.Time  `json:"deadline"`
	State             string     `json:"state"`
	Outcome           string     `json:"outcome"`
	UpdatedAt         time.Time  `json:"updated_at"`
	SnapshotState     string     `json:"snapshot_state,omitempty"`
	SnapshotOutcome   string     `json:"snapshot_outcome,omitempty"`
	SnapshotUpdatedAt *time.Time `json:"snapshot_updated_at,omitempty"`
	ImportedFrom      string     `json:"imported_from,omitempty"`
	ImportedAt        *time.Time `json:"imported_at,omitempty"`
}

func forwardOwner(a ForwardAttempt) string {
	if strings.HasPrefix(a.ID, "received:") {
		return a.PeerID
	}
	return a.SourceID
}

// mergeForwardReference changes only the imported ledger observation, never the
// signed manifest. A snapshot cannot resurrect a request goroutine or overwrite
// the original local owner's live/completed reference with a foreign copy.
func mergeForwardReference(tx *bolt.Tx, key string, raw []byte, localInstance string) ([]byte, error) {
	var incoming ForwardAttempt
	if err := json.Unmarshal(raw, &incoming); err != nil {
		return nil, err
	}
	if incoming.ID == "" || key != "release-forward:"+incoming.ID || localInstance == "" {
		return nil, ErrReleaseConflict
	}
	selected := incoming
	if previous := tx.Bucket([]byte("meta")).Get([]byte(key)); previous != nil {
		var prior ForwardAttempt
		if err := json.Unmarshal(previous, &prior); err != nil {
			return nil, err
		}
		if prior.ID != incoming.ID || prior.OperationID != incoming.OperationID || prior.SourceID != incoming.SourceID || prior.PeerID != incoming.PeerID || prior.Path != incoming.Path || !prior.Deadline.Equal(incoming.Deadline) {
			return nil, ErrReleaseConflict
		}
		if forwardOwner(prior) == localInstance && prior.ImportedAt == nil {
			return append([]byte(nil), previous...), nil
		}
		if !incoming.UpdatedAt.After(prior.UpdatedAt) {
			selected = prior
		}
	}
	if selected.State == Sending {
		observed, importedAt := selected.UpdatedAt, time.Now().UTC()
		selected.SnapshotState, selected.SnapshotOutcome, selected.SnapshotUpdatedAt = selected.State, selected.Outcome, &observed
		selected.State, selected.Outcome = Unknown, "snapshot_forward_owner_external"
		selected.ImportedFrom, selected.ImportedAt = forwardOwner(selected), &importedAt
		if selected.ImportedFrom == "" {
			selected.Outcome = "snapshot_forward_owner_unknown"
		}
		if selected.ImportedFrom == localInstance {
			selected.Outcome = "snapshot_forward_local_reference_unobserved"
		}
	}
	return json.Marshal(selected)
}

func (s *Store) BeginForwardAttempt(a ForwardAttempt) (ForwardAttempt, error) {
	if a.OperationID == "" || a.SourceID == "" || a.PeerID == "" || a.Deadline.IsZero() {
		return a, ErrReleaseConflict
	}
	id, err := ID()
	if err != nil {
		return a, err
	}
	a.ID, a.State, a.Outcome, a.UpdatedAt = id, Sending, "peer_request_entered", time.Now().UTC()
	err = s.update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("meta")), "release-forward:"+id, a) })
	return a, err
}

func (s *Store) FinishForwardAttempt(a ForwardAttempt, state, outcome string) error {
	a.State, a.Outcome, a.UpdatedAt = state, outcome, time.Now().UTC()
	return s.update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("meta")), "release-forward:"+a.ID, a) })
}

// ClaimForwardRequest rejects replay even if the first HTTP response was lost.
// A duplicate is uncertain, not proof that a platform call never happened.
func (s *Store) ClaimForwardRequest(a ForwardAttempt) (ForwardAttempt, error) {
	if !validReleaseID(a.ID) || !validReleaseID(a.SourceID) {
		return a, ErrReleaseConflict
	}
	a.ID = "received:" + a.SourceID + ":" + a.ID
	a.State, a.Outcome, a.UpdatedAt = Sending, "peer_request_received", time.Now().UTC()
	err := s.update(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		key := "release-forward:" + a.ID
		if meta.Get([]byte(key)) != nil {
			return ErrReleaseConflict
		}
		return put(meta, key, a)
	})
	return a, err
}

func (s *Store) ForwardAttempts() ([]ForwardAttempt, error) {
	var rows []ForwardAttempt
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("meta")).ForEach(func(k, v []byte) error {
			if !strings.HasPrefix(string(k), "release-forward:") {
				return nil
			}
			var a ForwardAttempt
			if err := json.Unmarshal(v, &a); err != nil {
				return err
			}
			rows = append(rows, a)
			return nil
		})
	})
	return rows, err
}

func recoverForwardAttempts(tx *bolt.Tx) error {
	meta := tx.Bucket([]byte("meta"))
	var rows []ForwardAttempt
	if err := meta.ForEach(func(k, v []byte) error {
		if !strings.HasPrefix(string(k), "release-forward:") {
			return nil
		}
		var a ForwardAttempt
		if err := json.Unmarshal(v, &a); err != nil {
			return err
		}
		if a.State == Sending {
			a.State, a.Outcome, a.UpdatedAt = Unknown, "forwarder_interrupted", time.Now().UTC()
			rows = append(rows, a)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, a := range rows {
		if err := put(meta, "release-forward:"+a.ID, a); err != nil {
			return err
		}
	}
	return nil
}

func cleanupForwardAttempts(tx *bolt.Tx, retention time.Duration, batch int, now time.Time) error {
	meta := tx.Bucket([]byte("meta"))
	var keys [][]byte
	if err := meta.ForEach(func(k, v []byte) error {
		if len(keys) >= batch || !strings.HasPrefix(string(k), "release-forward:") {
			return nil
		}
		var a ForwardAttempt
		if err := json.Unmarshal(v, &a); err != nil {
			return err
		}
		// Never discard an unknown attempt or replay protection while its
		// signed original request can still be valid.
		if a.State != Sending && a.State != Unknown && a.Deadline.Before(now) && a.UpdatedAt.Add(retention).Before(now) {
			keys = append(keys, append([]byte(nil), k...))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, key := range keys {
		if err := meta.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

package notification_spool

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kubesphere/notification-manager/pkg/utils"
	bolt "go.etcd.io/bbolt"
	"strings"
	"time"
)

// SynchronousAttempt is an uncertainty fence, not a queue or retry policy.
// Original Notification/Verify callers still receive the original result.
type SynchronousAttempt struct {
	ID        string    `json:"id"`
	ProfileID string    `json:"profile_id,omitempty"`
	State     string    `json:"state"`
	Outcome   string    `json:"outcome"`
	UpdatedAt time.Time `json:"updated_at"`
	Evidence  string    `json:"evidence,omitempty"`
}

func (s *Store) synchronousAttempt(profile string, send func() error) error {
	st, err := s.ReleaseStatus()
	if err != nil {
		return err
	}
	if st.InstanceID == "" {
		return send()
	}
	id, err := ID()
	if err != nil {
		return err
	}
	attempt := SynchronousAttempt{ID: id, ProfileID: profile, State: Sending, Outcome: "platform_call_entered", UpdatedAt: time.Now().UTC()}
	if err = s.update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("meta")), "release-sync:"+id, attempt) }); err != nil {
		return err
	}
	sendErr := send()
	state, outcome := utils.DeliveryOutcome(sendErr)
	attempt.State = state
	attempt.Outcome = outcome
	attempt.UpdatedAt = time.Now().UTC()
	if err = s.update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("meta")), "release-sync:"+id, attempt) }); err != nil {
		return err
	}
	return sendErr
}
func syncUnresolved(tx *bolt.Tx) (int, error) {
	n := 0
	err := tx.Bucket([]byte("meta")).ForEach(func(k, v []byte) error {
		if !strings.HasPrefix(string(k), "release-sync:") {
			return nil
		}
		var a SynchronousAttempt
		if err := json.Unmarshal(v, &a); err != nil {
			return err
		}
		if a.State == Sending || a.State == Unknown {
			n++
		}
		return nil
	})
	return n, err
}
func (s *Store) SynchronousAttempts() ([]SynchronousAttempt, error) {
	var rows []SynchronousAttempt
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("meta")).ForEach(func(k, v []byte) error {
			if !strings.HasPrefix(string(k), "release-sync:") {
				return nil
			}
			var a SynchronousAttempt
			if err := json.Unmarshal(v, &a); err != nil {
				return err
			}
			rows = append(rows, a)
			return nil
		})
	})
	return rows, err
}
func (s *Store) ResolveSynchronousAttempt(id, actor, evidence string, delivered bool) error {
	return s.ResolveSynchronousAttemptContext(context.Background(), id, actor, evidence, delivered)
}
func (s *Store) ResolveSynchronousAttemptContext(ctx context.Context, id, actor, evidence string, delivered bool) error {
	if !validReleaseID(id) || actor == "" || evidence == "" || len(actor) > 128 || len(evidence) > 512 {
		return errors.New("bounded synchronous reconciliation evidence required")
	}
	s.releaseMu.Lock()
	defer s.releaseMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		meta := tx.Bucket([]byte("meta"))
		var a SynchronousAttempt
		if err := json.Unmarshal(meta.Get([]byte("release-sync:"+id)), &a); err != nil {
			return err
		}
		if a.State != Unknown && a.State != Sending {
			return errors.New("synchronous attempt is not uncertain")
		}
		a.State = "not_sent"
		if delivered {
			a.State = Delivered
		}
		a.Outcome = "operator_reconciled"
		a.Evidence = evidence
		a.UpdatedAt = time.Now().UTC()
		if err := put(meta, "release-sync:"+id, a); err != nil {
			return err
		}
		return releaseAudit(tx, ReleaseCommand{Action: "resolve_synchronous", Actor: actor, Evidence: evidence}, ReleaseState{})
	})
}

func cleanupSynchronousAttempts(tx *bolt.Tx, retention time.Duration, batch int, now time.Time) error {
	meta := tx.Bucket([]byte("meta"))
	var keys [][]byte
	if err := meta.ForEach(func(k, v []byte) error {
		if len(keys) >= batch || !strings.HasPrefix(string(k), "release-sync:") {
			return nil
		}
		var a SynchronousAttempt
		if err := json.Unmarshal(v, &a); err != nil {
			return err
		}
		if a.State != Sending && a.State != Unknown && a.UpdatedAt.Add(retention).Before(now) {
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
func recoverSynchronousAttempts(tx *bolt.Tx) error {
	meta := tx.Bucket([]byte("meta"))
	var rows []SynchronousAttempt
	if err := meta.ForEach(func(k, v []byte) error {
		if !strings.HasPrefix(string(k), "release-sync:") {
			return nil
		}
		var a SynchronousAttempt
		if err := json.Unmarshal(v, &a); err != nil {
			return err
		}
		if a.State == Sending {
			a.State = Unknown
			a.Outcome = "previous_owner_interrupted"
			a.UpdatedAt = time.Now().UTC()
			rows = append(rows, a)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, a := range rows {
		if err := put(meta, "release-sync:"+a.ID, a); err != nil {
			return err
		}
	}
	return nil
}

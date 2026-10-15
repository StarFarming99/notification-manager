package notification_spool

import (
	"context"
	"time"

	bolt "go.etcd.io/bbolt"
)

func unretiredFrozenSender(st ReleaseState) bool {
	return st.InstanceID != "" && st.OperationID != "" && st.Phase == "frozen" && !st.NeverActivated && st.RetirementProof == nil
}

func resumeFrozenSender(tx *bolt.Tx, st ReleaseState, now time.Time, reason string) error {
	st.Phase = "cancelling"
	st.ExportedChecksum, st.ImportedChecksum = "", ""
	st.Revision++
	st.DeadlineExceeded = st.DeadlineExceeded || (!st.Deadline.IsZero() && !now.Before(st.Deadline))
	if err := releasePut(tx, st); err != nil {
		return err
	}
	return releaseAudit(tx, ReleaseCommand{Action: "automatic_cancel_unretired", OperationID: st.OperationID, Actor: "release-continuity-safety", Evidence: reason}, st)
}

// recoverExpiredSender is transactional: expiry cancels an uncommitted frozen
// publication, never the old sender's original business authority. A retirement
// proof committed first prevents this path, including after process restart.
func recoverExpiredSender(tx *bolt.Tx, now time.Time) (bool, error) {
	st, err := releaseRead(tx)
	if err != nil {
		return false, err
	}
	if !unretiredFrozenSender(st) || st.Deadline.IsZero() || now.Before(st.Deadline) {
		return false, nil
	}
	if err := resumeFrozenSender(tx, st, now, "original publication deadline expired before retirement; original sender resumed, publication cancelled"); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) releaseSafetyLock(ctx context.Context) error {
	for !s.releaseMu.TryLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := ctx.Err(); err != nil {
		s.releaseMu.Unlock()
		return err
	}
	return nil
}

// RefreshReleaseSafety is a cheap no-op before expiry. It takes the same writer
// permit as retirement and rechecks the durable state after acquiring it.
func (s *Store) RefreshReleaseSafety(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	needed := false
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := releaseRead(tx)
		needed = unretiredFrozenSender(st) && !st.Deadline.IsZero() && !time.Now().UTC().Before(st.Deadline)
		return err
	}); err != nil {
		return err
	}
	if !needed {
		return nil
	}
	if err := s.releaseSafetyLock(ctx); err != nil {
		return err
	}
	defer s.releaseMu.Unlock()
	return s.update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := recoverExpiredSender(tx, time.Now().UTC())
		return err
	})
}

// ResumeUnretiredSender reserves the frozen operation's original synchronous
// remainder for the current real request. It is callable only after routing has
// proved no platform attempt entered; transport uncertainty never invokes it.
func (s *Store) ResumeUnretiredSender(ctx context.Context, operation, peer string, requestDeadline time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if operation == "" || peer == "" || requestDeadline.IsZero() {
		return false, ErrReleaseConflict
	}
	eligible := func(st ReleaseState, now time.Time) bool {
		return unretiredFrozenSender(st) && st.OperationID == operation && st.PeerID == peer && st.SyncRemainingMillis > 0 && !requestDeadline.After(now.Add(time.Duration(st.SyncRemainingMillis)*time.Millisecond))
	}
	needed := false
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := releaseRead(tx)
		needed = eligible(st, time.Now().UTC())
		return err
	}); err != nil {
		return false, err
	}
	if !needed {
		return false, nil
	}
	if err := s.releaseSafetyLock(ctx); err != nil {
		return false, err
	}
	defer s.releaseMu.Unlock()
	changed := false
	err := s.update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		st, err := releaseRead(tx)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if !eligible(st, now) {
			return nil
		}
		if err := resumeFrozenSender(tx, st, now, "synchronous original request reached reserved remainder before retirement; original sender resumed, publication cancelled"); err != nil {
			return err
		}
		changed = true
		return nil
	})
	return changed, err
}

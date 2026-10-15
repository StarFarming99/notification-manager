package notification_spool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestForwardRetentionPreservesUnknownAndUnexpiredReplayProtection(t *testing.T) {
	s, _ := releaseFixture(t, "forward", "active")
	now := time.Now().UTC()
	for _, state := range []string{"completed", Unknown, Sending, "unexpired"} {
		a, err := s.BeginForwardAttempt(ForwardAttempt{OperationID: "retention", SourceID: "forward", PeerID: "peer", Path: "/api/v2/verify", Deadline: now.Add(-time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		a.UpdatedAt = now.Add(-2 * time.Hour)
		a.State = state
		if state == "unexpired" {
			a.State = "completed"
			a.Deadline = now.Add(time.Minute)
		}
		if err = s.update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("meta")), "release-forward:"+a.ID, a) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.update(func(tx *bolt.Tx) error { return cleanupSynchronousAttempts(tx, time.Hour, 100, now) }); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ForwardAttempts()
	if err != nil || len(rows) != 3 {
		t.Fatal(rows, err)
	}
	if err = s.update(recoverForwardAttempts); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.ForwardAttempts()
	for _, a := range rows {
		if a.State == Sending {
			t.Fatal("restart left a forward marked live", a)
		}
	}
}

func TestForwardSnapshotMergePreservesOriginalOwnerAndIsolatesExternalSending(t *testing.T) {
	now := time.Now().UTC()
	base := ForwardAttempt{ID: "request", OperationID: "handoff", SourceID: "blue", PeerID: "green", Path: "/api/v2/verify", Deadline: now.Add(time.Minute), State: Sending, Outcome: "peer_request_entered", UpdatedAt: now}
	for _, tc := range []struct {
		name, local                       string
		prior                             *ForwardAttempt
		incoming                          ForwardAttempt
		wantState, wantOutcome, wantOwner string
		snapshot                          bool
	}{
		{name: "local-live-wins-over-newer-copy", local: "blue", prior: &base, incoming: func() ForwardAttempt { a := base; a.State = "completed"; a.UpdatedAt = now.Add(time.Second); return a }(), wantState: Sending, wantOutcome: base.Outcome},
		{name: "local-completion-wins-over-copy", local: "blue", prior: func() *ForwardAttempt {
			a := base
			a.State = "completed"
			a.Outcome = "peer_result_returned"
			return &a
		}(), incoming: func() ForwardAttempt { a := base; a.UpdatedAt = now.Add(time.Second); return a }(), wantState: "completed", wantOutcome: "peer_result_returned"},
		{name: "newer-foreign-completion-wins", local: "green", prior: func() *ForwardAttempt {
			a := base
			a.State = "completed"
			a.Outcome = "peer_result_returned"
			a.UpdatedAt = now.Add(time.Second)
			return &a
		}(), incoming: base, wantState: "completed", wantOutcome: "peer_result_returned"},
		{name: "foreign-live-is-observation", local: "green", incoming: base, wantState: Unknown, wantOutcome: "snapshot_forward_owner_external", wantOwner: "blue", snapshot: true},
		{name: "local-owner-without-original-is-not-live", local: "blue", incoming: base, wantState: Unknown, wantOutcome: "snapshot_forward_local_reference_unobserved", wantOwner: "blue", snapshot: true},
		{name: "received-owner-is-receiver", local: "green", prior: func() *ForwardAttempt { a := base; a.ID = "received:blue:request"; return &a }(), incoming: func() ForwardAttempt {
			a := base
			a.ID = "received:blue:request"
			a.State = "completed"
			a.UpdatedAt = now.Add(time.Second)
			return a
		}(), wantState: Sending, wantOutcome: base.Outcome},
		{name: "received-foreign-owner", local: "blue", incoming: func() ForwardAttempt { a := base; a.ID = "received:blue:request"; return a }(), wantState: Unknown, wantOutcome: "snapshot_forward_owner_external", wantOwner: "green", snapshot: true},
		{name: "missing-owner-remains-unknown", local: "green", incoming: func() ForwardAttempt { a := base; a.SourceID = ""; return a }(), wantState: Unknown, wantOutcome: "snapshot_forward_owner_unknown", snapshot: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := releaseFixture(t, tc.local, "active")
			key := "release-forward:" + tc.incoming.ID
			original, _ := json.Marshal(tc.incoming)
			before := append([]byte(nil), original...)
			var merged []byte
			if err := s.update(func(tx *bolt.Tx) error {
				if tc.prior != nil {
					if err := put(tx.Bucket([]byte("meta")), key, tc.prior); err != nil {
						return err
					}
				}
				var err error
				merged, err = mergeForwardReference(tx, key, original, tc.local)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			var got ForwardAttempt
			if err := json.Unmarshal(merged, &got); err != nil {
				t.Fatal(err)
			}
			if got.State != tc.wantState || got.Outcome != tc.wantOutcome || got.ImportedFrom != tc.wantOwner {
				t.Fatal(got)
			}
			if tc.snapshot && (got.SnapshotState != Sending || got.SnapshotOutcome != tc.incoming.Outcome || got.SnapshotUpdatedAt == nil || !got.SnapshotUpdatedAt.Equal(tc.incoming.UpdatedAt) || got.ImportedAt == nil) {
				t.Fatal("original observation lost", got)
			}
			if !bytes.Equal(original, before) {
				t.Fatal("signed source bytes changed")
			}
		})
	}
}

func TestForwardSnapshotMergeRejectsIdentityReplacement(t *testing.T) {
	s, _ := releaseFixture(t, "green", "active")
	a := ForwardAttempt{ID: "request", OperationID: "old-operation", SourceID: "blue", PeerID: "green", Path: "/api/v2/verify", Deadline: time.Now().Add(time.Minute), State: Unknown, UpdatedAt: time.Now().UTC()}
	key := "release-forward:" + a.ID
	if err := s.update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("meta")), key, a) }); err != nil {
		t.Fatal(err)
	}
	b := a
	b.OperationID = "replacement"
	raw, _ := json.Marshal(b)
	if err := s.update(func(tx *bolt.Tx) error { _, err := mergeForwardReference(tx, key, raw, "green"); return err }); !errors.Is(err, ErrReleaseConflict) {
		t.Fatal("identity replacement was accepted", err)
	}
}

func TestForwardIngressBudgetAndReplayDoNotGrantSendingRights(t *testing.T) {
	s, _ := releaseFixture(t, "forward", "standby")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.WithSynchronousIngress(ctx, "", "", false, func(context.Context) error { t.Fatal("cancelled request executed"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.WithSynchronousIngress(context.Background(), "", "", false, func(context.Context) error { t.Fatal("standby executed"); return nil }); !errors.Is(err, ErrReleaseGate) {
		t.Fatal(err)
	}
	a := ForwardAttempt{ID: "unique-request", OperationID: "handoff", SourceID: "peer", PeerID: "forward", Path: "/api/v2/notifications", Deadline: time.Now().Add(time.Minute)}
	if _, err := s.ClaimForwardRequest(a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimForwardRequest(a); !errors.Is(err, ErrReleaseConflict) {
		t.Fatal("replay was admitted", err)
	}
}

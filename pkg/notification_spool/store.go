// Package notification_spool owns original notifications independently of Jev.
package notification_spool

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

const Version = 1
const (
	Pending    = "pending"
	Sending    = "sending"
	Delivered  = "delivered"
	Retryable  = "retryable"
	Unknown    = "unknown"
	DeadLetter = "dead_letter"
)

var ErrCapacity = errors.New("original notification spool capacity unavailable")
var ErrFence = errors.New("notification claim no longer owns this attempt")
var ErrEmpty = errors.New("no notification ready for claim")

var buckets = [][]byte{[]byte("meta"), []byte("intakes"), []byte("targets"), []byte("ready"), []byte("dedupe"), []byte("audit"), []byte("blocked")}

type Target struct {
	ProfileID           string `json:"profile_id,omitempty"`
	ProfileVersion      string `json:"profile_version,omitempty"`
	CardOwnerID         string `json:"card_owner_id,omitempty"`
	OriginalDestination string `json:"original_destination,omitempty"`
	ID                  string `json:"id"`
	IntakeID            string `json:"intake_id"`
	Receiver            string `json:"receiver"`
	Channel             string `json:"channel"`
	Destination         string `json:"destination"`
	ContentRevision     string `json:"content_revision"`
	// Payload contains a frozen render and credential references, never resolved secrets.
	Payload     json.RawMessage `json:"payload"`
	State       string          `json:"state"`
	Attempts    int             `json:"attempts"`
	Owner       string          `json:"owner,omitempty"`
	AttemptID   string          `json:"attempt_id,omitempty"`
	AvailableAt time.Time       `json:"available_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	Outcome     string          `json:"outcome,omitempty"`
	Evidence    string          `json:"evidence,omitempty"`
	// History becomes eligible after one referenced original target succeeds.
	DependencyRevisions []string `json:"dependency_revisions,omitempty"`
}

type Intake struct {
	ID             string    `json:"id"`
	RequestHash    string    `json:"request_hash"`
	AcceptedAt     time.Time `json:"accepted_at"`
	TargetIDs      []string  `json:"target_ids"`
	TerminalReason string    `json:"terminal_reason,omitempty"`
}

type Limits struct {
	MaxTargets   int
	MaxBytes     int64
	ReserveBytes uint64
}
type Store struct {
	db                   *bolt.DB
	path                 string
	owner                string
	limits               Limits
	healthMu             sync.Mutex
	storageWriteFailures uint64
	lastStorageError     string
}

// OpenReadOnly never repairs records or changes ownership. The shared lock
// cannot be obtained while the NM writer owns the file; use /status when live.
func OpenReadOnly(path string) (*Store, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true, Timeout: 250 * time.Millisecond})
	if err != nil {
		return nil, err
	}
	err = db.View(func(tx *bolt.Tx) error {
		for _, b := range buckets {
			if tx.Bucket(b) == nil {
				return errors.New("incomplete spool schema")
			}
		}
		if string(tx.Bucket([]byte("meta")).Get([]byte("version"))) != "1" {
			return errors.New("unsupported spool version")
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, path: path}, nil
}

type Claim struct {
	Target    Target
	Owner     string
	AttemptID string
}

func ID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func Hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func put(b *bolt.Bucket, key string, value interface{}) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return b.Put([]byte(key), raw)
}
func queueKey(t Target) []byte {
	return []byte(fmt.Sprintf("%020d:%s", t.AvailableAt.UnixNano(), t.ID))
}

// Open obtains an exclusive file lock. Sending records from a previous owner
// become unknown before this owner is allowed to claim; they are never replayed.
func Open(path string, limits Limits) (*Store, error) {
	if path == "" {
		return nil, errors.New("notification spool path is required")
	}
	if limits.MaxTargets <= 0 {
		limits.MaxTargets = 100000
	}
	if limits.MaxBytes <= 0 {
		limits.MaxBytes = 8 << 30
	}
	if limits.ReserveBytes == 0 {
		limits.ReserveBytes = 64 << 20
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	if fi, err := os.Lstat(path); err == nil && (fi.Mode()&os.ModeSymlink != 0 || !fi.Mode().IsRegular()) {
		return nil, errors.New("spool must be a regular file")
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 250 * time.Millisecond})
	if err != nil {
		return nil, fmt.Errorf("open fenced notification spool: %w", err)
	}
	owner, err := ID()
	if err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db, path: path, owner: owner, limits: limits}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range buckets {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		meta := tx.Bucket([]byte("meta"))
		if v := meta.Get([]byte("version")); v != nil && string(v) != "1" {
			return errors.New("unsupported notification spool version")
		}
		if err := meta.Put([]byte("version"), []byte("1")); err != nil {
			return err
		}
		if err := meta.Put([]byte("owner"), []byte(owner)); err != nil {
			return err
		}
		b := tx.Bucket([]byte("targets"))
		var changes []Target
		var active uint64
		if err := b.ForEach(func(_, v []byte) error {
			var t Target
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			if activeState(t.State) {
				active++
			}
			if t.State == Sending {
				t.State = Unknown
				t.Outcome = "previous_owner_interrupted"
				t.UpdatedAt = time.Now().UTC()
				changes = append(changes, t)
			}
			return nil
		}); err != nil {
			return err
		}
		if err := setActiveCount(tx, active); err != nil {
			return err
		}
		for _, t := range changes {
			if err := put(b, t.ID, t); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) capacity() error {
	var fs unix.Statfs_t
	if err := unix.Statfs(filepath.Dir(s.path), &fs); err != nil {
		return err
	}
	if uint64(fs.Bavail)*uint64(fs.Bsize) < s.limits.ReserveBytes {
		return ErrCapacity
	}
	if fi, err := os.Stat(s.path); err != nil {
		return err
	} else if fi.Size() >= s.limits.MaxBytes {
		return ErrCapacity
	}
	return nil
}

// Submit atomically persists the entire frozen target plan and idempotency key.
// A caller without a stable upstream key gets a new intake: an AM repeat must
// not silently disappear through an unbounded content-hash deduplication window.
func (s *Store) Submit(key, requestHash string, targets []Target, terminalReason string) (Intake, bool, error) {
	return s.submit(key, requestHash, targets, terminalReason, nil, 0)
}

func (s *Store) SubmitProfile(key, requestHash string, targets []Target, terminalReason string, snapshot ProfileSnapshot, window time.Duration) (Intake, bool, error) {
	return s.submit(key, requestHash, targets, terminalReason, &snapshot, window)
}

func (s *Store) submit(key, requestHash string, targets []Target, terminalReason string, snapshot *ProfileSnapshot, window time.Duration) (Intake, bool, error) {
	var result Intake
	duplicate := false
	if requestHash == "" || (len(targets) == 0 && terminalReason == "") {
		return result, false, errors.New("request hash and a frozen plan or terminal reason are required")
	}
	if len(key) > 256 {
		return result, false, errors.New("idempotency key too long")
	}
	planBytes := 0
	primaryRevisions := make(map[string]bool)
	for _, target := range targets {
		planBytes += len(target.Payload) + 2048
		if len(target.DependencyRevisions) == 0 {
			primaryRevisions[target.ContentRevision] = true
		}
	}
	if planBytes > 8<<20 {
		return result, false, ErrCapacity
	}
	for _, target := range targets {
		for _, dependency := range target.DependencyRevisions {
			if !primaryRevisions[dependency] {
				return result, false, errors.New("history dependency must reference an original target in this intake")
			}
		}
	}
	id, err := ID()
	if err != nil {
		return result, false, err
	}
	err = s.update(func(tx *bolt.Tx) error {
		if err := checkProfileAdmission(tx, snapshot); err != nil {
			return err
		}
		intakes, dedupe := tx.Bucket([]byte("intakes")), tx.Bucket([]byte("dedupe"))
		if key != "" {
			if previous := dedupe.Get([]byte(key)); previous != nil {
				if err := json.Unmarshal(intakes.Get(previous), &result); err != nil {
					return err
				}
				if result.RequestHash != requestHash {
					return errors.New("intake idempotency key reused with different content")
				}
				// Identical AM intents cannot create another card while the
				// original plan is still pending, retrying or uncertain. A
				// completed plan may repeat only after its configured AM repeat
				// interval; prolonged ACK loss is not a fresh repeat.
				incomplete := false
				if window > 0 {
					for _, targetID := range result.TargetIDs {
						var previousTarget Target
						if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get([]byte(targetID)), &previousTarget); err != nil {
							return err
						}
						if previousTarget.State != Delivered {
							incomplete = true
							break
						}
					}
				}
				if window <= 0 || incomplete || time.Now().Before(result.AcceptedAt.Add(window)) {
					duplicate = true
					return nil
				}
			}
		}
		if err := s.capacity(); err != nil {
			return err
		}
		b := tx.Bucket([]byte("targets"))
		if activeCount(tx)+uint64(len(targets)) > uint64(s.limits.MaxTargets) {
			return ErrCapacity
		}
		now := time.Now().UTC()
		result = Intake{ID: id, RequestHash: requestHash, AcceptedAt: now, TerminalReason: terminalReason}
		seen := make(map[string]bool)
		for _, t := range targets {
			if err := validateProfileTarget(tx, t); err != nil {
				return err
			}
			if snapshot != nil && (t.ProfileID != snapshot.ID || t.ProfileVersion != snapshot.Version) {
				return errors.New("frozen target does not match intake profile")
			}
			if t.Receiver == "" || t.Channel == "" || t.Destination == "" || t.ContentRevision == "" || !json.Valid(t.Payload) || len(t.Payload) > 2<<20 {
				return errors.New("invalid frozen target")
			}
			t.ID = Hash([]byte(id + "\x00" + t.Receiver + "\x00" + t.Channel + "\x00" + t.Destination + "\x00" + t.ContentRevision))
			if seen[t.ID] {
				return errors.New("duplicate frozen target")
			}
			seen[t.ID] = true
			t.IntakeID = id
			t.State = Pending
			t.Attempts = 0
			t.Owner = ""
			t.AttemptID = ""
			t.Outcome = ""
			t.Evidence = ""
			t.AvailableAt = now
			t.UpdatedAt = now
			if err := put(b, t.ID, t); err != nil {
				return err
			}
			index, indexKey := tx.Bucket([]byte("ready")), queueKey(t)
			if len(t.DependencyRevisions) > 0 {
				index = tx.Bucket([]byte("blocked"))
				indexKey = []byte(t.ID)
			}
			if err := index.Put(indexKey, []byte(t.ID)); err != nil {
				return err
			}
			result.TargetIDs = append(result.TargetIDs, t.ID)
		}
		if err := changeActiveCount(tx, len(targets)); err != nil {
			return err
		}
		if err := put(intakes, id, result); err != nil {
			return err
		}
		if key != "" {
			return dedupe.Put([]byte(key), []byte(id))
		}
		return nil
	})
	return result, duplicate, err
}

func (s *Store) Claim(now time.Time) (Claim, error) {
	var claim Claim
	err := s.update(func(tx *bolt.Tx) error {
		q := tx.Bucket([]byte("ready"))
		cursor := q.Cursor()
		var k []byte
		var t Target
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get(value), &t); err != nil {
				return err
			}
			if t.AvailableAt.After(now) {
				break
			}
			paused, err := profileDeliveryPaused(tx, t)
			if err != nil {
				return err
			}
			if paused {
				continue
			}
			k = key
			break
		}
		if k == nil {
			return ErrEmpty
		}
		if t.State != Pending && t.State != Retryable {
			return errors.New("invalid ready index state")
		}
		attempt, err := ID()
		if err != nil {
			return err
		}
		t.State = Sending
		t.Owner = s.owner
		t.AttemptID = attempt
		t.Attempts++
		t.UpdatedAt = now.UTC()
		if err := put(tx.Bucket([]byte("targets")), t.ID, t); err != nil {
			return err
		}
		if err := q.Delete(k); err != nil {
			return err
		}
		claim = Claim{Target: t, Owner: s.owner, AttemptID: attempt}
		return nil
	})
	return claim, err
}

// ClaimForRecovery cannot claim a delivered, unknown, blocked or unrelated
// target. Resolving unknown requires a separate audited operator decision.
func (s *Store) ClaimForRecovery(id, actor, evidence string, now time.Time) (Claim, error) {
	var claim Claim
	if actor == "" || evidence == "" || len(actor) > 128 || len(evidence) > 512 {
		return claim, errors.New("bounded recovery actor and evidence required")
	}
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("targets"))
		var t Target
		if err := json.Unmarshal(b.Get([]byte(id)), &t); err != nil {
			return err
		}
		if paused, err := profileDeliveryPaused(tx, t); err != nil || paused {
			return errors.New("profile is paused or invalid; recovery send forbidden")
		}
		if (t.State != Pending && t.State != Retryable) || t.AvailableAt.After(now) || tx.Bucket([]byte("ready")).Get(queueKey(t)) == nil {
			return errors.New("target not eligible for recovery send")
		}
		attempt, err := ID()
		if err != nil {
			return err
		}
		if err := put(tx.Bucket([]byte("audit")), attempt, map[string]interface{}{"action": "replay_exact_target", "target_id": id, "actor": actor, "evidence": evidence, "at": now.UTC()}); err != nil {
			return err
		}
		if err := tx.Bucket([]byte("ready")).Delete(queueKey(t)); err != nil {
			return err
		}
		t.State = Sending
		t.Owner = s.owner
		t.AttemptID = attempt
		t.Attempts++
		t.UpdatedAt = now.UTC()
		if err := put(b, id, t); err != nil {
			return err
		}
		claim = Claim{Target: t, Owner: s.owner, AttemptID: attempt}
		return nil
	})
	return claim, err
}

// Finish accepts only a matching persisted attempt. Unknown is the default for
// errors after a send may have reached the platform; it requires reconciliation.
func (s *Store) Finish(c Claim, state, outcome, evidence string, next time.Time) error {
	if state != Delivered && state != Retryable && state != Unknown && state != DeadLetter {
		return errors.New("illegal notification completion state")
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("targets"))
		var t Target
		if err := json.Unmarshal(b.Get([]byte(c.Target.ID)), &t); err != nil {
			return err
		}
		if t.State != Sending || t.Owner != s.owner || c.Owner != s.owner || t.AttemptID != c.AttemptID {
			return ErrFence
		}
		if !activeState(state) {
			if err := changeActiveCount(tx, -1); err != nil {
				return err
			}
		}
		t.State = state
		t.Outcome = outcome
		t.Evidence = evidence
		t.UpdatedAt = time.Now().UTC()
		if state == Retryable {
			t.AvailableAt = next.UTC()
			if err := tx.Bucket([]byte("ready")).Put(queueKey(t), []byte(t.ID)); err != nil {
				return err
			}
		}
		if err := put(b, t.ID, t); err != nil {
			return err
		}
		if state == Delivered {
			return releaseDependents(tx, t)
		}
		return nil
	})
}

func (s *Store) Snapshot() ([]Intake, []Target, error) {
	var intakes []Intake
	var targets []Target
	err := s.db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket([]byte("intakes")).ForEach(func(_, v []byte) error {
			var i Intake
			if err := json.Unmarshal(v, &i); err != nil {
				return err
			}
			intakes = append(intakes, i)
			return nil
		}); err != nil {
			return err
		}
		return tx.Bucket([]byte("targets")).ForEach(func(_, v []byte) error {
			var t Target
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			targets = append(targets, t)
			return nil
		})
	})
	return intakes, targets, err
}

// Resolve is an offline, fenced, target-only operation. Evidence is a reference
// to an external receipt or operator decision, never a token or raw response.
func (s *Store) Resolve(id, state, actor, evidence string) error {
	if actor == "" || evidence == "" || len(actor) > 128 || len(evidence) > 512 {
		return errors.New("actor and bounded reconciliation evidence are required")
	}
	if state != Delivered && state != Retryable && state != DeadLetter {
		return errors.New("resolve must choose delivered, retryable or dead_letter")
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("targets"))
		var t Target
		if err := json.Unmarshal(b.Get([]byte(id)), &t); err != nil {
			return err
		}
		if t.State != Unknown && t.State != DeadLetter {
			return errors.New("only unknown or dead-letter targets can be reconciled")
		}
		auditID, err := ID()
		if err != nil {
			return err
		}
		if err := put(tx.Bucket([]byte("audit")), auditID, map[string]interface{}{"target_id": id, "from": t.State, "to": state, "actor": actor, "evidence": evidence, "at": time.Now().UTC()}); err != nil {
			return err
		}
		if activeState(t.State) && !activeState(state) {
			if err := changeActiveCount(tx, -1); err != nil {
				return err
			}
		} else if !activeState(t.State) && activeState(state) {
			if err := changeActiveCount(tx, 1); err != nil {
				return err
			}
			if activeCount(tx) > uint64(s.limits.MaxTargets) {
				return ErrCapacity
			}
		}
		t.State = state
		t.Outcome = "operator_reconciled"
		t.Evidence = evidence
		t.UpdatedAt = time.Now().UTC()
		if state == Retryable {
			t.AvailableAt = time.Now().UTC()
			if err := tx.Bucket([]byte("ready")).Put(queueKey(t), []byte(id)); err != nil {
				return err
			}
		}
		if err := put(b, id, t); err != nil {
			return err
		}
		if state == Delivered {
			return releaseDependents(tx, t)
		}
		return nil
	})
}

func (s *Store) Status() (map[string]interface{}, error) {
	counts := map[string]int{Pending: 0, Sending: 0, Delivered: 0, Retryable: 0, Unknown: 0, DeadLetter: 0}
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("targets")).ForEach(func(_, v []byte) error {
			var t Target
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			counts[t.State]++
			return nil
		})
	})
	var active uint64
	if err == nil {
		err = s.db.View(func(tx *bolt.Tx) error { active = activeCount(tx); return nil })
	}
	s.healthMu.Lock()
	writeFailures, lastStorageError := s.storageWriteFailures, s.lastStorageError
	s.healthMu.Unlock()
	var storedBytes int64
	if fi, statErr := os.Stat(s.path); statErr == nil {
		storedBytes = fi.Size()
	}
	return map[string]interface{}{"version": Version, "owner": s.owner, "storage_write_failures": writeFailures, "last_storage_error": lastStorageError, "counts": counts, "active_targets": active, "active_target_limit": s.limits.MaxTargets, "stored_bytes": storedBytes, "storage_byte_limit": s.limits.MaxBytes, "completed_payload_retention": "48h", "completed_cleanup_batch": 200}, err
}
func (s *Store) Check() error {
	return s.db.View(func(tx *bolt.Tx) error {
		var failures []error
		for err := range tx.Check() {
			if err != nil {
				failures = append(failures, err)
			}
		}
		return errors.Join(failures...)
	})
}
func (s *Store) Ready() error {
	s.healthMu.Lock()
	failed := s.lastStorageError != ""
	s.healthMu.Unlock()
	if failed {
		return errors.New("notification storage write unavailable")
	}
	if err := s.capacity(); err != nil {
		return err
	}
	return s.db.View(func(tx *bolt.Tx) error {
		if activeCount(tx) >= uint64(s.limits.MaxTargets) {
			return ErrCapacity
		}
		return nil
	})
}
func (s *Store) Close() error { return s.db.Close() }

func releaseDependents(tx *bolt.Tx, parent Target) error {
	blocked, targets := tx.Bucket([]byte("blocked")), tx.Bucket([]byte("targets"))
	var release []Target
	if err := blocked.ForEach(func(_, id []byte) error {
		var t Target
		if err := json.Unmarshal(targets.Get(id), &t); err != nil {
			return err
		}
		if t.IntakeID != parent.IntakeID {
			return nil
		}
		for _, revision := range t.DependencyRevisions {
			if revision == parent.ContentRevision {
				release = append(release, t)
				break
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for _, t := range release {
		t.AvailableAt = time.Now().UTC()
		if err := put(targets, t.ID, t); err != nil {
			return err
		}
		if err := tx.Bucket([]byte("ready")).Put(queueKey(t), []byte(t.ID)); err != nil {
			return err
		}
		if err := blocked.Delete([]byte(t.ID)); err != nil {
			return err
		}
	}
	return nil
}

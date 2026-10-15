package notification_spool

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const ReleaseProtocolVersion = "nm-local-bluegreen-v1"

var ErrReleaseConflict = errors.New("instance release revision or identity conflict")
var ErrReleaseGate = errors.New("instance release gate is closed")
var ErrLegacyAMMetadata = errors.New("legacy AM ledger lacks generation metadata required for bluegreen handoff")
var ErrAmbiguousGeneration = errors.New("buffered AM generation is ambiguous; retry after controlled handoff")
var ErrReleaseDeadline = errors.New("instance release deadline exceeded")

// Instance state fences the entire binary independently from delivery profiles.
type ReleaseState struct {
	RejectedAmbiguousAdmissionsTotal uint64           `json:"rejected_ambiguous_admissions_total"`
	ProtocolVersion                  string           `json:"protocol_version"`
	InstanceID                       string           `json:"instance_id"`
	Revision                         uint64           `json:"revision"`
	Phase                            string           `json:"phase"`
	OperationID                      string           `json:"operation_id,omitempty"`
	PeerID                           string           `json:"peer_id,omitempty"`
	Deadline                         time.Time        `json:"deadline,omitempty"`
	MaxSendDelayMillis               int64            `json:"max_send_delay_millis,omitempty"`
	SyncRemainingMillis              int64            `json:"sync_remaining_millis,omitempty"`
	StartedAt                        time.Time        `json:"started_at,omitempty"`
	ActivatedAt                      time.Time        `json:"activated_at,omitempty"`
	ImportedChecksum                 string           `json:"imported_checksum,omitempty"`
	ExportedChecksum                 string           `json:"exported_checksum,omitempty"`
	RetirementProof                  *RetirementProof `json:"retirement_proof,omitempty"`
	NeverActivated                   bool             `json:"never_activated"`
	DeadlineExceeded                 bool             `json:"deadline_exceeded,omitempty"`
	KeyFingerprint                   string           `json:"-"`
	SynchronousUnresolved            int              `json:"synchronous_unresolved"`
	SynchronousInFlight              int              `json:"synchronous_in_flight"`
	AmbiguousAdmissions              uint64           `json:"ambiguous_admissions"`
}
type ReleaseCommand struct {
	OperationID         string           `json:"operation_id"`
	ExpectedRevision    uint64           `json:"expected_revision"`
	Action              string           `json:"action"`
	PeerID              string           `json:"peer_id"`
	Deadline            time.Time        `json:"deadline"`
	MaxSendDelayMillis  int64            `json:"max_send_delay_millis"`
	SyncRemainingMillis int64            `json:"sync_remaining_millis"`
	Actor               string           `json:"actor"`
	Evidence            string           `json:"evidence"`
	Proof               *RetirementProof `json:"retirement_proof,omitempty"`
}
type RetirementProof struct {
	ProtocolVersion  string    `json:"protocol_version"`
	OperationID      string    `json:"operation_id"`
	InstanceID       string    `json:"instance_id"`
	SuccessorID      string    `json:"successor_id"`
	SnapshotChecksum string    `json:"snapshot_checksum"`
	FenceRevision    uint64    `json:"fence_revision"`
	Deadline         time.Time `json:"deadline"`
	MAC              string    `json:"mac"`
}
type ReleaseManifest struct {
	ProtocolVersion  string                       `json:"protocol_version"`
	OperationID      string                       `json:"operation_id"`
	SourceID         string                       `json:"source_id"`
	DestinationID    string                       `json:"destination_id"`
	Deadline         time.Time                    `json:"deadline"`
	Final            bool                         `json:"final"`
	BufferedOnly     bool                         `json:"buffered_only"`
	ExportedAt       time.Time                    `json:"exported_at"`
	Buckets          map[string]map[string][]byte `json:"buckets"`
	SnapshotChecksum string                       `json:"snapshot_checksum"`
	MAC              string                       `json:"mac"`
}

func releaseRead(tx *bolt.Tx) (ReleaseState, error) {
	var st ReleaseState
	raw := tx.Bucket([]byte("meta")).Get([]byte("instance-release"))
	if raw == nil {
		return st, nil
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return st, err
	}
	st.KeyFingerprint = string(tx.Bucket([]byte("meta")).Get([]byte("instance-release-key")))
	if st.ProtocolVersion != ReleaseProtocolVersion {
		return st, errors.New("unsupported instance release protocol")
	}
	return st, nil
}
func releasePut(tx *bolt.Tx, st ReleaseState) error {
	return put(tx.Bucket([]byte("meta")), "instance-release", st)
}
func (s *Store) ReleaseStatus() (ReleaseState, error) {
	var st ReleaseState
	err := s.db.View(func(tx *bolt.Tx) error {
		var e error
		st, e = releaseRead(tx)
		if e == nil {
			st.SynchronousUnresolved, e = syncUnresolved(tx)
			if e == nil {
				st.SynchronousInFlight, e = syncSending(tx)
			}
			if raw := tx.Bucket([]byte("meta")).Get([]byte("instance-release-ambiguous-admissions")); raw != nil && e == nil {
				e = json.Unmarshal(raw, &st.RejectedAmbiguousAdmissionsTotal)
			}
		}
		if raw := tx.Bucket([]byte("meta")).Get([]byte("release-ambiguity:" + st.OperationID)); raw != nil && e == nil {
			e = json.Unmarshal(raw, &st.AmbiguousAdmissions)
		}
		if e == nil && st.OperationID != "" && st.Phase != "active" && st.Phase != "retired" {
			limit := st.Deadline
			if st.Phase == "buffering" || st.Phase == "returning" || st.Phase == "buffer-closed" {
				limit = bufferDeadline(st)
			}
			st.DeadlineExceeded = st.DeadlineExceeded || (!limit.IsZero() && time.Now().UTC().After(limit))
		}
		return e
	})
	return st, err
}
func validReleaseID(v string) bool {
	if v == "" || len(v) > 128 {
		return false
	}
	for _, c := range v {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("-_.:", c)) {
			return false
		}
	}
	return true
}
func (s *Store) ConfigureRelease(id, mode string, key []byte) error {
	if !validReleaseID(id) || (mode != "active" && mode != "standby") || len(key) < 32 {
		return errors.New("bounded instance identity, active/standby mode and protocol key required")
	}
	fingerprint := Hash(key)
	err := s.update(func(tx *bolt.Tx) error {
		st, err := releaseRead(tx)
		if err != nil {
			return err
		}
		meta := tx.Bucket([]byte("meta"))
		if st.InstanceID != "" {
			if st.InstanceID != id || st.KeyFingerprint != fingerprint {
				return ErrReleaseConflict
			}
			return nil
		}
		st = ReleaseState{ProtocolVersion: ReleaseProtocolVersion, InstanceID: id, Revision: 1, Phase: mode, NeverActivated: mode == "standby"}
		if err := meta.Put([]byte("instance-release-key"), []byte(fingerprint)); err != nil {
			return err
		}
		if err := markLegacyAMIntakes(tx); err != nil {
			return err
		}
		return releasePut(tx, st)
	})
	if err == nil {
		s.releaseKey = append([]byte(nil), key...)
	}
	return err
}
func releaseAdmission(tx *bolt.Tx, synchronous bool, now time.Time) error {
	st, err := releaseRead(tx)
	if err != nil {
		return err
	}
	if st.InstanceID == "" {
		return nil
	}
	switch st.Phase {
	case "active", "releasing", "cancelling":
		return nil
	case "standby", "buffering", "returning":
		if synchronous {
			return ErrReleaseGate
		}
		return nil
	default:
		return ErrReleaseGate
	}
}
func releaseSending(tx *bolt.Tx, now time.Time) error {
	st, err := releaseRead(tx)
	if err != nil {
		return err
	}
	if st.InstanceID == "" {
		return nil
	}
	switch st.Phase {
	case "active":
		return nil
	case "releasing", "draining", "cancelling":
		return nil
	default:
		return ErrReleaseGate
	}
}
func (s *Store) AdmissionAllowed(synchronous bool) error {
	return s.db.View(func(tx *bolt.Tx) error { return releaseAdmission(tx, synchronous, time.Now().UTC()) })
}
func (s *Store) WithInstancePermit(synchronous bool, send func() error) error {
	s.releaseMu.RLock()
	defer s.releaseMu.RUnlock()
	if err := s.db.View(func(tx *bolt.Tx) error {
		if err := releaseAdmission(tx, synchronous, time.Now().UTC()); err != nil {
			return err
		}
		return releaseSending(tx, time.Now().UTC())
	}); err != nil {
		return err
	}
	return s.synchronousAttempt("", send)
}
func (s *Store) releaseMAC(v interface{}) string {
	raw, _ := json.Marshal(v)
	m := hmac.New(sha256.New, s.releaseKey)
	m.Write(raw)
	return hex.EncodeToString(m.Sum(nil))
}
func (s *Store) proofValid(p RetirementProof) bool {
	mac := p.MAC
	p.MAC = ""
	return len(s.releaseKey) >= 32 && hmac.Equal([]byte(mac), []byte(s.releaseMAC(p)))
}
func releaseAudit(tx *bolt.Tx, cmd ReleaseCommand, st ReleaseState) error {
	id, err := ID()
	if err != nil {
		return err
	}
	return put(tx.Bucket([]byte("audit")), id, map[string]interface{}{"action": "instance_" + cmd.Action, "operation_id": st.OperationID, "instance_id": st.InstanceID, "revision": st.Revision, "actor": cmd.Actor, "evidence": cmd.Evidence, "at": time.Now().UTC()})
}
func validateCommand(st ReleaseState, c ReleaseCommand) error {
	if !validReleaseID(c.OperationID) || c.Actor == "" || c.Evidence == "" || len(c.Actor) > 128 || len(c.Evidence) > 512 || st.InstanceID == "" {
		return errors.New("release operation and bounded audit evidence required")
	}
	if st.Revision != c.ExpectedRevision {
		return ErrReleaseConflict
	}
	if st.OperationID != "" && st.OperationID != c.OperationID && st.Phase != "active" {
		return ErrReleaseConflict
	}
	if st.OperationID == c.OperationID {
		if c.PeerID != "" && c.PeerID != st.PeerID {
			return ErrReleaseConflict
		}
		if !c.Deadline.IsZero() && !c.Deadline.Equal(st.Deadline) {
			return ErrReleaseConflict
		}
		if c.MaxSendDelayMillis != 0 && c.MaxSendDelayMillis != st.MaxSendDelayMillis {
			return ErrReleaseConflict
		}
		if c.SyncRemainingMillis != 0 && c.SyncRemainingMillis != st.SyncRemainingMillis {
			return ErrReleaseConflict
		}
	}
	return nil
}
func (s *Store) ControlRelease(c ReleaseCommand) (ReleaseState, error) {
	return s.ControlReleaseContext(context.Background(), c)
}
func (s *Store) ControlReleaseContext(ctx context.Context, c ReleaseCommand) (ReleaseState, error) {
	// Physical send fences need the process-wide send boundary. Metadata
	// changes retain the same sender and
	// are serialized with retirement by the Bolt transaction and revision CAS.
	// Waiting behind a slow platform call here would stall unrelated requests
	// merely because publication has begun.
	switch c.Action {
	case "freeze":
		// A pending RWMutex writer blocks subsequent readers. Do not let a
		// publication waiting for a slow platform call stop other legitimate
		// synchronous calls while this instance is still draining. If no
		// physical quiet point fits the control budget, publication fails.
		if err := s.releaseSafetyLock(ctx); err != nil {
			return ReleaseState{}, err
		}
		defer s.releaseMu.Unlock()
	case "retire", "rollback-standby":
		s.releaseMu.Lock()
		defer s.releaseMu.Unlock()
	}
	// Activation can only commit from a non-sending phase. Its transactional
	// checksum/proof/CAS checks establish authority; a local send lock would
	// only stall already-active requests on a redundant proof-assist call.
	var result ReleaseState
	if err := ctx.Err(); err != nil {
		return result, err
	}
	err := s.update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		receiptKey := "release-command:" + s.releaseMAC(c)
		if raw := tx.Bucket([]byte("meta")).Get([]byte(receiptKey)); raw != nil {
			return json.Unmarshal(raw, &result)
		}
		st, err := releaseRead(tx)
		if err != nil {
			return err
		}
		if err = validateCommand(st, c); err != nil {
			return err
		}
		now := time.Now().UTC()
		if len(s.releaseKey) < 32 {
			return ErrReleaseGate
		}
		switch c.Action {
		case "begin-active", "begin-standby":
			want := "active"
			phase := "releasing"
			if c.Action == "begin-standby" {
				want = "standby"
				phase = "buffering"
			}
			if st.OperationID == c.OperationID || tx.Bucket([]byte("meta")).Get([]byte("release-operation:"+c.OperationID)) != nil {
				return ErrReleaseConflict
			}
			if st.Phase != want || !validReleaseID(c.PeerID) || c.PeerID == st.InstanceID || !c.Deadline.After(now) || c.MaxSendDelayMillis <= 0 || c.SyncRemainingMillis <= 0 || c.MaxSendDelayMillis > (24*time.Hour).Milliseconds() || c.SyncRemainingMillis > (24*time.Hour).Milliseconds() || c.Deadline.Sub(now) > 24*time.Hour {
				return ErrReleaseConflict
			}
			if c.Action == "begin-active" {
				if size, err := estimateReleaseTransferBytes(tx); err != nil {
					return err
				} else if size > MaxReleaseTransferBytes {
					return errors.New("release transfer exceeds supported bounded protocol size")
				}
			}
			st.OperationID = c.OperationID
			st.PeerID = c.PeerID
			st.Deadline = c.Deadline.UTC()
			st.MaxSendDelayMillis = c.MaxSendDelayMillis
			st.SyncRemainingMillis = c.SyncRemainingMillis
			st.StartedAt = now
			st.Phase = phase
			st.ImportedChecksum = ""
			st.ExportedChecksum = ""
			st.RetirementProof = nil
			st.DeadlineExceeded = false
			if err := put(tx.Bucket([]byte("meta")), "release-operation:"+c.OperationID, map[string]interface{}{"peer_id": st.PeerID, "deadline": st.Deadline, "max_send_delay_millis": st.MaxSendDelayMillis, "sync_remaining_millis": st.SyncRemainingMillis}); err != nil {
				return err
			}
		case "close-admission":
			if st.OperationID != c.OperationID {
				return ErrReleaseConflict
			}
			if st.Phase == "releasing" || st.Phase == "active" || st.Phase == "cancelling" {
				st.Phase = "draining"
			} else if st.Phase == "buffering" || st.Phase == "standby" {
				st.Phase = "buffer-closed"
			} else if st.Phase != "draining" && st.Phase != "buffer-closed" && st.Phase != "frozen" {
				return ErrReleaseConflict
			}
		case "freeze":
			if st.OperationID != c.OperationID || (st.Phase != "draining" && st.Phase != "frozen") {
				return ErrReleaseConflict
			}
			st.Phase = "frozen"
		case "retire":
			if st.OperationID != c.OperationID || (st.Phase != "frozen" && st.Phase != "buffer-closed") || st.ExportedChecksum == "" {
				return ErrReleaseConflict
			}
			if err := releaseSafe(tx, st.NeverActivated); err != nil {
				return err
			}
			st.Phase = "retired"
			p := RetirementProof{ProtocolVersion: ReleaseProtocolVersion, OperationID: st.OperationID, InstanceID: st.InstanceID, SuccessorID: st.PeerID, SnapshotChecksum: st.ExportedChecksum, FenceRevision: st.Revision + 1, Deadline: st.Deadline}
			p.MAC = s.releaseMAC(p)
			st.RetirementProof = &p
		case "activate":
			if err := releaseAmbiguity(tx, st.OperationID); err != nil {
				return err
			}
			if st.OperationID != c.OperationID || (st.Phase != "buffering" && st.Phase != "returning" && st.Phase != "buffer-closed") || st.ImportedChecksum == "" || c.Proof == nil {
				return ErrReleaseConflict
			}
			p := *c.Proof
			if !s.proofValid(p) || p.ProtocolVersion != ReleaseProtocolVersion || p.OperationID != st.OperationID || p.InstanceID != st.PeerID || p.SuccessorID != st.InstanceID || p.SnapshotChecksum != st.ImportedChecksum || !p.Deadline.Equal(st.Deadline) {
				return errors.New("retirement proof does not match imported final state")
			}
			proofKey := "release-peer-proof:" + p.InstanceID
			var consumed uint64
			if raw := tx.Bucket([]byte("meta")).Get([]byte(proofKey)); raw != nil {
				if err := json.Unmarshal(raw, &consumed); err != nil {
					return err
				}
			}
			if p.FenceRevision <= consumed {
				return errors.New("retirement proof was already consumed or superseded")
			}
			if err := put(tx.Bucket([]byte("meta")), proofKey, p.FenceRevision); err != nil {
				return err
			}
			st.Phase = "active"
			st.NeverActivated = false
			st.ActivatedAt = now
			st.DeadlineExceeded = now.After(st.Deadline)
		case "rollback-standby":
			if st.OperationID != c.OperationID || st.Phase != "retired" || st.NeverActivated {
				return ErrReleaseConflict
			}
			st.Phase = "returning"
			st.ImportedChecksum = ""
			st.ExportedChecksum = ""
			st.RetirementProof = nil
		case "cancel":
			if err := prepareCancellation(tx, &st, c, now); err != nil {
				return err
			}
		case "cancel-complete":
			if st.Phase != "cancelling" || st.OperationID != c.OperationID || st.ImportedChecksum == "" || c.Proof == nil {
				return ErrReleaseConflict
			}
			p := *c.Proof
			if !s.proofValid(p) || p.ProtocolVersion != ReleaseProtocolVersion || p.OperationID != st.OperationID || p.InstanceID != st.PeerID || p.SuccessorID != st.InstanceID || p.SnapshotChecksum != st.ImportedChecksum || !p.Deadline.Equal(st.Deadline) {
				return errors.New("cancel proof does not match returned buffer")
			}
			proofKey := "release-peer-proof:" + p.InstanceID
			var consumed uint64
			if raw := tx.Bucket([]byte("meta")).Get([]byte(proofKey)); raw != nil {
				if err := json.Unmarshal(raw, &consumed); err != nil {
					return err
				}
			}
			if p.FenceRevision <= consumed {
				return errors.New("retirement proof was already consumed or superseded")
			}
			if err := put(tx.Bucket([]byte("meta")), proofKey, p.FenceRevision); err != nil {
				return err
			}
			st.Phase = "active"
			if err := put(tx.Bucket([]byte("meta")), "release-cancel-complete:"+st.OperationID, map[string]interface{}{"peer": st.PeerID, "checksum": st.ImportedChecksum, "proof_revision": p.FenceRevision}); err != nil {
				return err
			}
		default:
			return errors.New("unsupported instance release action")
		}
		st.Revision++
		st.DeadlineExceeded = st.DeadlineExceeded || (!st.Deadline.IsZero() && now.After(st.Deadline))
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := releasePut(tx, st); err != nil {
			return err
		}
		if err := releaseAudit(tx, c, st); err != nil {
			return err
		}
		result = st
		return put(tx.Bucket([]byte("meta")), receiptKey, result)
	})
	return result, err
}

// releaseSafe protects physical sends, not the repair of historical outcomes.
// Pending/retry/unknown/dead-letter facts migrate unchanged. A never-active
// successor can return only its own zero-attempt buffers, not stale preseed rows.
func releaseSafe(tx *bolt.Tx, buffered bool) error {
	if n, err := syncSending(tx); err != nil {
		return err
	} else if n > 0 {
		return errors.New("in-flight synchronous platform attempt blocks retirement")
	}
	base := map[string]bool{}
	if buffered {
		st, err := releaseRead(tx)
		if err != nil {
			return err
		}
		base, err = inheritedIntakes(tx, st)
		if err != nil {
			return err
		}
	}
	return tx.Bucket([]byte("targets")).ForEach(func(_, raw []byte) error {
		var t Target
		if err := json.Unmarshal(raw, &t); err != nil {
			return err
		}
		if buffered && base[t.IntakeID] {
			return nil
		}
		if t.State == Sending {
			return errors.New("in-flight target blocks state retirement")
		}
		if buffered && t.Attempts != 0 {
			return errors.New("buffer has a local send attempt")
		}
		return nil
	})
}
func exportedMeta(k string) bool {
	return strings.HasPrefix(k, "profile:") || strings.HasPrefix(k, "profile-state:") || k == "profile-send-next-at" || k == "formal-send-next-at" || strings.HasPrefix(k, "release-sync:") || strings.HasPrefix(k, "release-ambiguity:") || strings.HasPrefix(k, "release-forward:")
}
func manifestChecksum(m ReleaseManifest) string {
	m.MAC = ""
	m.SnapshotChecksum = ""
	raw, _ := json.Marshal(m)
	return Hash(raw)
}
func (s *Store) ExportRelease(operation string, expected uint64, final bool) (ReleaseManifest, error) {
	return s.ExportReleaseContext(context.Background(), operation, expected, final)
}
func (s *Store) ExportReleaseContext(ctx context.Context, operation string, expected uint64, final bool) (ReleaseManifest, error) {
	// A preseed is a consistent transactional observation, not a fence. It
	// may contain in-flight attempts; only the frozen final snapshot grants
	// authority to the successor and therefore takes the send boundary.
	if final {
		s.releaseMu.Lock()
		defer s.releaseMu.Unlock()
	}
	var m ReleaseManifest
	if err := ctx.Err(); err != nil {
		return m, err
	}

	err := s.update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if final {
			if raw := tx.Bucket([]byte("meta")).Get([]byte(fmt.Sprintf("release-export-revision:%s:%d", operation, expected))); raw != nil {
				cached := tx.Bucket([]byte("meta")).Get([]byte("release-export:" + operation + ":" + string(raw)))
				if cached == nil {
					return errors.New("release export revision points to missing snapshot")
				}
				return json.Unmarshal(cached, &m)
			}
		}
		st, err := releaseRead(tx)
		if err != nil {
			return err
		}
		if st.OperationID != operation || st.Revision != expected || len(s.releaseKey) < 32 {
			return ErrReleaseConflict
		}
		if size, err := estimateReleaseTransferBytes(tx); err != nil {
			return err
		} else if size > MaxReleaseTransferBytes {
			return errors.New("release transfer exceeds supported bounded protocol size")
		}
		if final {
			if st.Phase != "frozen" && st.Phase != "buffer-closed" {
				return ErrReleaseGate
			}
			if st.ExportedChecksum != "" {
				raw := tx.Bucket([]byte("meta")).Get([]byte("release-export:" + operation + ":" + st.ExportedChecksum))
				if raw == nil {
					return errors.New("persisted final snapshot missing")
				}
				return json.Unmarshal(raw, &m)
			}
			if err := releaseSafe(tx, st.NeverActivated); err != nil {
				return err
			}
		}
		m = ReleaseManifest{ProtocolVersion: ReleaseProtocolVersion, OperationID: operation, SourceID: st.InstanceID, DestinationID: st.PeerID, Deadline: st.Deadline, Final: final, BufferedOnly: st.NeverActivated, ExportedAt: time.Now().UTC(), Buckets: map[string]map[string][]byte{}}
		for _, name := range []string{"meta", "intakes", "targets", "dedupe", "audit", "blocked"} {
			rows := map[string][]byte{}
			if err := tx.Bucket([]byte(name)).ForEach(func(k, v []byte) error {
				if name == "meta" && !exportedMeta(string(k)) {
					return nil
				}
				rows[string(k)] = append([]byte(nil), v...)
				return nil
			}); err != nil {
				return err
			}
			m.Buckets[name] = rows
		}
		if final && st.NeverActivated {
			if err := filterBufferedManifest(tx, &m, st); err != nil {
				return err
			}
		}
		m.SnapshotChecksum = manifestChecksum(m)
		m.MAC = s.releaseMAC(m)
		if final {
			st.ExportedChecksum = m.SnapshotChecksum
			st.Revision++
			if err := put(tx.Bucket([]byte("meta")), "release-export:"+operation+":"+m.SnapshotChecksum, m); err != nil {
				return err
			}
			if err := tx.Bucket([]byte("meta")).Put([]byte(fmt.Sprintf("release-export-revision:%s:%d", operation, expected)), []byte(m.SnapshotChecksum)); err != nil {
				return err
			}
			if err := releasePut(tx, st); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	})
	return m, err
}

// orderedIntakes retains each actual AM generation. Equal content after an
// intervening generation is not a duplicate, and repeat expiry is preserved.
func orderedIntakes(rows map[string][]byte, key string) []Intake {
	var result []Intake
	for _, raw := range rows {
		var i Intake
		if json.Unmarshal(raw, &i) == nil && i.DedupeKey == key {
			result = append(result, i)
		}
	}
	sort.Slice(result, func(a, b int) bool {
		if result[a].AcceptedAt.Equal(result[b].AcceptedAt) {
			return result[a].ID < result[b].ID
		}
		return result[a].AcceptedAt.Before(result[b].AcceptedAt)
	})
	return result
}
func cloneRows(tx *bolt.Tx, name string) map[string][]byte {
	rows := map[string][]byte{}
	tx.Bucket([]byte(name)).ForEach(func(k, v []byte) error { rows[string(k)] = append([]byte(nil), v...); return nil })
	return rows
}
func targetIdentity(t Target) string {
	t = canonicalFeishuTarget(t)
	t.ID = ""
	t.IntakeID = ""
	t.State = ""
	t.Attempts = 0
	t.Owner = ""
	t.AttemptID = ""
	t.AvailableAt = time.Time{}
	t.UpdatedAt = time.Time{}
	t.Outcome = ""
	t.Evidence = ""
	t.SendPrepared = false
	raw, _ := json.Marshal(t)
	return string(raw)
}
func intakeTargets(rows map[string][]byte, i Intake) (map[string]Target, error) {
	originals := make([]Target, 0, len(i.TargetIDs))
	revisions := map[string]string{}
	for _, id := range i.TargetIDs {
		var t Target
		if err := json.Unmarshal(rows[id], &t); err != nil {
			return nil, err
		}
		originals = append(originals, t)
		revisions[t.ContentRevision] = canonicalFeishuTarget(t).ContentRevision
	}
	out := map[string]Target{}
	for _, t := range originals {
		identity := t
		identity.DependencyRevisions = append([]string(nil), t.DependencyRevisions...)
		for n, dependency := range identity.DependencyRevisions {
			canonical, ok := revisions[dependency]
			if !ok {
				return nil, errors.New("handoff history dependency lacks exact frozen parent")
			}
			identity.DependencyRevisions[n] = canonical
		}
		out[targetIdentity(identity)] = t
	}
	if len(out) != len(i.TargetIDs) {
		return nil, errors.New("duplicate manifest target identity")
	}
	return out, nil
}
func confirmAlias(local Intake, source Intake, localTargets, sourceTargets map[string][]byte) error {
	if local.RequestHash != source.RequestHash || local.TerminalReason != source.TerminalReason {
		return errors.New("handoff intake content conflict")
	}
	lt, err := intakeTargets(localTargets, local)
	if err != nil {
		return err
	}
	rt, err := intakeTargets(sourceTargets, source)
	if err != nil {
		return err
	}
	if len(lt) != len(rt) {
		return errors.New("handoff target plan conflict")
	}
	for ident, t := range lt {
		prior, ok := rt[ident]
		if !ok || prior.State != Delivered || t.State != Pending || t.Attempts != 0 {
			return fmt.Errorf("handoff duplicate lacks exact successful predecessor receipt: match=%t source_state=%s local_state=%s local_attempts=%d", ok, prior.State, t.State, t.Attempts)
		}
		t.State = Delivered
		t.Outcome = "handoff_confirmed_duplicate"
		t.Evidence = prior.Evidence
		t.UpdatedAt = prior.UpdatedAt
		raw, _ := json.Marshal(t)
		localTargets[t.ID] = raw
	}
	return nil
}
func sameBoundary(a, b Intake) bool {
	if a.RequestHash != b.RequestHash || a.AMGroup != b.AMGroup || a.WindowMillis != b.WindowMillis {
		return false
	}
	window := time.Duration(a.WindowMillis) * time.Millisecond
	if window <= 0 {
		return true
	}
	d := a.AcceptedAt.Sub(b.AcceptedAt)
	if d < 0 {
		d = -d
	}
	return d < window
}
func (s *Store) ImportRelease(m ReleaseManifest, c ReleaseCommand) (ReleaseState, error) {
	return s.ImportReleaseContext(context.Background(), m, c)
}
func (s *Store) ImportReleaseContext(ctx context.Context, m ReleaseManifest, c ReleaseCommand) (ReleaseState, error) {
	// Import never transfers sending authority. Standby cannot send, and a
	// cancelled release retains the original sender. Transactional phase/CAS
	// checks race safely with activation/retirement while preserving current
	// local attempts during buffer return.
	var result ReleaseState
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if m.ProtocolVersion != ReleaseProtocolVersion || m.SnapshotChecksum != manifestChecksum(m) {
		return result, errors.New("release manifest protocol or checksum mismatch")
	}
	mac := m.MAC
	m.MAC = ""
	if len(s.releaseKey) < 32 || !hmac.Equal([]byte(mac), []byte(s.releaseMAC(m))) {
		return result, errors.New("release manifest authentication failed")
	}
	m.MAC = mac
	if err := s.importStorageCapacity(m); err != nil {
		return result, err
	}
	for _, b := range []string{"meta", "intakes", "targets", "dedupe", "audit", "blocked"} {
		if m.Buckets[b] == nil {
			return result, errors.New("incomplete release snapshot")
		}
	}
	err := s.update(func(tx *bolt.Tx) error {
		st, err := releaseRead(tx)
		if err != nil {
			return err
		}
		if st.ImportedChecksum == m.SnapshotChecksum && st.OperationID == c.OperationID {
			result = st
			return nil
		}
		if err := validateCommand(st, c); err != nil {
			return err
		}
		if c.Action == "cancel-import" {
			if err := validateCancelledBuffer(m); err != nil {
				return err
			}
			if err := prepareCancellation(tx, &st, c, time.Now().UTC()); err != nil {
				return err
			}
		}
		if (c.Action != "import" && c.Action != "cancel-import") || st.OperationID != m.OperationID || st.PeerID != m.SourceID || st.InstanceID != m.DestinationID || !st.Deadline.Equal(m.Deadline) || (st.Phase != "buffering" && st.Phase != "returning" && st.Phase != "buffer-closed" && !(st.Phase == "cancelling" && m.Final && m.BufferedOnly)) {
			return ErrReleaseConflict
		}
		if st.Phase == "cancelling" {
			if err := validateCancelledBuffer(m); err != nil {
				return err
			}
		}
		if !m.Final && st.ImportedChecksum != "" {
			return errors.New("preseed may only be imported once")
		}
		meta := tx.Bucket([]byte("meta"))
		baseKey := "release-base:" + st.OperationID
		baseIDs := map[string]bool{}
		if raw := meta.Get([]byte(baseKey)); raw != nil {
			if err := json.Unmarshal(raw, &baseIDs); err != nil {
				return err
			}
		}
		localIntakes := cloneRows(tx, "intakes")
		localTargets := cloneRows(tx, "targets")
		localDedupe := cloneRows(tx, "dedupe")
		sourceIDs := map[string]bool{}
		for id, raw := range m.Buckets["intakes"] {
			var in Intake
			if err := json.Unmarshal(raw, &in); err != nil || in.ID != id {
				return errors.New("invalid manifest intake")
			}
			sourceIDs[id] = true
			for _, tid := range in.TargetIDs {
				var t Target
				if err := json.Unmarshal(m.Buckets["targets"][tid], &t); err != nil || t.ID != tid || t.IntakeID != id {
					return errors.New("inconsistent manifest target dependency")
				}
			}
		}
		inherited, err := inheritedIntakes(tx, st)
		if err != nil {
			return err
		}
		for id := range sourceIDs {
			inherited[id] = true
		}
		if err := put(meta, "release-inherited:"+st.OperationID, inherited); err != nil {
			return err
		}
		if !m.Final {
			for id := range sourceIDs {
				baseIDs[id] = true
			}
			if err := put(meta, baseKey, baseIDs); err != nil {
				return err
			}
		}
		if err := validateReleaseDedupe(m); err != nil {
			return err
		}
		if st.Phase != "cancelling" {
			if err := reconcileAMHistories(tx, m, baseIDs, localIntakes, localTargets, localDedupe, sourceIDs); err != nil {
				return err
			}
		}
		for name, rows := range m.Buckets {
			if name != "meta" && name != "intakes" && name != "targets" && name != "dedupe" && name != "audit" && name != "blocked" {
				return errors.New("unsupported release snapshot bucket")
			}
			for key, value := range rows {
				if st.Phase == "cancelling" && name == "intakes" && localIntakes[key] != nil {
					var prior, incoming Intake
					if json.Unmarshal(localIntakes[key], &prior) != nil || json.Unmarshal(value, &incoming) != nil || !sameCancelledIntake(prior, incoming) {
						return errors.New("cancel intake identity conflict")
					}
					continue
				}
				if st.Phase == "cancelling" && name == "targets" && localTargets[key] != nil {
					var prior, incoming Target
					if json.Unmarshal(localTargets[key], &prior) != nil || json.Unmarshal(value, &incoming) != nil || targetIdentity(prior) != targetIdentity(incoming) {
						return errors.New("cancel target identity conflict")
					}
					continue
				}
				if name == "meta" {
					if !exportedMeta(key) {
						return errors.New("manifest attempted to replace instance fence")
					}
					if key == "profile-state:formal" && s.formalDisabled {
						var state ProfileState
						if json.Unmarshal(value, &state) != nil || !state.Paused {
							return errors.New("import conflicts with local formal startup fence")
						}
					}
					if prior := meta.Get([]byte(key)); prior != nil && strings.HasPrefix(key, "profile:") && string(prior) != string(value) {
						return errors.New("immutable delivery profile conflict")
					}
				}
				if name == "meta" && strings.HasPrefix(key, "release-forward:") {
					merged, err := mergeForwardReference(tx, key, value, st.InstanceID)
					if err != nil {
						return err
					}
					value = merged
				}
				if name == "meta" && strings.HasPrefix(key, "release-sync:") {
					merged, err := mergeSynchronousReference(tx, key, value, st.InstanceID)
					if err != nil {
						return err
					}
					value = merged
				}
				if err := tx.Bucket([]byte(name)).Put([]byte(key), value); err != nil {
					return err
				}
			}
		}
		// Keep every newly accepted B intake; only exact boundary aliases acquire
		// authenticated A receipt state. The current generation remains the last
		// local generation after the overlap, including legitimate content returns.
		for id, raw := range localIntakes {
			if sourceIDs[id] {
				continue
			}
			if err := tx.Bucket([]byte("intakes")).Put([]byte(id), raw); err != nil {
				return err
			}
		}
		for id, raw := range localTargets {
			var t Target
			if err := json.Unmarshal(raw, &t); err != nil {
				return err
			}
			if sourceIDs[t.IntakeID] {
				continue
			}
			if err := tx.Bucket([]byte("targets")).Put([]byte(id), raw); err != nil {
				return err
			}
		}
		for key, id := range localDedupe {
			var in Intake
			if json.Unmarshal(localIntakes[string(id)], &in) != nil {
				return errors.New("invalid local dedupe reference")
			}
			if sourceIDs[in.ID] || baseIDs[in.ID] {
				continue
			}
			if err := tx.Bucket([]byte("dedupe")).Put([]byte(key), id); err != nil {
				return err
			}
		}
		if err := markLegacyAMIntakes(tx); err != nil {
			return err
		}
		if st.Phase == "cancelling" {
			if err := reorderCancelledBuffer(tx, sourceIDs); err != nil {
				return err
			}
		}
		if err := reconcileLifecycleOrder(tx); err != nil {
			return err
		}
		if err := rebuildReleaseIndexes(tx); err != nil {
			return err
		}
		testCount, err := profileActiveCount(tx, "test")
		if err != nil {
			return err
		}
		testLimit := s.testActiveLimit
		if testLimit <= 0 {
			testLimit = s.limits.MaxTargets
		}
		if testCount > testLimit || activeCount(tx)-uint64(testCount) > uint64(s.limits.MaxTargets) {
			return ErrCapacity
		}
		if err := s.capacity(); err != nil {
			return err
		}
		st.ImportedChecksum = m.SnapshotChecksum
		st.Revision++
		st.DeadlineExceeded = time.Now().UTC().After(st.Deadline)
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := releasePut(tx, st); err != nil {
			return err
		}
		if err := releaseAudit(tx, c, st); err != nil {
			return err
		}
		result = st
		return nil
	})
	return result, err
}
func rebuildReleaseIndexes(tx *bolt.Tx) error {
	for _, name := range []string{"ready", "ready-lanes", "blocked"} {
		if err := tx.DeleteBucket([]byte(name)); err != nil {
			return err
		}
		if _, err := tx.CreateBucket([]byte(name)); err != nil {
			return err
		}
	}
	if err := rebuildGroupOrder(tx); err != nil {
		return err
	}
	active := uint64(0)
	counts := map[string]int{}
	if err := tx.Bucket([]byte("targets")).ForEach(func(_, raw []byte) error {
		var t Target
		if err := json.Unmarshal(raw, &t); err != nil {
			return err
		}
		if err := validateProfileTarget(tx, t); err != nil {
			return err
		}
		if activeState(t.State) {
			active++
			counts[t.ProfileID]++
		}
		if t.State != Pending && t.State != Retryable {
			return nil
		}
		blocked := len(t.DependencyRevisions) > 0
		if blocked {
			var in Intake
			if err := json.Unmarshal(tx.Bucket([]byte("intakes")).Get([]byte(t.IntakeID)), &in); err != nil {
				return err
			}
			for _, id := range in.TargetIDs {
				var parent Target
				if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get([]byte(id)), &parent); err != nil {
					return err
				}
				if parent.State == Delivered {
					for _, dep := range t.DependencyRevisions {
						if dep == parent.ContentRevision {
							blocked = false
						}
					}
				}
			}
		}
		if blocked {
			return tx.Bucket([]byte("blocked")).Put([]byte(t.ID), []byte(t.ID))
		}
		return putReady(tx, t)
	}); err != nil {
		return err
	}
	if err := setActiveCount(tx, active); err != nil {
		return err
	}
	for _, lane := range []string{"", "test", "formal"} {
		if err := put(tx.Bucket([]byte("meta")), "active-targets-profile:"+lane, counts[lane]); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) ReadReleaseExport(operation, checksum string) (ReleaseManifest, error) {
	var m ReleaseManifest
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte("meta")).Get([]byte("release-export:" + operation + ":" + checksum))
		if raw == nil {
			return errors.New("release export not found")
		}
		return json.Unmarshal(raw, &m)
	})
	return m, err
}
func (s *Store) StringReleaseStatus() string {
	st, err := s.ReleaseStatus()
	if err != nil {
		return "unavailable"
	}
	return fmt.Sprintf("%s:%s:%d", st.InstanceID, st.Phase, st.Revision)
}
func groupOrder(i Intake) uint64 {
	if i.DeliveryOrder != 0 {
		return i.DeliveryOrder
	}
	return i.Generation
}
func groupOrderKey(i Intake) string {
	return fmt.Sprintf("am-order:%s:%020d", Hash([]byte(i.DedupeKey)), groupOrder(i))
}
func groupDeliveryBlocked(tx *bolt.Tx, t Target) (bool, error) {
	st, err := releaseRead(tx)
	if err != nil || st.InstanceID == "" {
		return false, err
	}
	var current Intake
	if err := json.Unmarshal(tx.Bucket([]byte("intakes")).Get([]byte(t.IntakeID)), &current); err != nil {
		return false, err
	}
	if !current.AMGroup || current.DedupeKey == "" || current.IdentityOrigin == "legacy-unsequenced" {
		return false, nil
	}
	prefix := "am-order:" + Hash([]byte(current.DedupeKey)) + ":"
	limit := groupOrderKey(current)
	cursor := tx.Bucket([]byte("meta")).Cursor()
	for k, v := cursor.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix) && string(k) < limit; k, v = cursor.Next() {
		var prior Intake
		if err := json.Unmarshal(tx.Bucket([]byte("intakes")).Get(v), &prior); err != nil {
			return false, err
		}
		for _, id := range prior.TargetIDs {
			var target Target
			if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get([]byte(id)), &target); err != nil {
				return false, err
			}
			if target.State != Pending && target.State != Retryable && target.State != Sending {
				continue
			}
			if target.ProfileID == t.ProfileID && target.Receiver == t.Receiver && target.Channel == t.Channel && target.Destination == t.Destination && (lifecycleOverlap(target.Payload, t.Payload) || queuedObservationPrecedes(prior, current, target, t)) {
				return true, nil
			}
		}
	}
	return false, nil
}

// Frozen payloads normally contain exact alert IDs/labels and startsAt. Legacy
// renders without these facts conservatively retain same-group ordering only.
func lifecycleOverlap(left, right json.RawMessage) bool {
	identities := func(raw json.RawMessage) map[string]bool {
		var value struct {
			Data struct {
				Alerts []struct {
					ID       string            `json:"id"`
					Labels   map[string]string `json:"labels"`
					StartsAt time.Time         `json:"startsAt"`
				} `json:"alerts"`
			} `json:"data"`
		}
		if json.Unmarshal(raw, &value) != nil || len(value.Data.Alerts) == 0 {
			return nil
		}
		keys := map[string]bool{}
		for _, alert := range value.Data.Alerts {
			if alert.StartsAt.IsZero() || (alert.ID == "" && len(alert.Labels) == 0) {
				return nil
			}
			identity := alert.ID
			if len(alert.Labels) > 0 {
				encoded, _ := json.Marshal(alert.Labels)
				identity = Hash(encoded)
			}
			keys[identity+":"+alert.StartsAt.UTC().Format(time.RFC3339Nano)] = true
		}
		return keys
	}
	a, b := identities(left), identities(right)
	if len(a) == 0 || len(b) == 0 {
		return true
	}
	for key := range a {
		if b[key] {
			return true
		}
	}
	return false
}

func rebuildGroupOrder(tx *bolt.Tx) error {
	meta := tx.Bucket([]byte("meta"))
	var keys [][]byte
	if err := meta.ForEach(func(k, _ []byte) error {
		if strings.HasPrefix(string(k), "am-order:") {
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
	return tx.Bucket([]byte("intakes")).ForEach(func(_, raw []byte) error {
		var i Intake
		if err := json.Unmarshal(raw, &i); err != nil {
			return err
		}
		if !i.AMGroup || i.DedupeKey == "" || i.IdentityOrigin == "legacy-unsequenced" {
			return nil
		}
		return meta.Put([]byte(groupOrderKey(i)), []byte(i.ID))
	})
}

func bufferDeadline(st ReleaseState) time.Time {
	if st.Phase == "returning" {
		return st.Deadline
	}
	limit := st.Deadline
	if !st.StartedAt.IsZero() && st.MaxSendDelayMillis > 0 {
		sendLimit := st.StartedAt.Add(time.Duration(st.MaxSendDelayMillis) * time.Millisecond)
		if limit.IsZero() || sendLimit.Before(limit) {
			limit = sendLimit
		}
	}
	return limit
}

func intakeLifecycleUnsettled(tx *bolt.Tx, i Intake) (bool, error) {
	for _, id := range i.TargetIDs {
		var t Target
		if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get([]byte(id)), &t); err != nil {
			return false, err
		}
		if t.State != Delivered && (t.State != DeadLetter || t.Outcome != "operator_reconciled") {
			return true, nil
		}
	}
	return false, nil
}

const MaxReleaseTransferBytes int64 = 64 << 20

// EstimateReleaseTransferBytes is a conservative JSON wire upper bound, without
// constructing or exposing a second copy of any frozen notification payload.
func estimateReleaseTransferBytes(tx *bolt.Tx) (int64, error) {
	total := int64(32 << 10)
	for _, name := range []string{"meta", "intakes", "targets", "dedupe", "audit", "blocked"} {
		total += int64(6*len(name) + 8)
		if err := tx.Bucket([]byte(name)).ForEach(func(k, v []byte) error {
			if name == "meta" && !exportedMeta(string(k)) {
				return nil
			}
			total += int64(6*len(k) + 8 + 4*((len(v)+2)/3))
			return nil
		}); err != nil {
			return 0, err
		}
	}
	return total, nil
}
func (s *Store) EstimateReleaseTransferBytes() (int64, error) {
	var size int64
	err := s.db.View(func(tx *bolt.Tx) error { var err error; size, err = estimateReleaseTransferBytes(tx); return err })
	return size, err
}

func releaseAmbiguity(tx *bolt.Tx, operation string) error {
	if operation == "" {
		return nil
	}
	raw := tx.Bucket([]byte("meta")).Get([]byte("release-ambiguity:" + operation))
	if raw == nil {
		return nil
	}
	var n uint64
	if err := json.Unmarshal(raw, &n); err != nil {
		return err
	}
	return nil
}

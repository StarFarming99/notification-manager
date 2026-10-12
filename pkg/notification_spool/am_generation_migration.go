package notification_spool

import (
	"encoding/json"
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"
)

// SeedAMGroup is an audited upgrade operation for a paused lane. Legacy v1
// intakes did not store AM receiver/group metadata, so choosing an old hash as
// the current group automatically would either swallow a legitimate return or
// replay a card. The operator must prove the exact last accepted group intake.
// This only builds indexes; it never changes, claims or replays any target.
func (s *Store) SeedAMGroup(profileID, version, groupHash, intakeID, requestHash string, expectedRevision uint64, actor, evidence string) error {
	if len(groupHash) != 64 || len(requestHash) != 64 || actor == "" || evidence == "" || len(actor) > 128 || len(evidence) > 512 {
		return errors.New("bounded exact AM group migration evidence required")
	}
	return s.update(func(tx *bolt.Tx) error {
		state, err := readProfileState(tx, profileID)
		if err != nil {
			return err
		}
		if !state.Paused || state.Revision != expectedRevision {
			return ErrProfileConflict
		}
		if _, err := readProfile(tx, profileID, version); err != nil {
			return err
		}
		intakes, dedupe := tx.Bucket([]byte("intakes")), tx.Bucket([]byte("dedupe"))
		var intake Intake
		if err := json.Unmarshal(intakes.Get([]byte(intakeID)), &intake); err != nil {
			return err
		}
		legacyKey := profileID + ":" + version + ":am:" + requestHash
		if intake.RequestHash != requestHash || string(dedupe.Get([]byte(legacyKey))) != intakeID {
			return errors.New("legacy intake identity does not match migration proof")
		}
		groupKey := profileID + ":" + version + ":am-group:" + groupHash
		if previous := dedupe.Get([]byte(groupKey)); previous != nil && string(previous) != intakeID {
			return errors.New("AM group already has another current generation")
		}
		for _, id := range intake.TargetIDs {
			var target Target
			if err := json.Unmarshal(tx.Bucket([]byte("targets")).Get([]byte(id)), &target); err != nil {
				return err
			}
			if target.ProfileID != profileID || target.ProfileVersion != version {
				return errors.New("legacy target profile does not match migration")
			}
		}
		if err := dedupe.Put([]byte(groupKey), []byte(intakeID)); err != nil {
			return err
		}
		if err := dedupe.Put([]byte(groupKey+":intent:"+requestHash), []byte(intakeID)); err != nil {
			return err
		}
		id, err := ID()
		if err != nil {
			return err
		}
		return put(tx.Bucket([]byte("audit")), id, map[string]interface{}{"action": "seed_am_group_generation", "profile_id": profileID, "profile_version": version, "group_hash": groupHash, "intake_id": intakeID, "request_hash": requestHash, "actor": actor, "evidence": evidence, "at": time.Now().UTC()})
	})
}

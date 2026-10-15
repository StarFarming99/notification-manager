package notification_spool

import (
	"encoding/json"
	"time"

	bolt "go.etcd.io/bbolt"
)

// A consistent preseed can observe another process's Sending attempt. It is an
// uncertain external observation, never a physical in-flight request in this
// process. Keep its exact key and evidence, without inventing legacy ownership.
func mergeSynchronousReference(tx *bolt.Tx, key string, raw []byte, localInstance string) ([]byte, error) {
	var incoming SynchronousAttempt
	if err := json.Unmarshal(raw, &incoming); err != nil {
		return nil, err
	}
	if incoming.ID == "" || key != "release-sync:"+incoming.ID || localInstance == "" {
		return nil, ErrReleaseConflict
	}
	selected := incoming
	if previous := tx.Bucket([]byte("meta")).Get([]byte(key)); previous != nil {
		var prior SynchronousAttempt
		if err := json.Unmarshal(previous, &prior); err != nil {
			return nil, err
		}
		if prior.ID != incoming.ID || prior.ProfileID != incoming.ProfileID || prior.InstanceID != incoming.InstanceID {
			return nil, ErrReleaseConflict
		}
		if prior.InstanceID == localInstance && prior.ImportedAt == nil {
			return append([]byte(nil), previous...), nil
		}
		if !incoming.UpdatedAt.After(prior.UpdatedAt) {
			selected = prior
		}
	}
	if selected.State == Sending {
		observed, importedAt := selected.UpdatedAt, time.Now().UTC()
		selected.SnapshotState, selected.SnapshotOutcome, selected.SnapshotUpdatedAt = selected.State, selected.Outcome, &observed
		selected.State, selected.Outcome = Unknown, "snapshot_synchronous_owner_external"
		selected.ImportedFrom, selected.ImportedAt = selected.InstanceID, &importedAt
		if selected.InstanceID == "" {
			selected.Outcome = "snapshot_synchronous_owner_legacy_unknown"
		}
		if selected.InstanceID == localInstance {
			selected.Outcome = "snapshot_synchronous_local_reference_unobserved"
		}
	}
	return json.Marshal(selected)
}

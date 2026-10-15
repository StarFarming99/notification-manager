package notification_spool

import (
	"encoding/json"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// A current operation retains its exact payload/readback and all proof fences.
// Only unrelated completed operation caches become eligible for retention.
func cleanupReleaseCaches(tx *bolt.Tx, retention time.Duration, batch int, now time.Time) error {
	st, err := releaseRead(tx)
	if err != nil {
		return err
	}
	meta := tx.Bucket([]byte("meta"))
	keys := map[string]bool{}
	removedOps := map[string]bool{}
	checksums := map[string]bool{}
	if err := meta.ForEach(func(k, v []byte) error {
		if len(keys) >= batch {
			return nil
		}
		key := string(k)
		if strings.HasPrefix(key, "release-export:") {
			var m ReleaseManifest
			if err := json.Unmarshal(v, &m); err != nil {
				return err
			}
			if m.OperationID != st.OperationID && m.ExportedAt.Add(retention).Before(now) {
				keys[key] = true
				checksums[m.SnapshotChecksum] = true
				removedOps[m.OperationID] = true
			}
		}
		if strings.HasPrefix(key, "release-command:") {
			var previous ReleaseState
			if err := json.Unmarshal(v, &previous); err != nil {
				return err
			}
			if previous.OperationID != st.OperationID && !previous.StartedAt.IsZero() && previous.StartedAt.Add(retention).Before(now) {
				keys[key] = true
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if len(checksums) > 0 {
		if err := meta.ForEach(func(k, v []byte) error {
			if strings.HasPrefix(string(k), "release-export-revision:") && checksums[string(v)] {
				keys[string(k)] = true
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for operation := range removedOps {
		keys["release-base:"+operation] = true
	}
	for key := range keys {
		if err := meta.Delete([]byte(key)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) importStorageCapacity(m ReleaseManifest) error {
	if err := s.capacity(); err != nil {
		return err
	}
	// Conservative COW headroom: raw source rows twice, plus one additional
	// page per row and transaction margin. Near-limit imports fail closed.
	additional := uint64(64 << 10)
	for _, rows := range m.Buckets {
		for key, value := range rows {
			additional += uint64(2*(len(key)+len(value)) + 4096)
		}
	}
	fi, err := os.Stat(s.path)
	if err != nil {
		return err
	}
	if uint64(fi.Size())+additional > uint64(s.limits.MaxBytes) {
		return ErrCapacity
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(filepath.Dir(s.path), &fs); err != nil {
		return err
	}
	if uint64(fs.Bavail)*uint64(fs.Bsize) < s.limits.ReserveBytes+additional {
		return ErrCapacity
	}
	return nil
}

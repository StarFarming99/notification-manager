package notification_spool

import (
	"errors"
	"syscall"

	bolt "go.etcd.io/bbolt"
)

// update distinguishes rejected plans from failed durable commits. Only a
// successful write clears a storage outage; a health probe never clears it.
func (s *Store) update(fn func(*bolt.Tx) error) error {
	var rejected error
	err := s.db.Update(func(tx *bolt.Tx) error {
		rejected = fn(tx)
		return rejected
	})
	if rejected != nil {
		return err
	}
	s.healthMu.Lock()
	defer s.healthMu.Unlock()
	if err == nil {
		s.lastStorageError = ""
		return nil
	}
	s.storageWriteFailures++
	s.lastStorageError = "storage_write_failed"
	for _, class := range []struct {
		errno error
		name  string
	}{
		{syscall.ENOSPC, "storage_no_space"}, {syscall.EFBIG, "storage_file_limit"},
		{syscall.EROFS, "storage_read_only"}, {syscall.EIO, "storage_io_error"},
	} {
		if errors.Is(err, class.errno) {
			s.lastStorageError = class.name
			break
		}
	}
	return err
}

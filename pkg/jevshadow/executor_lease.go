package jevshadow

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/sys/unix"
)

// AcquireExecutorLease fences both persistent stores before starting listeners
// or outbox workers. Recreate/RWO alone is not a process-level ownership fence.
func AcquireExecutorLease(config Config) (func(), error) {
	dirs := []string{config.ReceiptOutboxDir, config.CardStateDir}
	sort.Strings(dirs)
	var files []*os.File
	seen := make(map[string]bool)
	closeAll := func() {
		for _, file := range files {
			file.Close()
		}
	}
	for _, dir := range dirs {
		dir = filepath.Clean(dir)
		if seen[dir] {
			continue
		}
		seen[dir] = true
		if !filepath.IsAbs(dir) {
			closeAll()
			return nil, fmt.Errorf("executor persistent directories must be absolute")
		}
		if err := os.MkdirAll(dir, 0700); err != nil {
			closeAll()
			return nil, err
		}
		fd, err := unix.Open(filepath.Join(dir, ".executor-owner.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW, 0600)
		if err != nil {
			closeAll()
			return nil, err
		}
		file := os.NewFile(uintptr(fd), "executor-owner")
		files = append(files, file)
		if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
			closeAll()
			return nil, fmt.Errorf("executor persistent store already owned: %w", err)
		}
	}
	return closeAll, nil
}

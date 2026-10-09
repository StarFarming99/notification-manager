package jevshadow

import (
	"path/filepath"
	"testing"
)

func TestExecutorLeaseFencesBothStoresAndDeduplicatesDirectory(t *testing.T) {
	root := t.TempDir()
	config := Config{ReceiptOutboxDir: filepath.Join(root, "receipt"), CardStateDir: filepath.Join(root, "cards")}
	closeFirst, err := AcquireExecutorLease(config)
	if err != nil {
		t.Fatal(err)
	}
	if release, err := AcquireExecutorLease(config); err == nil {
		release()
		t.Fatal("second executor obtained writer ownership")
	}
	closeFirst()
	config.CardStateDir = config.ReceiptOutboxDir
	release, err := AcquireExecutorLease(config)
	if err != nil {
		t.Fatal("same physical directory must not self-conflict", err)
	}
	release()
}

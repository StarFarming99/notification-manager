package jevshadow

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

// Gap records contain only reconciliation keys, never card text or credentials.
type RelayGap struct {
	Instance    string    `json:"instance"`
	Sequence    uint64    `json:"sequence"`
	RecordedAt  time.Time `json:"recorded_at"`
	Reason      string    `json:"reason"`
	Receiver    string    `json:"receiver"`
	Destination string    `json:"destination"`
	SenderApp   string    `json:"sender_app"`
	MessageID   string    `json:"message_id"`
}

func (r *successRelay) gap(d SuccessfulDelivery, reason string, sequence uint64) {
	g := RelayGap{Instance: r.instance, Sequence: sequence, Reason: reason, Receiver: d.Receiver,
		Destination: d.Destination, SenderApp: d.SenderApp, MessageID: d.MessageID}
	select {
	case r.gaps <- g:
	default:
		atomic.AddUint64(&r.gapOverflow, 1)
	}
}

func (r *successRelay) runGaps(ctx context.Context) {
	for {
		select {
		case g := <-r.gaps:
			r.recordGap(g)
		case <-ctx.Done():
			<-r.done
			// Drain the bounded channel without ever delaying original notification shutdown.
			for {
				select {
				case g := <-r.gaps:
					r.recordGap(g)
				default:
					return
				}
			}
		}
	}
}

func (r *successRelay) recordGap(g RelayGap) {
	g.RecordedAt = time.Now().UTC()
	r.gapMu.Lock()
	if len(r.recent) == 256 {
		copy(r.recent, r.recent[1:])
		r.recent = r.recent[:255]
	}
	r.recent = append(r.recent, g)
	r.gapMu.Unlock()
	_ = r.logger.Log("msg", "Jev success relay gap", "instance", g.Instance, "sequence", g.Sequence,
		"reason", g.Reason, "receiver", g.Receiver, "destination", g.Destination, "sender_app", g.SenderApp, "message_id", g.MessageID)
	if r.journalPath != "" {
		if err := r.appendGap(g); err != nil {
			atomic.AddUint64(&r.journalErrors, 1)
			_ = r.logger.Log("msg", "Jev relay gap journal unavailable", "instance", r.instance)
		}
	}
}

func (r *successRelay) appendGap(g RelayGap) error {
	if !filepath.IsAbs(r.journalPath) {
		return errors.New("gap journal requires an absolute path")
	}
	if err := os.MkdirAll(filepath.Dir(r.journalPath), 0700); err != nil {
		return err
	}
	// Preserve the original spool's 64 MiB reserve when sharing its PVC.
	var disk syscall.Statfs_t
	if err := syscall.Statfs(filepath.Dir(r.journalPath), &disk); err != nil {
		return err
	}
	if disk.Bavail*uint64(disk.Bsize) < 128<<20 {
		return errors.New("gap journal free-space reserve reached")
	}
	if info, err := os.Lstat(r.journalPath); err == nil {
		if !info.Mode().IsRegular() {
			return errors.New("gap journal must be a regular file")
		}
		if info.Size() >= 32<<20 {
			if err := os.Rename(r.journalPath, r.journalPath+".1"); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(r.journalPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = json.NewEncoder(f).Encode(g); err != nil {
		return err
	}
	return f.Sync()
}

func (s *Service) RelaySnapshot() map[string]interface{} {
	if !s.IsRelay() {
		return map[string]interface{}{"enabled": false}
	}
	r := s.relay
	r.gapMu.Lock()
	gaps := append([]RelayGap(nil), r.recent...)
	r.gapMu.Unlock()
	return map[string]interface{}{"enabled": true, "instance": r.instance, "counts": s.RelayStatus(),
		"recent_gaps": gaps, "gap_records_queued": len(r.gaps), "journal_enabled": r.journalPath != "",
		"journal_retention_bytes": 64 << 20, "reconciliation_complete": r.journalPath != "" && atomic.LoadUint64(&r.gapOverflow) == 0 && atomic.LoadUint64(&r.journalErrors) == 0}
}

package store

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/store/provider"
	"github.com/kubesphere/notification-manager/pkg/store/provider/memory"
)

const (
	providerMemory = "memory"
)

type AlertStore struct {
	provider.Provider
	Durable      *spool.Store
	closing      atomic.Bool
	shutdownOnce sync.Once
	shutdownMu   sync.RWMutex
	mode         string
	deadline     time.Time
}

func NewCheckedAlertStore(kind string) (*AlertStore, error) {
	if kind == providerMemory {
		return NewAlertStore(kind), nil
	}
	if kind != "notification_durable" {
		return nil, errors.New("unknown original notification store provider")
	}
	path := os.Getenv("NM_NOTIFICATION_SPOOL_PATH")
	if !filepath.IsAbs(path) {
		return nil, errors.New("NM_NOTIFICATION_SPOOL_PATH must be absolute")
	}
	mode := os.Getenv("NM_SHUTDOWN_MODE")
	if mode == "" {
		mode = "handoff"
	}
	if mode != "handoff" && mode != "drain" {
		return nil, errors.New("NM_SHUTDOWN_MODE must be handoff or drain")
	}
	db, err := spool.Open(path, spool.Limits{})
	if err != nil {
		return nil, err
	}
	return &AlertStore{Durable: db, mode: mode}, nil
}

func (s *AlertStore) Accepting() bool { return !s.closing.Load() }
func (s *AlertStore) ShutdownState() (bool, string, time.Time) {
	s.shutdownMu.RLock()
	defer s.shutdownMu.RUnlock()
	return s.closing.Load(), s.mode, s.deadline
}
func (s *AlertStore) Close() error {
	var err error
	s.shutdownOnce.Do(func() {
		s.shutdownMu.Lock()
		s.deadline = time.Now().Add(30 * time.Second)
		s.closing.Store(true)
		s.shutdownMu.Unlock()
		if s.Durable == nil && s.Provider != nil {
			err = s.Provider.Close()
		}
	})
	return err
}
func (s *AlertStore) FinalClose() error {
	if s.Durable != nil {
		return s.Durable.Close()
	}
	return nil
}

func NewAlertStore(provider string) *AlertStore {

	as := &AlertStore{}

	if provider == providerMemory {
		as.Provider = memory.NewProvider()
	}

	return as
}

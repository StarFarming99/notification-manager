package jevshadow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"golang.org/x/sys/unix"
)

const (
	cardStateVersion       = 1
	maxCardStateFileSize   = 32 * 1024 * 1024
	baseSendConfirmed      = "confirmed"
	patchSendNone          = "none"
	patchSendSending       = "sending"
	patchSendConfirmed     = "confirmed"
	patchSendFailed        = "failed"
	patchSendUnknown       = "unknown"
	maxCardStateErrorBytes = 1024
)

var (
	ErrCardWriterBusy        = errors.New("card writer is busy")
	ErrCardWriterUnavailable = errors.New("card writer is unavailable")
	ErrPatchStateUnknown     = errors.New("card patch result is unknown and requires reconciliation")
)

type persistedAppliedRequest struct {
	PayloadHash string `json:"payload_hash"`
	RequestID   string `json:"request_id"`
}

type cardPatchIntent struct {
	IdempotencyKey     string                 `json:"idempotency_key"`
	PayloadHash        string                 `json:"payload_hash"`
	RequestID          string                 `json:"request_id"`
	ComponentKey       string                 `json:"component_key"`
	Component          AnnotationComponent    `json:"component"`
	RenderedCard       map[string]interface{} `json:"rendered_card"`
	RenderedCardHash   string                 `json:"rendered_card_hash"`
	AnnotationRevision int                    `json:"annotation_revision"`
	StartedAt          time.Time              `json:"started_at"`
}

type cardPersistentState struct {
	Version            int                                `json:"version"`
	MessageID          string                             `json:"message_id"`
	Receiver           string                             `json:"receiver"`
	Destination        string                             `json:"destination"`
	SenderApp          string                             `json:"sender_app"`
	BaseCard           map[string]interface{}             `json:"base_card"`
	BaseRevision       int                                `json:"base_revision"`
	BaseSendState      string                             `json:"base_send_state"`
	AnnotationRevision int                                `json:"annotation_revision"`
	Annotations        map[string]AnnotationComponent     `json:"annotations"`
	AppliedKeys        map[string]persistedAppliedRequest `json:"applied_keys"`
	PatchSendState     string                             `json:"patch_send_state"`
	PendingPatch       *cardPatchIntent                   `json:"pending_patch,omitempty"`
	LastPatchError     string                             `json:"last_patch_error,omitempty"`
	CreatedAt          time.Time                          `json:"created_at"`
	UpdatedAt          time.Time                          `json:"updated_at"`
}

type cardStateStore struct {
	logger   log.Logger
	rootDir  string
	stateDir string
	deadDir  string
	lockDir  string
}

func newCardStateStore(logger log.Logger, rootDir string) (*cardStateStore, error) {
	rootDir = filepath.Clean(rootDir)
	store := &cardStateStore{
		logger:   logger,
		rootDir:  rootDir,
		stateDir: filepath.Join(rootDir, "bindings"),
		deadDir:  filepath.Join(rootDir, "dead-letter"),
		lockDir:  filepath.Join(rootDir, "locks"),
	}
	for _, directory := range []string{store.rootDir, store.stateDir, store.deadDir, store.lockDir} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create card state directory %s: %w", directory, err)
		}
	}
	if err := removeCardStateTemporaryFiles(store.stateDir); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *cardStateStore) WithMessageLock(messageID string, operation func() error) error {
	lockPath := filepath.Join(s.lockDir, cardStateKey(messageID)+".lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("%w: open card writer lock: %v", ErrCardWriterUnavailable, err)
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return ErrCardWriterBusy
		}
		return fmt.Errorf("%w: lock card writer: %v", ErrCardWriterUnavailable, err)
	}
	defer func() { _ = unix.Flock(int(lock.Fd()), unix.LOCK_UN) }()
	return operation()
}

func (s *cardStateStore) Save(state *cardPersistentState) error {
	state.UpdatedAt = time.Now().UTC()
	if err := validateCardPersistentState(*state); err != nil {
		return err
	}
	return writeCardStateFile(s.path(state.MessageID), *state)
}

func (s *cardStateStore) Load(messageID string) (cardPersistentState, error) {
	state, err := readCardStateFile(s.path(messageID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cardPersistentState{}, ErrCardNotFound
		}
		return cardPersistentState{}, err
	}
	if state.MessageID != messageID {
		return cardPersistentState{}, errors.New("card state message identity mismatch")
	}
	return state, nil
}

func (s *cardStateStore) LoadRecent(limit int) []cardPersistentState {
	entries, err := os.ReadDir(s.stateDir)
	if err != nil {
		_ = level.Error(s.logger).Log("msg", "Jev shadow card state scan failed", "error", err)
		return nil
	}
	states := make([]cardPersistentState, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(s.stateDir, entry.Name())
		state, readErr := readCardStateFile(path)
		if readErr != nil || entry.Name() != cardStateKey(state.MessageID)+".json" {
			reason := readErr
			if reason == nil {
				reason = errors.New("card state filename identity mismatch")
			}
			if quarantineErr := s.quarantine(path, reason); quarantineErr != nil {
				_ = level.Error(s.logger).Log("msg", "Jev shadow corrupt card state could not be quarantined", "path", path, "error", quarantineErr)
			} else {
				_ = level.Error(s.logger).Log("msg", "Jev shadow corrupt card state quarantined", "path", path, "error", reason)
			}
			continue
		}
		states = append(states, state)
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].UpdatedAt.After(states[j].UpdatedAt)
	})
	if limit > 0 && len(states) > limit {
		states = states[:limit]
	}
	return states
}

func (s *cardStateStore) path(messageID string) string {
	return filepath.Join(s.stateDir, cardStateKey(messageID)+".json")
}

func (s *cardStateStore) quarantine(path string, reason error) error {
	base := strings.TrimSuffix(filepath.Base(path), ".json")
	destination := filepath.Join(s.deadDir, fmt.Sprintf("%s-corrupt-%d.json", base, time.Now().UTC().UnixNano()))
	if err := os.Rename(path, destination); err != nil {
		return err
	}
	if err := syncDirectory(s.stateDir); err != nil {
		return err
	}
	if err := syncDirectory(s.deadDir); err != nil {
		return err
	}
	return os.WriteFile(destination+".error", []byte(boundedCardStateError(reason)+"\n"), 0o600)
}

func cardStateKey(messageID string) string {
	digest := sha256.Sum256([]byte(messageID))
	return hex.EncodeToString(digest[:])
}

func writeCardStateFile(path string, state cardPersistentState) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".card-state-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	encoder := json.NewEncoder(temporary)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(state); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func readCardStateFile(path string) (cardPersistentState, error) {
	file, err := os.Open(path)
	if err != nil {
		return cardPersistentState{}, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxCardStateFileSize+1))
	decoder.DisallowUnknownFields()
	var state cardPersistentState
	if err := decoder.Decode(&state); err != nil {
		return cardPersistentState{}, err
	}
	if err := ensureEOF(decoder); err != nil {
		return cardPersistentState{}, err
	}
	if err := validateCardPersistentState(state); err != nil {
		return cardPersistentState{}, err
	}
	return state, nil
}

func validateCardPersistentState(state cardPersistentState) error {
	if state.Version != cardStateVersion || strings.TrimSpace(state.MessageID) == "" {
		return errors.New("invalid card state identity")
	}
	if state.Receiver == "" || state.Destination == "" || state.SenderApp == "" {
		return errors.New("card state is missing writer identity")
	}
	if state.BaseRevision < 1 || state.AnnotationRevision < 0 || state.BaseSendState != baseSendConfirmed {
		return errors.New("invalid card state revision or base send state")
	}
	if state.CreatedAt.IsZero() || state.UpdatedAt.IsZero() || state.UpdatedAt.Before(state.CreatedAt) {
		return errors.New("invalid card state timestamps")
	}
	if _, ok := state.BaseCard["elements"].([]interface{}); !ok {
		return errors.New("persisted base card has no elements array")
	}
	if state.Annotations == nil || state.AppliedKeys == nil {
		return errors.New("persisted card state maps are missing")
	}
	allowedPatchStates := map[string]struct{}{
		patchSendNone: {}, patchSendSending: {}, patchSendConfirmed: {}, patchSendFailed: {}, patchSendUnknown: {},
	}
	if _, ok := allowedPatchStates[state.PatchSendState]; !ok {
		return errors.New("invalid card patch send state")
	}
	if (state.PatchSendState == patchSendSending || state.PatchSendState == patchSendUnknown) && state.PendingPatch == nil {
		return errors.New("ambiguous card patch state is missing its intent")
	}
	if state.PatchSendState == patchSendConfirmed && state.AnnotationRevision == 0 {
		return errors.New("confirmed card patch state has no annotation revision")
	}
	for key, component := range state.Annotations {
		if err := component.validate(); err != nil {
			return fmt.Errorf("invalid persisted annotation %s: %w", key, err)
		}
		if key != stableComponentKey(component.MemberEventIDs) {
			return errors.New("persisted annotation component key mismatch")
		}
		if component.AnnotationRevision > state.AnnotationRevision || component.ExpectedBaseRevision != state.BaseRevision {
			return errors.New("persisted annotation revision exceeds card state")
		}
	}
	for idempotencyKey, applied := range state.AppliedKeys {
		if len(idempotencyKey) < 8 || len(idempotencyKey) > 256 || len(applied.PayloadHash) != sha256.Size*2 {
			return errors.New("invalid persisted idempotency record")
		}
		if _, err := hex.DecodeString(applied.PayloadHash); err != nil {
			return errors.New("invalid persisted idempotency payload hash")
		}
		if applied.RequestID != requestID(state.MessageID, idempotencyKey) {
			return errors.New("persisted idempotency request identity mismatch")
		}
	}
	if state.PendingPatch != nil {
		pending := state.PendingPatch
		if state.PatchSendState != patchSendSending && state.PatchSendState != patchSendUnknown {
			return errors.New("pending patch has a non-ambiguous send state")
		}
		if len(pending.IdempotencyKey) < 8 || len(pending.IdempotencyKey) > 256 || pending.PayloadHash == "" || pending.RequestID == "" || pending.StartedAt.IsZero() {
			return errors.New("invalid pending card patch")
		}
		if err := pending.Component.validate(); err != nil {
			return fmt.Errorf("invalid pending card patch component: %w", err)
		}
		componentHash, err := hashCanonical(pending.Component)
		if err != nil || componentHash != pending.PayloadHash {
			return errors.New("pending card patch payload hash mismatch")
		}
		if pending.RequestID != requestID(state.MessageID, pending.IdempotencyKey) {
			return errors.New("pending card patch request identity mismatch")
		}
		if pending.ComponentKey != stableComponentKey(pending.Component.MemberEventIDs) {
			return errors.New("pending card patch component identity mismatch")
		}
		if pending.AnnotationRevision != pending.Component.AnnotationRevision || pending.AnnotationRevision != state.AnnotationRevision+1 {
			return errors.New("pending card patch revision mismatch")
		}
		renderedHash, err := hashCanonical(pending.RenderedCard)
		if err != nil || renderedHash != pending.RenderedCardHash {
			return errors.New("pending rendered card hash mismatch")
		}
		annotations := make(map[string]AnnotationComponent, len(state.Annotations)+1)
		for key, component := range state.Annotations {
			annotations[key] = component
		}
		annotations[pending.ComponentKey] = pending.Component
		expectedCard, err := renderCard(state.BaseCard, annotations)
		if err != nil {
			return errors.New("pending rendered card cannot be rebuilt")
		}
		expectedHash, err := hashCanonical(expectedCard)
		if err != nil || expectedHash != pending.RenderedCardHash {
			return errors.New("pending rendered card does not match persisted state")
		}
	}
	if len(state.LastPatchError) > maxCardStateErrorBytes {
		return errors.New("persisted card patch error is too large")
	}
	return nil
}

func removeCardStateTemporaryFiles(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".card-state-") || !strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		if err := os.Remove(filepath.Join(directory, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func boundedCardStateError(err error) string {
	message := strings.TrimSpace(err.Error())
	if len(message) > maxCardStateErrorBytes {
		return message[:maxCardStateErrorBytes]
	}
	return message
}

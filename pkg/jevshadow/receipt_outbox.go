package jevshadow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

const (
	receiptEnvelopeVersion = 1
	receiptRequestTimeout  = 3 * time.Second
	receiptIdlePoll        = 30 * time.Second
	maxReceiptErrorBytes   = 1024
	maxReceiptEnvelopeSize = 16 * 1024 * 1024
)

var errReceiptOutboxClosing = errors.New("Jev shadow receipt outbox is closing")

type receiptEnvelope struct {
	Version        int             `json:"version"`
	ID             string          `json:"id"`
	Receipt        deliveryReceipt `json:"receipt"`
	CreatedAt      time.Time       `json:"created_at"`
	Attempts       int             `json:"attempts"`
	NextAttemptAt  time.Time       `json:"next_attempt_at"`
	LastError      string          `json:"last_error,omitempty"`
	DeadLetteredAt *time.Time      `json:"dead_lettered_at,omitempty"`
}

type receiptDeliveryFailure struct {
	err       error
	permanent bool
}

func (e *receiptDeliveryFailure) Error() string {
	return e.err.Error()
}

func (e *receiptDeliveryFailure) Unwrap() error {
	return e.err
}

type receiptOutbox struct {
	logger      log.Logger
	deliver     func(context.Context, deliveryReceipt) error
	pendingDir  string
	deadDir     string
	maxAttempts int
	retryMin    time.Duration
	retryMax    time.Duration
	drain       time.Duration

	mu      sync.Mutex
	closing bool
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	close   sync.Once
}

func newReceiptOutbox(
	logger log.Logger,
	config Config,
	deliver func(context.Context, deliveryReceipt) error,
) (*receiptOutbox, error) {
	if deliver == nil {
		return nil, errors.New("receipt outbox delivery function is required")
	}
	rootDir := filepath.Clean(config.ReceiptOutboxDir)
	pendingDir := filepath.Join(rootDir, "pending")
	deadDir := filepath.Join(rootDir, "dead-letter")
	for _, directory := range []string{rootDir, pendingDir, deadDir} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create receipt outbox directory %s: %w", directory, err)
		}
	}
	if err := removeAbandonedTemporaryFiles(pendingDir); err != nil {
		return nil, err
	}

	outbox := &receiptOutbox{
		logger:      logger,
		deliver:     deliver,
		pendingDir:  pendingDir,
		deadDir:     deadDir,
		maxAttempts: config.ReceiptMaxAttempts,
		retryMin:    config.ReceiptRetryMin,
		retryMax:    config.ReceiptRetryMax,
		drain:       config.ReceiptDrainTimeout,
		wake:        make(chan struct{}, 1),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	go outbox.run()
	return outbox, nil
}

// Enqueue durably records a receipt before waking the asynchronous sender. It
// intentionally performs only local filesystem work: a slow or unavailable
// Jev endpoint never changes the result of the already-successful notification.
func (o *receiptOutbox) Enqueue(receipt deliveryReceipt) error {
	envelope := receiptEnvelope{
		Version:       receiptEnvelopeVersion,
		ID:            receiptEnvelopeID(receipt),
		Receipt:       receipt,
		CreatedAt:     time.Now().UTC(),
		NextAttemptAt: time.Now().UTC(),
	}
	path := o.pendingPath(envelope.ID)

	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closing {
		return errReceiptOutboxClosing
	}
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect receipt outbox entry: %w", err)
	}
	if err := writeReceiptEnvelope(path, envelope); err != nil {
		return fmt.Errorf("persist receipt outbox entry: %w", err)
	}
	o.signal()
	return nil
}

func (o *receiptOutbox) Close() {
	if o == nil {
		return
	}
	o.close.Do(func() {
		o.mu.Lock()
		o.closing = true
		close(o.stop)
		o.mu.Unlock()
		<-o.done
	})
}

func (o *receiptOutbox) run() {
	defer close(o.done)
	for {
		select {
		case <-o.stop:
			o.drainPending()
			return
		default:
		}

		processed, wait := o.processNext(context.Background())
		if processed {
			continue
		}
		if wait <= 0 || wait > receiptIdlePoll {
			wait = receiptIdlePoll
		}
		timer := time.NewTimer(wait)
		select {
		case <-o.stop:
			if !timer.Stop() {
				<-timer.C
			}
			o.drainPending()
			return
		case <-o.wake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

func (o *receiptOutbox) processNext(ctx context.Context) (bool, time.Duration) {
	path, envelope, wait, found := o.nextEnvelope()
	if !found {
		return false, wait
	}
	if wait > 0 {
		return false, wait
	}
	o.deliverEnvelope(ctx, path, envelope)
	return true, 0
}

func (o *receiptOutbox) deliverEnvelope(ctx context.Context, path string, envelope receiptEnvelope) {
	err := o.deliver(ctx, envelope.Receipt)
	if err == nil {
		if removeErr := o.removePending(path); removeErr != nil {
			_ = level.Error(o.logger).Log(
				"msg", "Jev shadow could not acknowledge a delivered receipt; idempotent retry will continue",
				"receiptID", envelope.ID,
				"error", removeErr,
			)
		}
		return
	}

	permanent := false
	var failure *receiptDeliveryFailure
	if errors.As(err, &failure) {
		permanent = failure.permanent
	}
	envelope.Attempts++
	envelope.LastError = boundedReceiptError(err)
	if permanent || envelope.Attempts >= o.maxAttempts {
		if deadErr := o.moveToDeadLetter(path, envelope); deadErr != nil {
			_ = level.Error(o.logger).Log(
				"msg", "Jev shadow receipt failed and could not be persisted to dead-letter",
				"receiptID", envelope.ID,
				"error", deadErr,
			)
			return
		}
		_ = level.Error(o.logger).Log(
			"msg", "Jev shadow receipt moved to dead-letter",
			"receiptID", envelope.ID,
			"attempts", envelope.Attempts,
			"permanent", permanent,
			"error", envelope.LastError,
		)
		return
	}

	envelope.NextAttemptAt = time.Now().UTC().Add(o.retryDelay(envelope.Attempts))
	if writeErr := o.replacePending(path, envelope); writeErr != nil {
		_ = level.Error(o.logger).Log(
			"msg", "Jev shadow receipt retry state could not be persisted; original entry retained",
			"receiptID", envelope.ID,
			"error", writeErr,
		)
		return
	}
	_ = level.Warn(o.logger).Log(
		"msg", "Jev shadow receipt delivery will retry from durable outbox",
		"receiptID", envelope.ID,
		"attempts", envelope.Attempts,
		"nextAttemptAt", envelope.NextAttemptAt,
		"error", envelope.LastError,
	)
}

func (o *receiptOutbox) nextEnvelope() (string, receiptEnvelope, time.Duration, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	entries, err := os.ReadDir(o.pendingDir)
	if err != nil {
		_ = level.Error(o.logger).Log("msg", "Jev shadow receipt outbox scan failed", "error", err)
		return "", receiptEnvelope{}, o.retryMin, false
	}
	now := time.Now().UTC()
	type candidate struct {
		path     string
		envelope receiptEnvelope
	}
	candidates := make([]candidate, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(o.pendingDir, entry.Name())
		envelope, readErr := readReceiptEnvelope(path)
		if readErr != nil {
			reason := readErr
			if moveErr := o.moveCorruptToDeadLetterLocked(path, reason); moveErr != nil {
				_ = level.Error(o.logger).Log("msg", "Jev shadow corrupt receipt outbox entry could not be quarantined", "path", path, "error", moveErr)
			} else {
				_ = level.Error(o.logger).Log("msg", "Jev shadow corrupt receipt outbox entry quarantined", "path", path, "error", reason)
			}
			continue
		}
		candidates = append(candidates, candidate{path: path, envelope: envelope})
	}
	if len(candidates) == 0 {
		return "", receiptEnvelope{}, receiptIdlePoll, false
	}
	sort.Slice(candidates, func(i, j int) bool {
		left := candidates[i].envelope.NextAttemptAt
		right := candidates[j].envelope.NextAttemptAt
		if left.Equal(right) {
			return candidates[i].envelope.CreatedAt.Before(candidates[j].envelope.CreatedAt)
		}
		return left.Before(right)
	})
	selected := candidates[0]
	wait := selected.envelope.NextAttemptAt.Sub(now)
	return selected.path, selected.envelope, wait, true
}

func (o *receiptOutbox) drainPending() {
	ctx, cancel := context.WithTimeout(context.Background(), o.drain)
	defer cancel()

	o.mu.Lock()
	entries, err := os.ReadDir(o.pendingDir)
	o.mu.Unlock()
	if err != nil {
		_ = level.Error(o.logger).Log("msg", "Jev shadow receipt outbox drain scan failed", "error", err)
		return
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			paths = append(paths, filepath.Join(o.pendingDir, entry.Name()))
		}
	}
	sort.Strings(paths)
	for _, path := range paths {
		if ctx.Err() != nil {
			_ = level.Warn(o.logger).Log("msg", "Jev shadow receipt outbox drain deadline reached; pending entries remain on disk")
			return
		}
		envelope, readErr := readReceiptEnvelope(path)
		if readErr != nil {
			o.mu.Lock()
			moveErr := o.moveCorruptToDeadLetterLocked(path, readErr)
			o.mu.Unlock()
			if moveErr != nil {
				_ = level.Error(o.logger).Log("msg", "Jev shadow corrupt receipt could not be quarantined while draining", "path", path, "error", moveErr)
			}
			continue
		}
		o.deliverEnvelope(ctx, path, envelope)
	}
}

func (o *receiptOutbox) retryDelay(attempt int) time.Duration {
	delay := o.retryMin
	for index := 1; index < attempt && delay < o.retryMax; index++ {
		if delay > o.retryMax/2 {
			return o.retryMax
		}
		delay *= 2
	}
	if delay > o.retryMax {
		return o.retryMax
	}
	return delay
}

func (o *receiptOutbox) pendingPath(id string) string {
	return filepath.Join(o.pendingDir, id+".json")
}

func (o *receiptOutbox) replacePending(path string, envelope receiptEnvelope) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return writeReceiptEnvelope(path, envelope)
}

func (o *receiptOutbox) removePending(path string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(o.pendingDir)
}

func (o *receiptOutbox) moveToDeadLetter(path string, envelope receiptEnvelope) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := time.Now().UTC()
	envelope.DeadLetteredAt = &now
	if err := writeReceiptEnvelope(path, envelope); err != nil {
		return err
	}
	deadPath := filepath.Join(o.deadDir, fmt.Sprintf("%s-%d.json", envelope.ID, now.UnixNano()))
	if err := os.Rename(path, deadPath); err != nil {
		return err
	}
	if err := syncDirectory(o.pendingDir); err != nil {
		return err
	}
	return syncDirectory(o.deadDir)
}

func (o *receiptOutbox) moveCorruptToDeadLetterLocked(path string, reason error) error {
	name := strings.TrimSuffix(filepath.Base(path), ".json")
	deadPath := filepath.Join(o.deadDir, fmt.Sprintf("%s-corrupt-%d.json", name, time.Now().UTC().UnixNano()))
	if err := os.Rename(path, deadPath); err != nil {
		return err
	}
	if err := syncDirectory(o.pendingDir); err != nil {
		return err
	}
	if err := syncDirectory(o.deadDir); err != nil {
		return err
	}
	reasonPath := deadPath + ".error"
	return os.WriteFile(reasonPath, []byte(boundedReceiptError(reason)+"\n"), 0o600)
}

func (o *receiptOutbox) signal() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func receiptEnvelopeID(receipt deliveryReceipt) string {
	digest := sha256.Sum256([]byte(receipt.DeliveryID + "\x00" + receipt.MessageID))
	return hex.EncodeToString(digest[:])
}

func writeReceiptEnvelope(path string, envelope receiptEnvelope) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".receipt-*.tmp")
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
	if err := encoder.Encode(envelope); err != nil {
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

func readReceiptEnvelope(path string) (receiptEnvelope, error) {
	file, err := os.Open(path)
	if err != nil {
		return receiptEnvelope{}, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxReceiptEnvelopeSize+1))
	decoder.DisallowUnknownFields()
	var envelope receiptEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return receiptEnvelope{}, err
	}
	if err := ensureEOF(decoder); err != nil {
		return receiptEnvelope{}, err
	}
	if err := validateReceiptEnvelope(envelope); err != nil {
		return receiptEnvelope{}, err
	}
	return envelope, nil
}

func validateReceiptEnvelope(envelope receiptEnvelope) error {
	if envelope.Version != receiptEnvelopeVersion {
		return fmt.Errorf("unsupported receipt outbox envelope version %d", envelope.Version)
	}
	if len(envelope.ID) != sha256.Size*2 {
		return errors.New("invalid receipt outbox envelope ID")
	}
	if _, err := hex.DecodeString(envelope.ID); err != nil {
		return errors.New("invalid receipt outbox envelope ID")
	}
	if envelope.Receipt.DeliveryID == "" || envelope.Receipt.MessageID == "" {
		return errors.New("receipt outbox envelope is missing delivery identity")
	}
	if envelope.ID != receiptEnvelopeID(envelope.Receipt) {
		return errors.New("receipt outbox envelope identity mismatch")
	}
	if envelope.Attempts < 0 || envelope.CreatedAt.IsZero() {
		return errors.New("invalid receipt outbox envelope state")
	}
	return nil
}

func removeAbandonedTemporaryFiles(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return fmt.Errorf("scan receipt outbox directory %s: %w", directory, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".receipt-") || !strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		if err := os.Remove(filepath.Join(directory, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove abandoned receipt outbox temporary file: %w", err)
		}
	}
	return nil
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func boundedReceiptError(err error) string {
	message := strings.TrimSpace(err.Error())
	if len(message) > maxReceiptErrorBytes {
		return message[:maxReceiptErrorBytes]
	}
	return message
}

func (s *Service) postDeliveryReceipt(parent context.Context, receipt deliveryReceipt) error {
	body, err := newRequestBody(receipt)
	if err != nil {
		return &receiptDeliveryFailure{err: fmt.Errorf("encode delivery receipt: %w", err), permanent: true}
	}
	ctx, cancel := context.WithTimeout(parent, receiptRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.config.ReceiptURL, body)
	if err != nil {
		return &receiptDeliveryFailure{err: fmt.Errorf("create delivery receipt request: %w", err), permanent: true}
	}
	request.Header.Set("Authorization", "Bearer "+s.config.Token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", receipt.DeliveryID+":"+receipt.MessageID)
	response, err := s.client.Do(request)
	if err != nil {
		return &receiptDeliveryFailure{err: err}
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	permanent := response.StatusCode >= http.StatusBadRequest && response.StatusCode < http.StatusInternalServerError &&
		response.StatusCode != http.StatusRequestTimeout &&
		response.StatusCode != http.StatusConflict &&
		response.StatusCode != http.StatusTooEarly &&
		response.StatusCode != http.StatusTooManyRequests
	return &receiptDeliveryFailure{
		err:       fmt.Errorf("receipt endpoint returned %d", response.StatusCode),
		permanent: permanent,
	}
}

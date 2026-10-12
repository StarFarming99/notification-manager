package v1

import (
	"context"
	"errors"

	"github.com/go-kit/kit/log/level"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
)

type admissionFailure struct {
	stage string
	err   error
}

func (e *admissionFailure) Error() string { return "notification admission failed: " + e.stage }
func (e *admissionFailure) Unwrap() error { return e.err }
func (h *HttpHandler) recordAdmissionFailure(err error, groupHash string) {
	class := "spool_commit"
	var typed *admissionFailure
	if errors.As(err, &typed) {
		class = typed.stage
	}
	if errors.Is(err, spool.ErrCapacity) {
		class = "spool_capacity"
	}
	if (errors.Is(err, spool.ErrProfilePaused) || errors.Is(err, spool.ErrProfileConflict)) && class != "test_profile_not_accepting" && class != "formal_profile_not_accepting" {
		class = "profile_changed_or_paused"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		class = "admission_deadline"
	}
	h.admissionMu.Lock()
	if h.admissionFailures == nil {
		h.admissionFailures = map[string]uint64{}
	}
	h.admissionFailures[class]++
	h.admissionMu.Unlock()
	// The reason is a fixed code and the group is a one-way identity digest.
	// Never log receiver payload, credential values or arbitrary error text.
	_ = level.Error(h.logger).Log("msg", "notification admission rejected", "class", class, "group_hash", groupHash)
}

func (h *HttpHandler) admissionFailureSnapshot() map[string]uint64 {
	h.admissionMu.Lock()
	defer h.admissionMu.Unlock()
	result := map[string]uint64{}
	for key, count := range h.admissionFailures {
		result[key] = count
	}
	return result
}
func (h *HttpHandler) RecordProfileAdmissionFailure(profileID string) {
	class := "formal_profile_not_accepting"
	if profileID == "test" {
		class = "test_profile_not_accepting"
	}
	h.recordAdmissionFailure(&admissionFailure{stage: class, err: spool.ErrProfilePaused}, "")
}

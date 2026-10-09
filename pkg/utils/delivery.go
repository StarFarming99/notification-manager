package utils

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
)

type deliveryKey struct{}
type DeliveryAttempt struct{ started uint32 }

func TrackDelivery(ctx context.Context) (context.Context, *DeliveryAttempt) {
	a := &DeliveryAttempt{}
	return context.WithValue(ctx, deliveryKey{}, a), a
}
func (a *DeliveryAttempt) Started() bool { return atomic.LoadUint32(&a.started) != 0 }
func CredentialContext(ctx context.Context) context.Context {
	// Acquiring a token cannot send the frozen notification.
	return context.WithValue(ctx, deliveryKey{}, (*DeliveryAttempt)(nil))
}
func markDelivery(ctx context.Context) {
	if a, _ := ctx.Value(deliveryKey{}).(*DeliveryAttempt); a != nil {
		atomic.StoreUint32(&a.started, 1)
	}
}

type PreSendError struct{ Err error }

func (e *PreSendError) Error() string { return "notification not sent: preflight unavailable" }
func (e *PreSendError) Unwrap() error { return e.Err }

// A parsed platform rejection proves there was no accepted notification.
type PlatformRejection struct{ Code int }

func (e *PlatformRejection) Error() string {
	return fmt.Sprintf("notification platform rejected code %d", e.Code)
}

// DeliveryOutcome is shared by the dispatcher and the audited recovery tool.
// Transport/response loss and HTTP 5xx remain unknown: retry may duplicate a card.
func DeliveryOutcome(err error) (string, string) {
	if err == nil {
		return "delivered", "platform_confirmed"
	}
	var pre *PreSendError
	if errors.As(err, &pre) {
		return "retryable", "notification_not_sent"
	}
	var rejection *PlatformRejection
	if errors.As(err, &rejection) {
		return "retryable", "platform_explicit_rejection"
	}
	var status *HTTPStatusError
	if errors.As(err, &status) {
		switch status.Status {
		case http.StatusTooManyRequests, http.StatusUnauthorized, http.StatusForbidden:
			return "retryable", "platform_rejected_recoverable"
		case http.StatusBadRequest, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
			return "dead_letter", "platform_rejected_invalid_request"
		}
	}
	return "unknown", "platform_result_uncertain"
}

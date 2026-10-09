package utils

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDeliveryFailureClasses(t *testing.T) {
	for _, test := range []struct {
		err   error
		state string
	}{
		{nil, "delivered"}, {&PreSendError{Err: errors.New("secret unavailable")}, "retryable"},
		{&PlatformRejection{Code: 99991663}, "retryable"}, {&HTTPStatusError{Status: 429}, "retryable"},
		{&HTTPStatusError{Status: 401}, "retryable"}, {&HTTPStatusError{Status: 400}, "dead_letter"},
		{&HTTPStatusError{Status: 503}, "unknown"}, {context.DeadlineExceeded, "unknown"},
	} {
		if got, _ := DeliveryOutcome(test.err); got != test.state {
			t.Fatalf("%v: %s", test.err, got)
		}
	}
}

func TestNilClientRetainsCallerBudgetBeyondFiveSeconds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(6 * time.Second); w.WriteHeader(200) }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request, _ := http.NewRequest(http.MethodPost, server.URL, nil)
	if _, err := DoHttpRequest(ctx, nil, request); err != nil {
		t.Fatalf("sixth-second acknowledgement lost: %v", err)
	}
}

package utils

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

type unsafeURLTransport func(*http.Request) (*http.Response, error)

func (f unsafeURLTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTransportErrorDoesNotExposeCredentialURL(t *testing.T) {
	cause := errors.New("synthetic transport unavailable")
	client := &http.Client{Transport: unsafeURLTransport(func(*http.Request) (*http.Response, error) { return nil, cause })}
	request, _ := http.NewRequest("POST", "https://synthetic-user:synthetic-password@mock.invalid/hook?access_token=synthetic-token", nil)
	_, err := DoHttpRequest(context.Background(), client, request)
	if err == nil || !errors.Is(err, cause) || strings.Contains(err.Error(), "synthetic-") || strings.Contains(err.Error(), "mock.invalid") {
		t.Fatal("transport error leaked URL or lost identity", err)
	}
}

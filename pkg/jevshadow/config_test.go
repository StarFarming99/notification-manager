package jevshadow

import (
	"strings"
	"testing"
)

func setRequiredConfigEnv(t *testing.T) {
	t.Helper()
	t.Setenv(envEnabled, "true")
	t.Setenv(envReceiptURL, "http://jev-alert-shadow.example/v1/delivery-receipts")
	t.Setenv(envToken, "test-token")
	t.Setenv(envReceiverAllowlist, "jev-shadow-uat-receiver")
	t.Setenv(envDestinationAllowlist, "oc_test")
	t.Setenv(envLogicalSource, "kubesphere-notification-manager")
	t.Setenv(envEnvironment, "uat")
	t.Setenv(envSourceRegion, "us-west-2")
	t.Setenv(envPermissionDomain, "sre-uat")
	t.Setenv(envObservationReceiver, "jev-shadow-uat-receiver")
	t.Setenv(envSenderApp, "infra-alerts")
	t.Setenv(envFeedbackEnabled, "true")
}

func TestConfigFromEnvAllowsNoExpiry(t *testing.T) {
	setRequiredConfigEnv(t)
	t.Setenv(envExpiresAt, "")

	config, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if !config.ExpiresAt.IsZero() {
		t.Fatalf("ExpiresAt = %v, want zero time", config.ExpiresAt)
	}
}

func TestConfigFromEnvRejectsInvalidOptionalExpiry(t *testing.T) {
	setRequiredConfigEnv(t)
	t.Setenv(envExpiresAt, "not-a-timestamp")

	_, err := ConfigFromEnv()
	if err == nil || !strings.Contains(err.Error(), envExpiresAt+" must be RFC3339") {
		t.Fatalf("ConfigFromEnv() error = %v, want RFC3339 validation error", err)
	}
}

func TestConfigFromEnvAnnotationProfilesRequireInstalledVersion(t *testing.T) {
	setRequiredConfigEnv(t)
	installedScopes(t)
	t.Setenv(envAnnotationProfiles, "test:v1")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.AnnotationProfiles["test:v1"]; !ok || len(cfg.AnnotationProfiles) != 1 {
		t.Fatal("explicit annotation profile not loaded", cfg.AnnotationProfiles)
	}
	t.Setenv(envAnnotationProfiles, "test:typo")
	if _, err := ConfigFromEnv(); err == nil || !strings.Contains(err.Error(), envAnnotationProfiles) {
		t.Fatal("uninstalled activation accepted", err)
	}
}

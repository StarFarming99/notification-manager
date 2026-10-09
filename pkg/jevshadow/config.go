package jevshadow

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	envEnabled              = "JEV_SHADOW_ENABLED"
	envReceiptURL           = "JEV_SHADOW_RECEIPT_URL"
	envToken                = "JEV_SHADOW_TOKEN"
	envReceiverAllowlist    = "JEV_SHADOW_RECEIVER_ALLOWLIST"
	envDestinationAllowlist = "JEV_SHADOW_DESTINATION_ALLOWLIST"
	envLogicalSource        = "JEV_SHADOW_LOGICAL_SOURCE"
	envEnvironment          = "JEV_SHADOW_ENVIRONMENT"
	envSourceRegion         = "JEV_SHADOW_SOURCE_REGION"
	envPermissionDomain     = "JEV_SHADOW_PERMISSION_DOMAIN"
	envObservationReceiver  = "JEV_SHADOW_OBSERVATION_RECEIVER"
	envSenderApp            = "JEV_SHADOW_SENDER_APP"
	envExpiresAt            = "JEV_SHADOW_EXPIRES_AT"
	envReceiptOutboxDir     = "JEV_SHADOW_RECEIPT_OUTBOX_DIR"
	envReceiptMaxAttempts   = "JEV_SHADOW_RECEIPT_MAX_ATTEMPTS"
	envReceiptRetryMin      = "JEV_SHADOW_RECEIPT_RETRY_MIN"
	envReceiptRetryMax      = "JEV_SHADOW_RECEIPT_RETRY_MAX"
	envReceiptDrainTimeout  = "JEV_SHADOW_RECEIPT_DRAIN_TIMEOUT"
	envCardStateDir         = "JEV_SHADOW_CARD_STATE_DIR"
	envFeedbackEnabled      = "JEV_SHADOW_FEEDBACK_ENABLED"
)

type Config struct {
	DeliveryScopes                            map[string]DeliveryScope
	expectedProfileID, expectedProfileVersion string
	CardOwnerID                               string
	ExecutionDomain                           string
	Enabled                                   bool
	ReceiptURL                                string
	ObservationURL                            string
	ExcludedIdentityLabels                    map[string]struct{}
	Token                                     string
	AnnotationToken                           string
	FeedbackToken                             string
	ReceiverAllowlist                         map[string]struct{}
	DestinationAllowlist                      map[string]struct{}
	LogicalSource                             string
	Environment                               string
	SourceRegion                              string
	PermissionDomain                          string
	ObservationReceiver                       string
	SenderApp                                 string
	ExpiresAt                                 time.Time
	ReceiptOutboxDir                          string
	ReceiptMaxAttempts                        int
	ReceiptRetryMin                           time.Duration
	ReceiptRetryMax                           time.Duration
	ReceiptDrainTimeout                       time.Duration
	CardStateDir                              string
	FeedbackEnabled                           bool
	ExistingCallbackIntegrated                bool
	MaxCards                                  int
}

func ConfigFromEnv() (Config, error) {
	enabled, err := strconv.ParseBool(valueOrDefault(envEnabled, "false"))
	if err != nil {
		return Config{}, fmt.Errorf("%s must be a boolean: %w", envEnabled, err)
	}
	config := Config{Enabled: enabled, MaxCards: 2000, CardOwnerID: os.Getenv("JEV_CARD_OWNER_ID"), ExecutionDomain: os.Getenv("JEV_EXECUTION_DOMAIN")}
	if !enabled {
		return config.withReceiptDefaults(), nil
	}

	config.ExistingCallbackIntegrated = os.Getenv("JEV_EXISTING_CALLBACK_INTEGRATED") == "true"
	config.ReceiptURL = strings.TrimSpace(os.Getenv(envReceiptURL))
	config.ObservationURL = strings.TrimSpace(os.Getenv("JEV_SHADOW_OBSERVATION_URL"))
	config.ExcludedIdentityLabels = csvSet(os.Getenv("JEV_SHADOW_IDENTITY_EXCLUDED_LABELS"))
	for key := range config.ExcludedIdentityLabels {
		if key == "cluster" || key == "namespace" || key == "tenant" || key == "tenant_id" || key == "instance" || key == "pod" || key == "node" || key == "alertname" {
			return Config{}, fmt.Errorf("identity resource label cannot be excluded")
		}
	}
	config.Token = strings.TrimSpace(os.Getenv(envToken))
	config.AnnotationToken = strings.TrimSpace(os.Getenv("JEV_SHADOW_ANNOTATION_TOKEN"))
	config.FeedbackToken = strings.TrimSpace(os.Getenv("JEV_SHADOW_FEEDBACK_TOKEN"))
	config.ReceiverAllowlist = csvSet(os.Getenv(envReceiverAllowlist))
	config.DestinationAllowlist = csvSet(os.Getenv(envDestinationAllowlist))
	config.LogicalSource = strings.TrimSpace(os.Getenv(envLogicalSource))
	config.Environment = strings.TrimSpace(os.Getenv(envEnvironment))
	config.SourceRegion = strings.TrimSpace(os.Getenv(envSourceRegion))
	config.PermissionDomain = strings.TrimSpace(os.Getenv(envPermissionDomain))
	config.ObservationReceiver = strings.TrimSpace(os.Getenv(envObservationReceiver))
	config.SenderApp = strings.TrimSpace(os.Getenv(envSenderApp))
	config.ReceiptOutboxDir = strings.TrimSpace(os.Getenv(envReceiptOutboxDir))
	config.CardStateDir = strings.TrimSpace(os.Getenv(envCardStateDir))
	if config.FeedbackEnabled, err = strconv.ParseBool(valueOrDefault(envFeedbackEnabled, "false")); err != nil {
		return Config{}, fmt.Errorf("%s must be a boolean: %w", envFeedbackEnabled, err)
	}
	config = config.withReceiptDefaults()
	if config.ReceiptMaxAttempts, err = positiveIntFromEnv(envReceiptMaxAttempts, config.ReceiptMaxAttempts); err != nil {
		return Config{}, err
	}
	if config.ReceiptRetryMin, err = positiveDurationFromEnv(envReceiptRetryMin, config.ReceiptRetryMin); err != nil {
		return Config{}, err
	}
	if config.ReceiptRetryMax, err = positiveDurationFromEnv(envReceiptRetryMax, config.ReceiptRetryMax); err != nil {
		return Config{}, err
	}
	if config.ReceiptDrainTimeout, err = positiveDurationFromEnv(envReceiptDrainTimeout, config.ReceiptDrainTimeout); err != nil {
		return Config{}, err
	}

	expiresAt := strings.TrimSpace(os.Getenv(envExpiresAt))
	if expiresAt != "" {
		config.ExpiresAt, err = time.Parse(time.RFC3339, expiresAt)
		if err != nil {
			return Config{}, fmt.Errorf("%s must be RFC3339 with a timezone: %w", envExpiresAt, err)
		}
	}

	if config.DeliveryScopes, err = loadDeliveryScopes(true); err != nil {
		return Config{}, err
	}
	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) validate() error {
	if c.Environment == "production" {
		if len(c.DeliveryScopes) == 0 && (c.CardOwnerID == "" || c.ExecutionDomain == "") {
			return fmt.Errorf("production executor requires explicit card owner and execution domain")
		}
		u, err := url.Parse(c.ObservationURL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("production executor observation URL is required")
		}
	}
	if !c.Enabled {
		return nil
	}
	if c.Environment == "prod" || c.Environment == "production" {
		if c.FeedbackEnabled && !c.ExistingCallbackIntegrated {
			return fmt.Errorf("production feedback requires the existing callback handler integration")
		}
		if c.FeedbackEnabled && os.Getenv("JEV_CARD_STATE_COORDINATED") != "true" {
			return fmt.Errorf("production feedback requires verified original card writer coordination")
		}
		if len(c.DeliveryScopes) == 0 && (len(c.AnnotationToken) < 32 || c.AnnotationToken == c.Token ||
			(c.FeedbackEnabled && (len(c.FeedbackToken) < 32 || c.FeedbackToken == c.Token || c.FeedbackToken == c.AnnotationToken))) {
			return fmt.Errorf("production executor requires separate receipt, annotation and feedback credentials")
		}
	}
	parsedURL, err := url.Parse(c.ReceiptURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return fmt.Errorf("%s must be an absolute HTTP(S) URL", envReceiptURL)
	}
	required := map[string]string{
		envToken:               c.Token,
		envLogicalSource:       c.LogicalSource,
		envEnvironment:         c.Environment,
		envSourceRegion:        c.SourceRegion,
		envPermissionDomain:    c.PermissionDomain,
		envObservationReceiver: c.ObservationReceiver,
		envSenderApp:           c.SenderApp,
	}
	for name, value := range required {
		if name == envToken && len(c.DeliveryScopes) > 0 {
			continue
		}
		if value == "" {
			return fmt.Errorf("%s is required when %s=true", name, envEnabled)
		}
	}
	if c.FeedbackEnabled && len(c.DeliveryScopes) == 0 && c.expectedProfileID == "" && (len(c.ReceiverAllowlist) != 1 || len(c.DestinationAllowlist) != 1) {
		return fmt.Errorf(
			"%s requires exactly one receiver and one destination in the UAT allowlists",
			envFeedbackEnabled,
		)
	}
	if len(c.DeliveryScopes) == 0 && len(c.ReceiverAllowlist) == 0 {
		return fmt.Errorf("%s must contain at least one receiver", envReceiverAllowlist)
	}
	if len(c.DeliveryScopes) == 0 && len(c.DestinationAllowlist) == 0 {
		return fmt.Errorf("%s must contain at least one destination", envDestinationAllowlist)
	}
	if c.MaxCards < 1 {
		return fmt.Errorf("Jev shadow card limit must be positive")
	}
	if strings.TrimSpace(c.ReceiptOutboxDir) == "" {
		return fmt.Errorf("%s must not be empty", envReceiptOutboxDir)
	}
	if strings.TrimSpace(c.CardStateDir) == "" {
		return fmt.Errorf("%s must not be empty", envCardStateDir)
	}
	if c.ReceiptMaxAttempts < 1 || c.ReceiptRetryMin <= 0 || c.ReceiptRetryMax <= 0 || c.ReceiptDrainTimeout <= 0 {
		return fmt.Errorf("Jev shadow receipt outbox limits must be positive")
	}
	if c.ReceiptRetryMax < c.ReceiptRetryMin {
		return fmt.Errorf("%s must be greater than or equal to %s", envReceiptRetryMax, envReceiptRetryMin)
	}
	return nil
}

func (c Config) withReceiptDefaults() Config {
	if strings.TrimSpace(c.ReceiptOutboxDir) == "" {
		c.ReceiptOutboxDir = filepath.Join(os.TempDir(), "notification-manager", "jev-shadow-receipts")
	}
	if strings.TrimSpace(c.CardStateDir) == "" {
		c.CardStateDir = filepath.Join(c.ReceiptOutboxDir, "card-state")
	}
	if c.ReceiptMaxAttempts == 0 {
		c.ReceiptMaxAttempts = 12
	}
	if c.ReceiptRetryMin == 0 {
		c.ReceiptRetryMin = time.Second
	}
	if c.ReceiptRetryMax == 0 {
		c.ReceiptRetryMax = 5 * time.Minute
	}
	if c.ReceiptDrainTimeout == 0 {
		c.ReceiptDrainTimeout = 5 * time.Second
	}
	return c
}

func positiveIntFromEnv(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func positiveDurationFromEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}

func csvSet(value string) map[string]struct{} {
	result := make(map[string]struct{})
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result[item] = struct{}{}
		}
	}
	return result
}

func valueOrDefault(name, defaultValue string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return defaultValue
}

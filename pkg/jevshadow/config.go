package jevshadow

import (
	"fmt"
	"net/url"
	"os"
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
)

type Config struct {
	Enabled              bool
	ReceiptURL           string
	Token                string
	ReceiverAllowlist    map[string]struct{}
	DestinationAllowlist map[string]struct{}
	LogicalSource        string
	Environment          string
	SourceRegion         string
	PermissionDomain     string
	ObservationReceiver  string
	SenderApp            string
	ExpiresAt            time.Time
	ReceiptQueueSize     int
	MaxCards             int
}

func ConfigFromEnv() (Config, error) {
	enabled, err := strconv.ParseBool(valueOrDefault(envEnabled, "false"))
	if err != nil {
		return Config{}, fmt.Errorf("%s must be a boolean: %w", envEnabled, err)
	}
	config := Config{Enabled: enabled, ReceiptQueueSize: 256, MaxCards: 2000}
	if !enabled {
		return config, nil
	}

	config.ReceiptURL = strings.TrimSpace(os.Getenv(envReceiptURL))
	config.Token = strings.TrimSpace(os.Getenv(envToken))
	config.ReceiverAllowlist = csvSet(os.Getenv(envReceiverAllowlist))
	config.DestinationAllowlist = csvSet(os.Getenv(envDestinationAllowlist))
	config.LogicalSource = strings.TrimSpace(os.Getenv(envLogicalSource))
	config.Environment = strings.TrimSpace(os.Getenv(envEnvironment))
	config.SourceRegion = strings.TrimSpace(os.Getenv(envSourceRegion))
	config.PermissionDomain = strings.TrimSpace(os.Getenv(envPermissionDomain))
	config.ObservationReceiver = strings.TrimSpace(os.Getenv(envObservationReceiver))
	config.SenderApp = strings.TrimSpace(os.Getenv(envSenderApp))

	expiresAt := strings.TrimSpace(os.Getenv(envExpiresAt))
	if expiresAt == "" {
		return Config{}, fmt.Errorf("%s is required when %s=true", envExpiresAt, envEnabled)
	}
	config.ExpiresAt, err = time.Parse(time.RFC3339, expiresAt)
	if err != nil {
		return Config{}, fmt.Errorf("%s must be RFC3339 with a timezone: %w", envExpiresAt, err)
	}

	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) validate() error {
	if !c.Enabled {
		return nil
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
		if value == "" {
			return fmt.Errorf("%s is required when %s=true", name, envEnabled)
		}
	}
	if len(c.ReceiverAllowlist) == 0 {
		return fmt.Errorf("%s must contain at least one receiver", envReceiverAllowlist)
	}
	if len(c.DestinationAllowlist) == 0 {
		return fmt.Errorf("%s must contain at least one destination", envDestinationAllowlist)
	}
	if c.ExpiresAt.IsZero() {
		return fmt.Errorf("%s is required when %s=true", envExpiresAt, envEnabled)
	}
	if c.ReceiptQueueSize < 1 || c.MaxCards < 1 {
		return fmt.Errorf("Jev shadow queue and card limits must be positive")
	}
	return nil
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

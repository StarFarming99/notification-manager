package jevshadow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kubesphere/notification-manager/pkg/template"
)

func framedDigest(version string, parts ...string) string {
	h := sha256.New()
	for _, part := range append([]string{version}, parts...) {
		fmt.Fprintf(h, "%d:", len([]byte(part)))
		h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func fingerprintV2(source, environment, domain string, labels map[string]string, excluded map[string]struct{}) (string, error) {
	if source == "" || environment == "" || domain == "" {
		return "", errors.New("trusted producer scope required")
	}
	parts := []string{source, environment, domain}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		if k == "receiver" {
			continue
		}
		if _, omit := excluded[k]; !omit {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !utf8.ValidString(k) || !utf8.ValidString(labels[k]) {
			return "", errors.New("identity labels must be UTF-8")
		}
		parts = append(parts, k, labels[k])
	}
	return "fp2_" + framedDigest("fingerprint-v2", parts...), nil
}

func episodeV2(source, environment, domain, fingerprint string, startsAt time.Time) (string, error) {
	if startsAt.Year() < 1970 || !strings.HasPrefix(fingerprint, "fp2_") {
		return "", errors.New("valid canonical episode required")
	}
	return "ep2_" + framedDigest("episode-v2", source, environment, domain, fingerprint, pythonISOTime(startsAt))[:48], nil
}

func (s *Service) canonicalData(data *template.Data) (*template.Data, error) {
	clone := data.Clone()
	for _, alert := range clone.Alerts {
		if alert.StartsAt.Year() < 1970 {
			return nil, errors.New("invalid canonical startsAt")
		}
		labels := labelsWithoutReceiver(alert.Labels)
		for key := range s.config.ExcludedIdentityLabels {
			delete(labels, key)
		}
		fingerprint, err := fingerprintV2(s.config.LogicalSource, s.config.Environment, s.config.PermissionDomain, labels, nil)
		if err != nil {
			return nil, err
		}
		alert.ID = fingerprint
		alert.Labels = template.KV(labels)
	}
	return clone, nil
}

func occurrenceV2(episode string, alert *template.Alert) string {
	end := ""
	if alert.Status == "resolved" && !alert.EndsAt.Before(alert.StartsAt) {
		end = pythonISOTime(alert.EndsAt)
	}
	parts := []string{episode, alert.Status, end, "labels"}
	keys := make([]string, 0, len(alert.Labels))
	for key := range alert.Labels {
		if key != "receiver" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, key, alert.Labels[key])
	}
	parts = append(parts, "annotations")
	keys = nil
	for key := range alert.Annotations {
		if key != "alerttime" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, key, alert.Annotations[key])
	}
	return "occ2_" + framedDigest("occurrence-v2", parts...)[:48]
}

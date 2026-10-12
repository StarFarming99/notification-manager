package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/kubesphere/notification-manager/pkg/template"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/kubesphere/notification-manager/apis/v2beta2"
	feishutype "github.com/kubesphere/notification-manager/pkg/internal/feishu"
	"k8s.io/apimachinery/pkg/types"
)

func (c *Controller) FreezeFeishuCredentialReference(config *feishutype.Config) (*feishutype.CredentialConfigRef, error) {
	if config == nil || config.Common == nil || config.Common.Name == "" || c.staticIsolated {
		return nil, errors.New("literal credentials require an existing CR-backed config")
	}
	var obj v2beta2.Config
	if err := c.cache.Get(c.ctx, types.NamespacedName{Name: config.Common.Name}, &obj); err != nil {
		return nil, err
	}
	if obj.Spec.Feishu == nil || string(obj.UID) == "" {
		return nil, errors.New("original config identity unavailable")
	}
	appID, err := c.GetCredential(config.AppID)
	if err != nil || appID == "" {
		return nil, errors.New("original app identity unavailable")
	}
	ref := &feishutype.CredentialConfigRef{Name: obj.Name, UID: string(obj.UID), AppID: appID}
	if _, err := c.ResolveFeishuCredentialReference(ref); err != nil {
		return nil, err
	}
	return ref, nil
}

func (c *Controller) ResolveFeishuCredentialReference(ref *feishutype.CredentialConfigRef) (string, error) {
	if ref == nil || ref.Name == "" || ref.UID == "" || ref.AppID == "" || c.staticIsolated {
		return "", errors.New("original config credential reference unavailable")
	}
	var obj v2beta2.Config
	if err := c.cache.Get(c.ctx, types.NamespacedName{Name: ref.Name}, &obj); err != nil {
		return "", err
	}
	if string(obj.UID) != ref.UID || obj.Spec.Feishu == nil {
		return "", errors.New("original config identity changed")
	}
	appID, err := c.GetCredential(obj.Spec.Feishu.AppID)
	if err != nil || appID != ref.AppID {
		return "", errors.New("original application identity changed")
	}
	secret, err := c.GetCredential(obj.Spec.Feishu.AppSecret)
	if err != nil || secret == "" {
		return "", errors.New("original application credential unavailable")
	}
	return secret, nil
}

// FrozenRuntime is a request-local view. It retains live credential resolution,
// but never changes the shared controller options/template or copies its mutex.
func (c *Controller) FrozenRuntime(opts *v2beta2.Options, snapshot template.Snapshot) (*Controller, error) {
	tmpl, err := template.FromSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	return &Controller{logger: c.logger, ctx: c.ctx, cache: c.cache, namespace: c.namespace, staticIsolated: c.staticIsolated,
		staticCluster: c.staticCluster, ReceiverOpts: opts, frozenTemplate: tmpl, jevShadow: c.jevShadow,
		DeliveryProfiles: c.DeliveryProfiles, receivers: c.receivers, configs: c.configs}, nil
}

// CredentialReference points to the exact CR UID and credential field. Ordinary
// secret values may rotate; destination credentials (chatbot webhook) cannot
// silently move a previously accepted notification to a different endpoint.
type CredentialReference struct {
	Scalar          bool     `json:"scalar,omitempty"`
	TargetPath      []string `json:"target_path"`
	SourcePath      []string `json:"source_path"`
	Kind            string   `json:"kind"`
	Name            string   `json:"name"`
	UID             string   `json:"uid"`
	DestinationHash string   `json:"destination_hash,omitempty"`
	SenderApp       string   `json:"sender_app,omitempty"`
	KeyID           string   `json:"key_id,omitempty"`
	Ciphertext      string   `json:"ciphertext,omitempty"`
}

func credentialValueHash(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])
}
func visitLiteralCredentials(value interface{}, path []string, fn func(map[string]interface{}, []string) error) error {
	switch v := value.(type) {
	case map[string]interface{}:
		if text, ok := v["value"].(string); ok && text != "" {
			return fn(v, path)
		}
		keys := []string{}
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := visitLiteralCredentials(v[key], append(append([]string{}, path...), key), fn); err != nil {
				return err
			}
		}
	case []interface{}:
		for i, child := range v {
			if err := visitLiteralCredentials(child, append(append([]string{}, path...), strconv.Itoa(i)), fn); err != nil {
				return err
			}
		}
	}
	return nil
}
func atJSONPath(value interface{}, path []string) (interface{}, error) {
	for _, field := range path {
		switch v := value.(type) {
		case map[string]interface{}:
			value = v[field]
		case []interface{}:
			i, err := strconv.Atoi(field)
			if err != nil || i < 0 || i >= len(v) {
				return nil, errors.New("credential reference field unavailable")
			}
			value = v[i]
		default:
			return nil, errors.New("credential reference field unavailable")
		}
	}
	if value == nil {
		return nil, errors.New("credential reference field unavailable")
	}
	return value, nil
}
func (c *Controller) credentialSource(kind, name string) (interface{}, string, error) {
	if c.staticIsolated || c.cache == nil || name == "" {
		return nil, "", errors.New("literal credential requires original CR reference")
	}
	var obj interface{}
	var uid string
	switch kind {
	case "receiver":
		value := &v2beta2.Receiver{}
		if err := c.cache.Get(c.ctx, types.NamespacedName{Name: name}, value); err != nil {
			return nil, "", err
		}
		obj = value
		uid = string(value.UID)
	case "config":
		value := &v2beta2.Config{}
		if err := c.cache.Get(c.ctx, types.NamespacedName{Name: name}, value); err != nil {
			return nil, "", err
		}
		obj = value
		uid = string(value.UID)
	default:
		return nil, "", errors.New("credential reference kind invalid")
	}
	if uid == "" {
		return nil, "", errors.New("original credential CR identity unavailable")
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, "", err
	}
	var decoded interface{}
	err = json.Unmarshal(raw, &decoded)
	return decoded, uid, err
}
func (c *Controller) FreezeCredentialReferences(raw json.RawMessage, receiverName, configName, channel string) (json.RawMessage, []CredentialReference, error) {
	var decoded interface{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, nil, err
	}
	refs := []CredentialReference{}
	err := visitLiteralCredentials(decoded, nil, func(credential map[string]interface{}, targetPath []string) error {
		field := ""
		if len(targetPath) > 0 {
			field = targetPath[len(targetPath)-1]
		}
		if field == "appID" {
			return nil
		} // immutable public sender identity
		value := credential["value"].(string)
		matched := false
		for _, source := range []struct{ kind, name string }{{"receiver", receiverName}, {"config", configName}} {
			if source.name == "" {
				continue
			}
			obj, uid, err := c.credentialSource(source.kind, source.name)
			if err != nil {
				continue
			}
			root, err := atJSONPath(obj, []string{"spec", channel})
			if err != nil {
				continue
			}
			var found []string
			err = visitLiteralCredentials(root, []string{"spec", channel}, func(candidate map[string]interface{}, path []string) error {
				if candidate["value"] == value && len(path) > 0 && strings.EqualFold(path[len(path)-1], field) && found == nil {
					found = append([]string{}, path...)
				}
				return nil
			})
			if err != nil {
				return err
			}
			if found != nil {
				ref := CredentialReference{TargetPath: append([]string{}, targetPath...), SourcePath: found, Kind: source.kind, Name: source.name, UID: uid}
				if channel == "feishu" && source.kind == "config" {
					if appField, e := atJSONPath(root, []string{"appID"}); e == nil {
						appRaw, _ := json.Marshal(appField)
						var app v2beta2.Credential
						_ = json.Unmarshal(appRaw, &app)
						ref.SenderApp, err = c.GetCredential(&app)
						if err != nil || ref.SenderApp == "" {
							return errors.New("original sender app identity unavailable")
						}
					}
				}
				if strings.EqualFold(field, "webhook") {
					ref.DestinationHash = credentialValueHash(value)
				}
				refs = append(refs, ref)
				delete(credential, "value")
				matched = true
				break
			}
		}
		if !matched {
			ref, err := encryptInlineCredential(value, targetPath)
			if err != nil {
				return err
			}
			refs = append(refs, ref)
			delete(credential, "value")
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if err := c.freezeSensitiveURLs(decoded, nil, receiverName, configName, channel, &refs); err != nil {
		return nil, nil, err
	}
	safe, err := json.Marshal(decoded)
	return safe, refs, err
}
func (c *Controller) ResolveCredentialReferences(raw json.RawMessage, refs []CredentialReference) (json.RawMessage, error) {
	var decoded interface{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	for _, ref := range refs {
		if ref.Kind == "encrypted_inline" {
			value, err := decryptInlineCredential(ref)
			if err != nil {
				return nil, err
			}
			if ref.Scalar {
				if err := replaceScalarReference(decoded, ref.TargetPath, value); err != nil {
					return nil, err
				}
				continue
			}
			item, err := atJSONPath(decoded, ref.TargetPath)
			if err != nil {
				return nil, err
			}
			m, ok := item.(map[string]interface{})
			if !ok || len(m) > 0 {
				return nil, errors.New("frozen credential placeholder modified")
			}
			m["value"] = value
			continue
		}
		obj, uid, err := c.credentialSource(ref.Kind, ref.Name)
		if err != nil {
			return nil, err
		}
		if uid != ref.UID {
			return nil, errors.New("original credential CR identity changed")
		}
		if ref.SenderApp != "" {
			appField, err := atJSONPath(obj, []string{"spec", "feishu", "appID"})
			if err != nil {
				return nil, err
			}
			appRaw, _ := json.Marshal(appField)
			var app v2beta2.Credential
			_ = json.Unmarshal(appRaw, &app)
			liveApp, err := c.GetCredential(&app)
			if err != nil || liveApp != ref.SenderApp {
				return nil, errors.New("original sender app identity changed")
			}
		}
		item, err := atJSONPath(obj, ref.SourcePath)
		if err != nil {
			return nil, err
		}
		if ref.Scalar {
			value, ok := item.(string)
			if !ok || credentialValueHash(value) != ref.DestinationHash {
				return nil, errors.New("original frozen URL destination changed")
			}
			if err := replaceScalarReference(decoded, ref.TargetPath, value); err != nil {
				return nil, err
			}
			continue
		}
		credential, ok := item.(map[string]interface{})
		if !ok {
			return nil, errors.New("original credential field invalid")
		}
		body, err := json.Marshal(credential)
		if err != nil {
			return nil, err
		}
		var live v2beta2.Credential
		if err := json.Unmarshal(body, &live); err != nil {
			return nil, err
		}
		value, err := c.GetCredential(&live)
		if err != nil || value == "" {
			return nil, errors.New("original credential unavailable")
		}
		if ref.DestinationHash != "" && credentialValueHash(value) != ref.DestinationHash {
			return nil, errors.New("original frozen destination credential changed")
		}
		target, err := atJSONPath(decoded, ref.TargetPath)
		if err != nil {
			return nil, err
		}
		m, ok := target.(map[string]interface{})
		if !ok {
			return nil, errors.New("frozen credential field invalid")
		}
		if len(m) > 0 {
			return nil, errors.New("frozen credential placeholder modified")
		}
		m["value"] = value
	}
	return json.Marshal(decoded)
}

func replaceScalarReference(decoded interface{}, path []string, value string) error {
	if len(path) == 0 {
		return errors.New("frozen URL reference invalid")
	}
	parent, err := atJSONPath(decoded, path[:len(path)-1])
	if err != nil {
		return err
	}
	m, ok := parent.(map[string]interface{})
	if !ok || m[path[len(path)-1]] != "" {
		return errors.New("frozen URL placeholder modified")
	}
	m[path[len(path)-1]] = value
	return nil
}
func (c *Controller) freezeSensitiveURLs(value interface{}, path []string, receiverName, configName, channel string, refs *[]CredentialReference) error {
	switch v := value.(type) {
	case map[string]interface{}:
		keys := []string{}
		for field := range v {
			keys = append(keys, field)
		}
		sort.Strings(keys)
		for _, field := range keys {
			child := v[field]
			next := append(append([]string(nil), path...), field)
			if text, ok := child.(string); ok && strings.EqualFold(field, "userKey") && text != "" {
				ref, err := encryptInlineCredential(text, next)
				if err != nil {
					return err
				}
				ref.Scalar = true
				*refs = append(*refs, ref)
				v[field] = ""
				continue
			}
			if text, ok := child.(string); ok && (strings.EqualFold(field, "url") || strings.EqualFold(field, "proxyUrl") || strings.EqualFold(field, "apiurl")) {
				parsed, err := url.Parse(text)
				if err == nil && (parsed.User != nil || parsed.RawQuery != "") {
					matched := false
					for _, source := range []struct{ kind, name string }{{"receiver", receiverName}, {"config", configName}} {
						obj, uid, e := c.credentialSource(source.kind, source.name)
						if e != nil {
							continue
						}
						sourcePath := append([]string{"spec", channel}, next...)
						item, e := atJSONPath(obj, sourcePath)
						if e == nil && item == text {
							*refs = append(*refs, CredentialReference{Kind: source.kind, Name: source.name, UID: uid, TargetPath: next, SourcePath: sourcePath, DestinationHash: credentialValueHash(text), Scalar: true})
							matched = true
							break
						}
					}
					if !matched {
						ref, e := encryptInlineCredential(text, next)
						if e != nil {
							return e
						}
						ref.Scalar = true
						*refs = append(*refs, ref)
					}
					v[field] = ""
					continue
				}
			}
			if err := c.freezeSensitiveURLs(child, next, receiverName, configName, channel, refs); err != nil {
				return err
			}
		}
	case []interface{}:
		for i, child := range v {
			next := append(append([]string(nil), path...), strconv.Itoa(i))
			if err := c.freezeSensitiveURLs(child, next, receiverName, configName, channel, refs); err != nil {
				return err
			}
		}
	}
	return nil
}

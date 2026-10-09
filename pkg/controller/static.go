package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/internal"
	"github.com/kubesphere/notification-manager/pkg/template"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

type StaticConfiguration struct {
	Routers     []v2beta2.Router   `json:"routers"`
	Silences    []v2beta2.Silence  `json:"silences"`
	Environment string             `json:"environment"`
	Cluster     string             `json:"cluster"`
	Template    string             `json:"template"`
	Receivers   []v2beta2.Receiver `json:"receivers"`
	Configs     []v2beta2.Config   `json:"configs"`
	Options     *v2beta2.Options   `json:"options"`
	GroupLabels []string           `json:"group_labels"`
}

// NewStatic is deliberately disjoint from the CR-backed controller. It never
// creates a Kubernetes client, reads a cluster Secret, or starts an informer.
func NewStatic(ctx context.Context, logger log.Logger, config StaticConfiguration) (*Controller, error) {
	if config.Environment != "uat" && config.Environment != "test" {
		return nil, errors.New("static-isolated configuration is UAT only")
	}
	if config.Cluster == "" || len(config.Receivers) == 0 {
		return nil, errors.New("static cluster and receivers are required")
	}
	raw, _ := json.Marshal(config)
	for _, field := range []string{`"valueFrom":`, `"tmplText":`, `"templateFiles":`} {
		if strings.Contains(string(raw), field) {
			return nil, errors.New("static-isolated config cannot reference cluster resources or template files")
		}
	}
	tmpl, err := template.New("", nil)
	if err != nil {
		return nil, err
	}
	if tmpl, err = tmpl.ParserText(config.Template); err != nil {
		return nil, err
	}
	c := &Controller{logger: logger, ctx: ctx, staticIsolated: true, staticCluster: config.Cluster, staticRouters: config.Routers, staticSilences: config.Silences,
		receivers: make(map[string]map[string]internal.Receiver), configs: make(map[string]map[string]internal.Config),
		ch: make(chan *task, ChannelCapacity), tenantKey: defaultTenantKey, ReceiverOpts: config.Options,
		batchMaxSize: 100, batchMaxWait: metav1.Duration{Duration: time.Second}, groupLabels: config.GroupLabels, tmpl: tmpl}
	c.receivers[globalTenantID] = make(map[string]internal.Receiver)
	// Global receivers resolve credentials through defaultConfig, just like
	// the CR-backed controller. The receiver tenant is a different key.
	c.configs[defaultConfig] = make(map[string]internal.Config)
	for _, obj := range config.Configs {
		for key, value := range NewConfigs(&obj) {
			if err := value.Validate(); err != nil {
				return nil, err
			}
			c.configs[defaultConfig][key] = value
		}
	}
	for _, obj := range config.Receivers {
		for key, value := range NewReceivers(globalTenantID, &obj) {
			getMatchedConfig(value, c.configs)
			if err := value.Validate(); err != nil {
				return nil, err
			}
			c.receivers[globalTenantID][key] = value
		}
	}
	return c, nil
}

func staticFromFile(ctx context.Context, logger log.Logger) (*Controller, error) {
	data, err := os.ReadFile(os.Getenv("NM_STATIC_CONFIG_FILE"))
	if err != nil {
		return nil, err
	}
	if len(data) > 2*1024*1024 {
		return nil, errors.New("static config too large")
	}
	var config StaticConfiguration
	if err := yaml.UnmarshalStrict(data, &config); err != nil {
		return nil, err
	}
	allowed := func(env, value string) bool {
		for _, item := range strings.Split(os.Getenv(env), ",") {
			if strings.TrimSpace(item) == value && value != "" {
				return true
			}
		}
		return false
	}
	// The operator must name test app/chat/hosts separately from the config.
	// This prevents an accidentally copied production receiver from being sent.
	for _, receiver := range config.Receivers {
		if f := receiver.Spec.Feishu; f != nil {
			for _, chat := range f.ChatIDs {
				if !allowed("NM_STATIC_ALLOWED_CHAT_IDS", chat) {
					return nil, errors.New("test chat is not explicitly allowlisted")
				}
			}
			if f.ChatBot != nil || len(f.User) > 0 || len(f.Department) > 0 {
				return nil, errors.New("static tests support explicit chat IDs only")
			}
		}
		if w := receiver.Spec.Webhook; w != nil {
			if w.URL == nil {
				return nil, errors.New("static webhook URL required")
			}
			parsed, err := url.Parse(*w.URL)
			if err != nil || parsed.User != nil || !allowed("NM_STATIC_ALLOWED_HOSTS", parsed.Hostname()) {
				return nil, errors.New("test webhook host is not explicitly allowlisted")
			}
		}
		// Other channel tests are offline golden fixtures; static live sends
		// are intentionally restricted to the dedicated test app or mock host.
		raw, _ := json.Marshal(receiver.Spec)
		var fields map[string]interface{}
		_ = json.Unmarshal(raw, &fields)
		for kind := range fields {
			if kind != "feishu" && kind != "webhook" {
				return nil, errors.New("static live channel is not supported")
			}
		}
	}
	for _, item := range config.Configs {
		if f := item.Spec.Feishu; f != nil && (f.AppID == nil || !allowed("NM_STATIC_ALLOWED_APP_IDS", f.AppID.Value)) {
			return nil, errors.New("test app is not explicitly allowlisted")
		}
	}
	return NewStatic(ctx, logger, config)
}

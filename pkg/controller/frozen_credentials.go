package controller

import (
	"errors"

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

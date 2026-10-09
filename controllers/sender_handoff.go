package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/kubesphere/notification-manager/apis/v2beta2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Annotation intent is part of the existing single CR, so old senders still
// read the same configuration and no second shared NM object is introduced.
const SenderHandoffAnnotation = "notification.kubesphere.io/sender-handoff"

type SenderHandoff struct {
	OperationID   string            `json:"operation_id"`
	Phase         string            `json:"phase"` // active, retired, rollback
	Deployment    string            `json:"deployment"`
	DeploymentUID string            `json:"deployment_uid"`
	Selector      map[string]string `json:"selector"`
	DrainEvidence string            `json:"drain_evidence,omitempty"`
}

func handoffIntent(nm *v2beta2.NotificationManager) (*SenderHandoff, error) {
	raw := nm.Annotations[SenderHandoffAnnotation]
	if raw == "" {
		return nil, nil
	}
	if len(raw) > 16<<10 {
		return nil, errors.New("handoff intent too large")
	}
	var h SenderHandoff
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&h); err != nil {
		return nil, err
	}
	if err := dec.Decode(new(interface{})); err != io.EOF {
		return nil, errors.New("handoff intent must contain one JSON object")
	}
	if h.OperationID == "" || len(h.OperationID) > 128 || h.Deployment == "" || h.DeploymentUID == "" || len(h.Selector) == 0 {
		return nil, errors.New("handoff requires operation, candidate identity and disjoint selector")
	}
	if h.Phase != "active" && h.Phase != "retired" && h.Phase != "rollback" {
		return nil, errors.New("invalid handoff phase")
	}
	if h.Phase == "retired" && (h.DrainEvidence == "" || len(h.DrainEvidence) > 512) {
		return nil, errors.New("retiring original sender requires bounded drain/reconciliation evidence")
	}
	return &h, nil
}

func labelsMatch(selector, labels map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}
func deploymentReady(d *appsv1.Deployment) bool {
	return d.Spec.Replicas != nil && *d.Spec.Replicas > 0 && d.Status.ObservedGeneration >= d.Generation && d.Status.ReadyReplicas >= *d.Spec.Replicas && d.Status.UpdatedReplicas >= *d.Spec.Replicas
}

func (r *NotificationManagerReconciler) handoffSelector(ctx context.Context, nm *v2beta2.NotificationManager, h *SenderHandoff) (map[string]string, error) {
	old := *r.makeCommonLabels(nm)
	if labelsMatch(h.Selector, old) {
		return nil, errors.New("candidate selector overlaps original sender")
	}
	if h.Deployment == nm.Name+"-deployment" {
		return nil, errors.New("candidate must be independent of the owned original Deployment")
	}
	if h.Phase == "rollback" {
		var original appsv1.Deployment
		if err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: nm.Name + "-deployment"}, &original); err != nil {
			return nil, err
		}
		if !deploymentReady(&original) {
			return nil, errors.New("rollback original backend is not Ready; stable Service remains on candidate")
		}
		return old, nil
	}
	var candidate appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: h.Deployment}, &candidate); err != nil {
		return nil, err
	}
	if string(candidate.UID) != h.DeploymentUID || !labelsMatch(h.Selector, candidate.Spec.Template.Labels) {
		return nil, errors.New("candidate UID/selector mismatch")
	}
	if !deploymentReady(&candidate) {
		return nil, errors.New("candidate backend is not Ready")
	}
	port := nm.Spec.PortName
	if port == "" {
		port = defaultPortName
	}
	validPort := false
	for _, c := range candidate.Spec.Template.Spec.Containers {
		for _, p := range c.Ports {
			if p.Name == port && p.ContainerPort == 19093 && (p.Protocol == "" || p.Protocol == corev1.ProtocolTCP) && formalCandidateContract(c) {
				validPort = true
			}
		}
	}
	if !validPort {
		return nil, errors.New("candidate requires enabled formal compatibility and a formal-readiness probe on its separate primary port")
	}
	var deployments appsv1.DeploymentList
	if err := r.List(ctx, &deployments, client.InNamespace(r.Namespace)); err != nil {
		return nil, err
	}
	for _, d := range deployments.Items {
		if d.Name != h.Deployment && labelsMatch(h.Selector, d.Spec.Template.Labels) {
			return nil, fmt.Errorf("candidate selector also selects deployment %s", d.Name)
		}
	}
	return h.Selector, nil
}

func formalCandidateContract(c corev1.Container) bool {
	compat, sources, primary := false, false, false
	for _, env := range c.Env {
		if env.Name == "NM_FORMAL_COMPAT_LISTEN_ADDRESS" && (env.Value == ":19093" || env.Value == "0.0.0.0:19093") {
			compat = true
		}
		if env.Name == "NM_FORMAL_COMPAT_SOURCE_CIDRS" && (env.Value != "" || env.ValueFrom != nil) {
			sources = true
		}
	}
	for i, arg := range c.Args {
		if arg == "--webhook.address=:19094" || arg == "--webhook.address=0.0.0.0:19094" || (arg == "--webhook.address" && i+1 < len(c.Args) && (c.Args[i+1] == ":19094" || c.Args[i+1] == "0.0.0.0:19094")) {
			primary = true
		}
	}
	if !compat || !sources || !primary || c.ReadinessProbe == nil || c.ReadinessProbe.HTTPGet == nil {
		return false
	}
	probe := c.ReadinessProbe.HTTPGet
	if probe.Path != "/-/formal-ready" || probe.Host != "" || (probe.Scheme != "" && probe.Scheme != corev1.URISchemeHTTP) {
		return false
	}
	if probe.Port.Type == intstr.Int {
		return probe.Port.IntVal == 19094
	}
	for _, p := range c.Ports {
		if p.Name == probe.Port.StrVal && p.ContainerPort == 19094 && (p.Protocol == "" || p.Protocol == corev1.ProtocolTCP) {
			return true
		}
	}
	return false
}

// createManagedHandoffService preserves ClusterIP and existing Service
// metadata while continuously reconciling the chosen backend. Recreation uses
// the same persisted CR intent. Legacy installations keep create-only behavior.
func (r *NotificationManagerReconciler) createManagedHandoffService(ctx context.Context, nm *v2beta2.NotificationManager, h *SenderHandoff) error {
	selector, err := r.handoffSelector(ctx, nm, h)
	if err != nil {
		return err
	}
	var svc corev1.Service
	key := types.NamespacedName{Namespace: r.Namespace, Name: nm.Name + "-svc"}
	if err := r.Get(ctx, key, &svc); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return err
		}
		port := nm.Spec.PortName
		if port == "" {
			port = defaultPortName
		}
		svc = corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace, Labels: *r.makeCommonLabels(nm), Annotations: map[string]string{SenderHandoffAnnotation: h.OperationID + ":" + h.Phase}}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: selector, Ports: []corev1.ServicePort{{Name: port, Port: 19093, TargetPort: intstr.FromString(port)}}}}
		if err := ctrl.SetControllerReference(nm, &svc, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, &svc)
	}
	svc.Spec.Selector = selector
	if svc.Annotations == nil {
		svc.Annotations = map[string]string{}
	}
	svc.Annotations[SenderHandoffAnnotation] = h.OperationID + ":" + h.Phase
	return r.Update(ctx, &svc)
}

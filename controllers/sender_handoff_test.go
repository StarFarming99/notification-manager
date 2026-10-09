package controllers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/go-logr/logr"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func handoffFixture(t *testing.T, phase string) (*NotificationManagerReconciler, *v2beta2.NotificationManager, SenderHandoff) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = v2beta2.AddToScheme(scheme)
	one := int32(1)
	image := "fixture"
	nm := &v2beta2.NotificationManager{ObjectMeta: metav1.ObjectMeta{Name: "notification-manager", UID: "nm-uid"}, Spec: v2beta2.NotificationManagerSpec{Image: &image, Replicas: &one, Receivers: &v2beta2.ReceiversSpec{}}}
	h := SenderHandoff{OperationID: "promotion-1", Phase: phase, Deployment: "new-nm", DeploymentUID: "new-uid", Selector: map[string]string{"nm-delivery-instance": "candidate"}, DrainEvidence: "verified no old intake; reconciled unknown boundary"}
	raw, _ := json.Marshal(h)
	nm.Annotations = map[string]string{SenderHandoffAnnotation: string(raw)}
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: h.Deployment, Namespace: "monitoring", UID: types.UID(h.DeploymentUID), Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &one, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: h.Selector}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "nm", Args: []string{"--webhook.address=:19094"}, Env: []corev1.EnvVar{{Name: "NM_FORMAL_COMPAT_LISTEN_ADDRESS", Value: ":19093"}, {Name: "NM_FORMAL_COMPAT_SOURCE_CIDRS", Value: "10.0.0.0/24"}}, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/-/formal-ready", Port: intstr.FromInt(19094)}}}, Ports: []corev1.ContainerPort{{Name: "webhook", ContainerPort: 19093}, {Name: "test-intake", ContainerPort: 19094}}}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, ReadyReplicas: 1, UpdatedReplicas: 1}}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: nm.Name + "-svc", Namespace: "monitoring", Annotations: map[string]string{"keep": "metadata"}}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.10", Selector: map[string]string{"app": "notification-manager", "notification-manager": nm.Name}, Ports: []corev1.ServicePort{{Name: "webhook", Port: 19093, TargetPort: intstr.FromString("webhook")}}}}
	r := &NotificationManagerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(nm, d, svc).Build(), Namespace: "monitoring", Scheme: scheme, Log: logr.Discard()}
	return r, nm, h
}
func TestManagedServiceHandoffReconcileRecreationAndRetirement(t *testing.T) {
	r, nm, h := handoffFixture(t, "retired")
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: nm.Name}}
	for round := 0; round < 2; round++ {
		if _, err := r.Reconcile(ctx, req); err != nil {
			t.Fatal(err)
		}
		var svc corev1.Service
		var old appsv1.Deployment
		if err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: nm.Name + "-svc"}, &svc); err != nil {
			t.Fatal(err)
		}
		if !labelsMatch(h.Selector, svc.Spec.Selector) || labelsMatch(svc.Spec.Selector, map[string]string{"app": "notification-manager", "notification-manager": nm.Name}) {
			t.Fatal("Service selected original or mixed backend", svc.Spec.Selector)
		}
		if round == 0 && (svc.Spec.ClusterIP != "10.0.0.10" || svc.Annotations["keep"] != "metadata") {
			t.Fatal("handoff lost stable Service identity")
		}
		if err := r.Get(ctx, types.NamespacedName{Namespace: r.Namespace, Name: nm.Name + "-deployment"}, &old); err != nil {
			t.Fatal(err)
		}
		if *old.Spec.Replicas != 0 {
			t.Fatal("retired sender restored by reconcile")
		}
		if round == 0 {
			if err := r.Delete(ctx, &svc); err != nil {
				t.Fatal(err)
			}
			if err := r.Delete(ctx, &old); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Removing the active ownership intent is not an implicit rollback.
	var live v2beta2.NotificationManager
	_ = r.Get(ctx, types.NamespacedName{Name: nm.Name}, &live)
	delete(live.Annotations, SenderHandoffAnnotation)
	_ = r.Update(ctx, &live)
	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("active handoff disappeared without explicit rollback")
	}
}
func TestManagedHandoffRejectsUnsafeCandidateAndMissingDrain(t *testing.T) {
	for _, change := range []func(*SenderHandoff){func(h *SenderHandoff) {
		h.Selector = map[string]string{"app": "notification-manager", "notification-manager": "notification-manager"}
	}, func(h *SenderHandoff) { h.DeploymentUID = "wrong" }, func(h *SenderHandoff) { h.Phase = "retired"; h.DrainEvidence = "" }} {
		r, nm, h := handoffFixture(t, "active")
		change(&h)
		raw, _ := json.Marshal(h)
		nm.Annotations[SenderHandoffAnnotation] = string(raw)
		if err := r.Update(context.Background(), nm); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: nm.Name}}); err == nil {
			t.Fatal("unsafe handoff accepted", h)
		}
		var svc corev1.Service
		_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-svc"}, &svc)
		if svc.Spec.Selector["notification-manager"] != nm.Name {
			t.Fatal("failed handoff changed stable selector")
		}
	}
}
func TestManagedRollbackReadiesOriginalBeforeServiceSwitch(t *testing.T) {
	r, nm, h := handoffFixture(t, "retired")
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: nm.Name}}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	var live v2beta2.NotificationManager
	_ = r.Get(ctx, client.ObjectKey{Name: nm.Name}, &live)
	h.Phase = "rollback"
	raw, _ := json.Marshal(h)
	live.Annotations[SenderHandoffAnnotation] = string(raw)
	_ = r.Update(ctx, &live)
	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("rollback selected an unready original")
	}
	var svc corev1.Service
	_ = r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-svc"}, &svc)
	if svc.Spec.Selector["nm-delivery-instance"] != "candidate" {
		t.Fatal("rollback gap selected unready original")
	}
	var old appsv1.Deployment
	_ = r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-deployment"}, &old)
	if *old.Spec.Replicas != 1 {
		t.Fatal("rollback did not ready original")
	}
	old.Status = appsv1.DeploymentStatus{ObservedGeneration: old.Generation, ReadyReplicas: 1, UpdatedReplicas: 1}
	if err := r.Update(ctx, &old); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	_ = r.Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-svc"}, &svc)
	if svc.Spec.Selector["notification-manager"] != nm.Name {
		t.Fatal("rollback did not restore stable backend")
	}
}

func TestManagedHandoffRejectsTestReadyOrDisabledCompatibility(t *testing.T) {
	for _, change := range []func(*corev1.Container){func(c *corev1.Container) { c.ReadinessProbe.HTTPGet.Path = "/-/ready" }, func(c *corev1.Container) { c.Env = nil }, func(c *corev1.Container) { c.ReadinessProbe.HTTPGet.Port = intstr.FromInt(19093) }} {
		r, nm, h := handoffFixture(t, "active")
		var candidate appsv1.Deployment
		_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: h.Deployment}, &candidate)
		change(&candidate.Spec.Template.Spec.Containers[0])
		_ = r.Update(context.Background(), &candidate)
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: nm.Name}}); err == nil {
			t.Fatal("test Ready alone allowed production takeover")
		}
		var svc corev1.Service
		_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: nm.Name + "-svc"}, &svc)
		if svc.Spec.Selector["notification-manager"] != nm.Name {
			t.Fatal("formal-not-ready changed shared Service")
		}
	}
}

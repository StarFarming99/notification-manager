package controllers

import (
	"context"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func addProfiledCandidate(t *testing.T, r *NotificationManagerReconciler) {
	t.Helper()
	d := getDep(t, r, "new-nm")
	d.Spec.Template.Spec.Containers[0].Env = append(d.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "NM_DELIVERY_PROFILES_FILE", Value: "/etc/nm/profiles.json"})
	d.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "ledger", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "formal-pvc"}}}}
	if err := r.Update(context.Background(), d); err != nil {
		t.Fatal(err)
	}
}

func addPreservedTest(t *testing.T, r *NotificationManagerReconciler, h *SenderHandoff) {
	t.Helper()
	ctx := context.Background()
	labels := map[string]string{"nm-isolated-test": "existing"}
	controller := true
	one := int32(1)
	test := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "preserved-test", Namespace: r.Namespace, UID: "preserved-test-uid", Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "nm", Image: "existing-test-byte-pinned"}}, Volumes: []corev1.Volume{{Name: "ledger", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "test-only-pvc"}}}}}}}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, ReadyReplicas: 1, UpdatedReplicas: 1}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "preserved-test-rs", Namespace: r.Namespace, UID: "preserved-test-rs-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: test.Name, UID: test.UID, Controller: &controller}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "preserved-test-pod", Namespace: r.Namespace, UID: "preserved-test-pod-uid", Labels: labels, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: &controller}}}, Spec: corev1.PodSpec{NodeName: "node1", Containers: test.Spec.Template.Spec.Containers}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.13", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "preserved-test", Namespace: r.Namespace, UID: "preserved-test-svc-uid"}, Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.14", Selector: labels, Ports: []corev1.ServicePort{{Name: "test-intake", Port: 19094, TargetPort: intstr.FromInt(19094)}}}}
	endpoints := &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Name: svc.Name, Namespace: r.Namespace}, Subsets: []corev1.EndpointSubset{{Ports: []corev1.EndpointPort{{Name: "test-intake", Port: 19094}}, Addresses: []corev1.EndpointAddress{{IP: pod.Status.PodIP, TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: r.Namespace, Name: pod.Name, UID: pod.UID}}}}}}
	for _, object := range []client.Object{test, rs, pod, svc, endpoints} {
		if err := r.Create(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	h.PreservedTest = &PreservedTestReference{Deployment: test.Name, DeploymentUID: string(test.UID), Service: svc.Name, ServiceUID: string(svc.UID), Selector: labels}
	setPhase(t, r, h, h.Phase)
}

func TestProfiledFirstCutoverRejectsSharedOrUnprovenTestBeforeStoppingOriginal(t *testing.T) {
	for _, variant := range []string{"missing-proof", "shared-sender", "shared-ledger", "changed-service-uid", "foreign-endpoint"} {
		t.Run(variant, func(t *testing.T) {
			r, _, h := handoffFixture(t, "stopped")
			addProfiledCandidate(t, r)
			if variant != "missing-proof" {
				addPreservedTest(t, r, &h)
			}
			switch variant {
			case "shared-sender":
				h.PreservedTest.Deployment, h.PreservedTest.DeploymentUID = "new-nm", "new-uid"
				setPhase(t, r, &h, "stopped")
			case "shared-ledger":
				d := getDep(t, r, "preserved-test")
				d.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "formal-pvc"
				_ = r.Update(context.Background(), d)
			case "changed-service-uid":
				h.PreservedTest.ServiceUID = "replaced-service"
				setPhase(t, r, &h, "stopped")
			case "foreign-endpoint":
				var endpoints corev1.Endpoints
				_ = r.Get(context.Background(), client.ObjectKey{Namespace: r.Namespace, Name: "preserved-test"}, &endpoints)
				endpoints.Subsets[0].Addresses[0].TargetRef.UID = types.UID("other-pod")
				_ = r.Update(context.Background(), &endpoints)
			}
			if err := reconcileHandoffTest(r); err == nil || !strings.Contains(err.Error(), "BLOCKED") {
				t.Fatal("unsafe shared test/formal handoff was not blocked", err)
			}
			if *getDep(t, r, "notification-manager-deployment").Spec.Replicas != 1 || stateOf(t, r) != nil {
				t.Fatal("failed test-continuity precondition stopped original or created stop intent")
			}
			assertSelector(t, r, "notification-manager")
		})
	}
}

func TestIndependentTestSenderRemainsUnchangedThroughFormalStopSwitchAndRollback(t *testing.T) {
	r, _, h := handoffFixture(t, "stopped")
	addProfiledCandidate(t, r)
	addPreservedTest(t, r, &h)
	before := getDep(t, r, "preserved-test").DeepCopy()
	stopOld(t, r)
	setPhase(t, r, &h, "retired")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	if stateOf(t, r).TestContinuity != "independent_test_sender_verified" {
		t.Fatal("independent test verification missing")
	}
	setPhase(t, r, &h, "rollback")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	normalTerminate(t, r, "new-pod", 30)
	for i := 0; i < 3; i++ {
		if err := reconcileHandoffTest(r); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(before.Spec, getDep(t, r, "preserved-test").Spec) || *getDep(t, r, "preserved-test").Spec.Replicas != 1 {
		t.Fatal("formal rollback changed preserved test sender")
	}
	assertSelector(t, r, "notification-manager")
	setPhase(t, r, &h, "test-restored")
	if err := reconcileHandoffTest(r); err != nil {
		t.Fatal(err)
	}
	if *getDep(t, r, "new-nm").Spec.Replicas != 0 || !reflect.DeepEqual(before.Spec, getDep(t, r, "preserved-test").Spec) || stateOf(t, r).Stage != "test-restored" {
		t.Fatal("test-restored resurrected the stopped formal sender or changed preserved test")
	}
}

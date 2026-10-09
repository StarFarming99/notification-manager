package controller

import (
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kcache "k8s.io/client-go/tools/cache"
)

func TestNamedConfigurationIgnoresOtherCRAndDeletionTombstones(t *testing.T) {
	c := &Controller{configurationName: "notification-manager", logger: log.NewNopLogger(), nmAdd: true, tenantKey: "production-tenant"}
	for _, obj := range []interface{}{&v2beta2.NotificationManager{ObjectMeta: metav1.ObjectMeta{Name: "unrelated"}}, kcache.DeletedFinalStateUnknown{Obj: &v2beta2.NotificationManager{ObjectMeta: metav1.ObjectMeta{Name: "unrelated"}}}} {
		task := &task{op: opDel, obj: obj, done: make(chan interface{})}
		c.nmChange(task)
		if !c.nmAdd || c.tenantKey != "production-tenant" {
			t.Fatal("foreign CR changed selected configuration")
		}
	}
	job := &task{op: opDel, obj: kcache.DeletedFinalStateUnknown{Obj: &v2beta2.NotificationManager{ObjectMeta: metav1.ObjectMeta{Name: "notification-manager"}}}, done: make(chan interface{})}
	c.nmChange(job)
	if c.nmAdd || c.tenantKey != defaultTenantKey {
		t.Fatal("selected CR deletion was ignored")
	}
}

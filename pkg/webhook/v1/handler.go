package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/aggregation"
	"github.com/kubesphere/notification-manager/pkg/constants"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/deliveryprofiles"
	"github.com/kubesphere/notification-manager/pkg/filter"
	"github.com/kubesphere/notification-manager/pkg/internal"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/notify"
	"github.com/kubesphere/notification-manager/pkg/route"
	"github.com/kubesphere/notification-manager/pkg/silence"
	"github.com/kubesphere/notification-manager/pkg/stage"
	"github.com/kubesphere/notification-manager/pkg/store"
	"github.com/kubesphere/notification-manager/pkg/template"
	"github.com/kubesphere/notification-manager/pkg/utils"
)

type HttpHandler struct {
	logger            log.Logger
	wkrTimeout        time.Duration
	notifierCtl       *controller.Controller
	alerts            *store.AlertStore
	admissionMu       sync.Mutex
	admissionFailures map[string]uint64
}

type response struct {
	Status  int
	Message string
}

func New(logger log.Logger, wkrTimeout time.Duration, ctl *controller.Controller, alerts *store.AlertStore) *HttpHandler {
	h := &HttpHandler{
		logger:      logger,
		wkrTimeout:  wkrTimeout,
		notifierCtl: ctl,
		alerts:      alerts,
	}
	return h
}

func (h *HttpHandler) Alert(w http.ResponseWriter, r *http.Request) {
	defer func() {
		_ = r.Body.Close()
	}()
	if h.rejectInstanceAdmission(w, false) {
		return
	}

	// Parse alerts sent through Alertmanager webhook, more detail please refer to
	// https://github.com/prometheus/alertmanager/blob/master/template/template.go#L231
	data := template.Data{}
	raw, readErr := io.ReadAll(io.LimitReader(r.Body, (2<<20)+1))
	if readErr != nil || len(raw) > 2<<20 {
		h.handle(w, &response{http.StatusRequestEntityTooLarge, "Invalid or oversized notification request"})
		return
	}
	if err := utils.JsonDecode(bytes.NewReader(raw), &data); err != nil {
		h.handle(w, &response{http.StatusBadRequest, err.Error()})
		return
	}

	//if alerts, err := utils.JsonMarshalIndent(data, "", "  "); err != nil {
	//	_ = level.Error(h.logger).Log("msg", "Failed to encode alerts:", "err", err)
	//} else {
	//	fmt.Println(string(alerts))
	//}

	if !h.alerts.Accepting() {
		w.Header().Set("Retry-After", "1")
		h.handle(w, &response{http.StatusServiceUnavailable, "Notification intake is stopping"})
		return
	}
	cluster := h.notifierCtl.GetCluster()
	for _, alert := range data.Alerts {
		if alert.Labels == nil {
			alert.Labels = template.KV{}
		}
		if alert.Annotations == nil {
			alert.Annotations = template.KV{}
		}
		if v := alert.Labels["cluster"]; v == "" {
			alert.Labels["cluster"] = cluster
		}

		if alert.Labels["alerttype"] == "metric" {
			alert.Annotations["alerttime"] = time.Now().Local().String()
		}

		alert.ID = utils.Hash(alert)
		if h.alerts.Durable != nil {
			continue
		}
		if err := h.alerts.Push(alert); err != nil {
			_ = level.Error(h.logger).Log("msg", "push alert error", "error", err.Error())
			w.Header().Set("Retry-After", "1")
			h.handle(w, &response{http.StatusServiceUnavailable, "Notification store unavailable; retry required"})
			return
		}
	}

	if h.alerts.Durable != nil {
		requestHash := spool.Hash(raw)
		key := r.Header.Get("Idempotency-Key")
		if snap, ok := deliveryprofiles.Snapshot(r.Context()); ok {
			if key == "" {
				var err error
				requestHash, err = CanonicalAMIntent(raw)
				if err != nil {
					h.handle(w, &response{http.StatusBadRequest, "invalid AM intent"})
					return
				}
				group, groupErr := CanonicalAMGroup(raw)
				if groupErr != nil {
					h.handle(w, &response{http.StatusBadRequest, "invalid AM group"})
					return
				}
				key = "am-group:" + group
			} else {
				key = "explicit:" + key
			}
			key = snap.ID + ":" + snap.Version + ":" + key
		}
		intake, err := h.acceptDurable(r.Context(), data.Alerts, nil, key, requestHash)
		if err != nil {
			groupHash, _ := CanonicalAMGroup(raw)
			h.recordAdmissionFailure(err, groupHash)
			w.Header().Set("Retry-After", "1")
			if errors.Is(err, spool.ErrAmbiguousGeneration) {
				w.Header().Set("X-NM-Rejection-Code", "instance_generation_ambiguous")
				h.handle(w, &response{http.StatusServiceUnavailable, "Notification generation is ambiguous across release; retry and reconciliation required"})
				return
			}
			h.handle(w, &response{http.StatusServiceUnavailable, "Notification frozen plan could not be persisted"})
			return
		}
		w.Header().Set("X-NM-Intake-ID", intake.ID)
	}
	h.handle(w, &response{http.StatusOK, "Notification request accepted"})
}

func (h *HttpHandler) ServeMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	h.admissionMu.Lock()
	for class, count := range h.admissionFailures {
		_, _ = fmt.Fprintf(w, "nm_notification_admission_failures_total{class=%q} %d\n", class, count)
	}
	h.admissionMu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	if h.alerts.Durable != nil {
		if status, err := h.alerts.Durable.Status(); err == nil {
			counts := status["counts"].(map[string]int)
			for _, state := range []string{spool.Pending, spool.Sending, spool.Delivered, spool.Retryable, spool.Unknown, spool.DeadLetter} {
				_, _ = fmt.Fprintf(w, "nm_notification_spool_targets{state=%q} %d\n", state, counts[state])
			}
			_, _ = fmt.Fprintf(w, "nm_notification_spool_active_targets %v\nnm_notification_spool_active_target_limit %v\nnm_notification_spool_stored_bytes %v\nnm_notification_spool_storage_byte_limit %v\n", status["active_targets"], status["active_target_limit"], status["stored_bytes"], status["storage_byte_limit"])
			_, _ = fmt.Fprintf(w, "nm_notification_spool_storage_write_failures_total %v\n", status["storage_write_failures"])
			if class, _ := status["last_storage_error"].(string); class != "" {
				_, _ = fmt.Fprintf(w, "nm_notification_spool_storage_write_failed{class=%q} 1\n", class)
			}
		}
	}
	if shadow := h.notifierCtl.GetJevShadow(); shadow != nil {
		for name, value := range shadow.RelayStatus() {
			_, _ = fmt.Fprintf(w, "nm_jev_relay_%s %d\n", name, value)
		}
	}
}

func (h *HttpHandler) ServeReload(w http.ResponseWriter, _ *http.Request) {
	h.handle(w, &response{http.StatusOK, "reload"})
}

func (h *HttpHandler) ServeHealthCheck(w http.ResponseWriter, _ *http.Request) {
	h.handle(w, &response{http.StatusOK, "health check"})
}

func (h *HttpHandler) ServeReadinessCheck(w http.ResponseWriter, _ *http.Request) {
	if !h.alerts.Accepting() {
		h.handle(w, &response{http.StatusServiceUnavailable, "Original notification intake stopping"})
		return
	}
	if h.rejectInstanceAdmission(w, false) {
		return
	}
	if profiles := h.notifierCtl.DeliveryProfiles; profiles != nil {
		// A broken/over-budget test receiver must not remove an active formal
		// backend from the original Service. Formal's dedicated readiness separately
		// validates full configuration and the guarded compatibility contract.
		if _, formalErr := profiles.Store.ProfileSnapshot("formal"); formalErr != nil {
			if snap, err := profiles.Store.ProfileSnapshot("test"); err == nil {
				if err := h.validateTestScope(snap.DeliveryProfile); err != nil {
					h.handle(w, &response{http.StatusServiceUnavailable, "Scoped test Receiver incompatible"})
					return
				}
			} else {
				h.handle(w, &response{http.StatusServiceUnavailable, "No delivery profile accepting"})
				return
			}
		}
	}
	if h.alerts.Durable != nil {
		var err error
		if profiles := h.notifierCtl.DeliveryProfiles; profiles != nil {
			lane := "test"
			if _, e := profiles.Store.ProfileSnapshot("formal"); e == nil {
				lane = "formal"
			}
			err = h.alerts.Durable.ReadyProfile(lane)
		} else {
			err = h.alerts.Durable.Ready()
		}
		if err != nil {
			h.handle(w, &response{http.StatusServiceUnavailable, "Original notification spool unavailable"})
			return
		}
	}
	h.handle(w, &response{http.StatusOK, "ready"})
}

func (h *HttpHandler) ServeStatus(w http.ResponseWriter, _ *http.Request) {
	if h.alerts.Durable != nil {
		status, err := h.alerts.Durable.Status()
		if err != nil {
			h.handle(w, &response{http.StatusServiceUnavailable, "Original notification spool status unavailable"})
			return
		}
		closing, mode, deadline := h.alerts.ShutdownState()
		status["admission_failures"] = h.admissionFailureSnapshot()
		status["accepting"] = !closing
		status["shutdown_mode"] = mode
		status["shutdown_deadline"] = deadline
		if shadow := h.notifierCtl.GetJevShadow(); shadow != nil {
			status["jev_relay"] = shadow.RelaySnapshot()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
		return
	}
	status := map[string]interface{}{"accepting": h.alerts.Accepting(), "admission_failures": h.admissionFailureSnapshot()}
	if shadow := h.notifierCtl.GetJevShadow(); shadow != nil {
		status["jev_relay"] = shadow.RelaySnapshot()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

func (h *HttpHandler) handle(w http.ResponseWriter, resp *response) {
	bytes, _ := utils.JsonMarshal(resp)
	msg := string(bytes[:])
	w.WriteHeader(resp.Status)
	_, _ = io.WriteString(w, msg)

	if resp.Status != http.StatusOK {
		_ = level.Error(h.logger).Log("msg", resp.Message)
	} else {
		_ = level.Debug(h.logger).Log("msg", resp.Message)
	}
}

func (h *HttpHandler) ListReceivers(w http.ResponseWriter, r *http.Request) {

	_ = r.ParseForm()
	bs, _ := utils.JsonMarshalIndent(h.notifierCtl.ListReceiver(r.FormValue("tenant"), r.FormValue("type")), "", "  ")
	_, _ = w.Write(bs)
	return
}

func (h *HttpHandler) ListConfigs(w http.ResponseWriter, r *http.Request) {

	_ = r.ParseForm()
	bs, _ := utils.JsonMarshalIndent(h.notifierCtl.ListConfig(r.FormValue("tenant"), r.FormValue("type")), "", "  ")
	_, _ = w.Write(bs)
	return
}

func (h *HttpHandler) ListReceiverWithConfig(w http.ResponseWriter, r *http.Request) {

	_ = r.ParseForm()
	bs, _ := utils.JsonMarshalIndent(h.notifierCtl.ListReceiverWithConfig(r.FormValue("tenant"), r.FormValue("name"), r.FormValue("type")), "", "  ")
	_, _ = w.Write(bs)
	return
}

func (h *HttpHandler) Verify(w http.ResponseWriter, r *http.Request) {
	if !spool.HasSynchronousIngress(r.Context(), h.alerts.Durable) && h.rejectInstanceAdmission(w, true) {
		return
	}

	m := make(map[string]interface{})
	if err := utils.JsonDecode(io.LimitReader(r.Body, 2<<20), &m); err != nil {
		h.handle(w, &response{http.StatusBadRequest, err.Error()})
		return
	}

	receivers, err := h.getReceiversFromRequest(m)
	if err != nil {
		h.handle(w, &response{http.StatusBadRequest, err.Error()})
		return
	}

	alerts := []*template.Alert{
		{
			Labels: template.KV{
				constants.AlertName: constants.Verify,
				constants.AlertType: constants.Verify,
				constants.AlertTime: time.Now().Local().String(),
			},
			Annotations: template.KV{
				constants.AlertMessage: "Congratulations, your notification configuration is correct!",
			},
			StartsAt: time.Now(),
			EndsAt:   time.Now(),
		},
	}

	if err := h.send(r.Context(), receivers, alerts, constants.Verify); err != nil {
		h.sendFailure(w, err)
		return
	}
	h.handle(w, &response{http.StatusOK, "Verify successfully"})
}

func (h *HttpHandler) Notification(w http.ResponseWriter, r *http.Request) {
	if !spool.HasSynchronousIngress(r.Context(), h.alerts.Durable) && h.rejectInstanceAdmission(w, true) {
		return
	}

	m := make(map[string]interface{})
	if err := utils.JsonDecode(io.LimitReader(r.Body, 2<<20), &m); err != nil {
		h.handle(w, &response{http.StatusBadRequest, err.Error()})
		return
	}

	receivers, err := h.getReceiversFromRequest(m)
	if err != nil {
		h.handle(w, &response{http.StatusBadRequest, err.Error()})
		return
	}

	d := template.Data{}
	alert, ok := m["alert"]
	if !ok {
		h.handle(w, &response{http.StatusBadRequest, "alert is nil"})
		return
	}

	bs, err := utils.JsonMarshal(alert)
	if err != nil {
		h.handle(w, &response{http.StatusBadRequest, err.Error()})
		return
	}

	if err := utils.JsonUnmarshal(bs, &d); err != nil {
		h.handle(w, &response{http.StatusBadRequest, err.Error()})
		return
	}

	if err := h.send(r.Context(), receivers, d.Alerts, constants.Notification); err != nil {
		h.sendFailure(w, err)
		return
	}
	h.handle(w, &response{http.StatusOK, "Send alerts successfully"})
}

func (h *HttpHandler) getReceiversFromRequest(m map[string]interface{}) ([]internal.Receiver, error) {

	nr := v2beta2.Receiver{}
	if _, ok := m["receiver"]; !ok {
		return nil, utils.Error("Receiver is nil")
	}

	rawReceiver, ok := m["receiver"].(map[string]interface{})
	if !ok {
		return nil, utils.Error("Receiver must be an object")
	}
	if err := utils.MapToStruct(rawReceiver, &nr); err != nil {
		return nil, err
	}

	var nc *v2beta2.Config = nil
	if _, ok := m["config"]; ok {
		tmp := v2beta2.Config{}
		rawConfig, ok := m["config"].(map[string]interface{})
		if !ok {
			return nil, utils.Error("Config must be an object")
		}
		if err := utils.MapToStruct(rawConfig, &tmp); err != nil {
			return nil, err
		}
		nc = &tmp
	}

	receivers, err := h.notifierCtl.GenerateReceivers(&nr, nc)
	if err != nil {
		return nil, err
	}

	return receivers, nil
}

// Notification and Verify retain the original synchronous platform-result
// contract. Buffering an Alertmanager webhook does not authorize reporting
// these APIs successful before their actual platform request has completed.
func (h *HttpHandler) send(parent context.Context, receivers []internal.Receiver, alerts template.Alerts, seq string) error {
	ctx, cancel := context.WithTimeout(parent, h.wkrTimeout)
	ctx = context.WithValue(ctx, "seq", seq)
	defer cancel()

	pipeline := stage.MultiStage{aggregation.NewStage(h.notifierCtl)}
	val := make(map[internal.Receiver][]*template.Alert)
	for _, receiver := range receivers {
		val[receiver] = alerts
	}
	send := func() error {
		// Publication deadlines do not shorten the original caller/worker
		// budget of a still-authoritative sender.
		_, output, err := pipeline.Exec(ctx, h.logger, val)
		if err != nil {
			return err
		}
		if groups, ok := output.(map[internal.Receiver][]*template.Data); ok {
			for _, items := range groups {
				for _, data := range items {
					data.JevCaptureExcluded = true
				}
			}
		}
		_, _, err = notify.NewStage(h.notifierCtl).Exec(ctx, h.logger, output)
		return err
	}
	if spool.HasSynchronousIngress(parent, h.alerts.Durable) {
		snapshot, ok := deliveryprofiles.Snapshot(parent)
		if !ok {
			snapshot = nil
		}
		return h.alerts.Durable.WithSynchronousContextPermit(ctx, snapshot, send)
	}
	if snap, ok := deliveryprofiles.Snapshot(parent); ok && snap.ID == "formal" {
		if !h.alerts.Accepting() {
			return errors.New("notification intake is stopping")
		}
		// WithProfilePermit holds the instance and profile boundaries together.
		// Do not nest another instance RLock: a queued release writer could
		// otherwise deadlock a nested read lock during an in-flight API call.
		return h.alerts.Durable.WithProfilePermit(*snap, send)
	}
	if h.alerts.Durable != nil {
		return h.alerts.Durable.WithInstancePermit(true, send)
	}
	return send()
}

func (h *HttpHandler) rejectInstanceAdmission(w http.ResponseWriter, synchronous bool) bool {
	if h.alerts.Durable == nil {
		return false
	}
	if err := h.alerts.Durable.AdmissionAllowed(synchronous); err != nil {
		w.Header().Set("Retry-After", "1")
		h.handle(w, &response{http.StatusServiceUnavailable, "Instance release gate is not accepting this request; retry required"})
		return true
	}
	return false
}

func (h *HttpHandler) sendFailure(w http.ResponseWriter, err error) {
	if errors.Is(err, spool.ErrReleaseGate) || errors.Is(err, spool.ErrReleaseDeadline) {
		w.Header().Set("Retry-After", "1")
		h.handle(w, &response{http.StatusServiceUnavailable, "Instance release gate prevented platform sending; retry required"})
		return
	}
	h.handle(w, &response{http.StatusBadRequest, err.Error()})
}

// acceptDurable executes the original pipeline only as far as aggregation. The
// complete plan is committed once, before any external platform side effect.
func (h *HttpHandler) acceptDurable(parent context.Context, alerts template.Alerts, receivers []internal.Receiver, key, hash string) (result spool.Intake, failure error) {
	stageClass := "intake_admission"
	defer func() {
		if failure != nil {
			failure = &admissionFailure{stage: stageClass, err: failure}
		}
	}()
	if !h.alerts.Accepting() {
		return spool.Intake{}, errors.New("intake stopping")
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	// Pipeline stages consume the unnamed slice, including the silence stage.
	var input interface{} = []*template.Alert(alerts)
	pipeline := stage.MultiStage{}
	if receivers == nil {
		pipeline = append(pipeline, silence.NewStage(h.notifierCtl), route.NewStage(h.notifierCtl), filter.NewStage(h.notifierCtl))
	} else {
		m := map[internal.Receiver][]*template.Alert{}
		for _, receiver := range receivers {
			m[receiver] = alerts
		}
		input = m
	}
	pipeline = append(pipeline, aggregation.NewStage(h.notifierCtl))
	stageClass = "original_pipeline"
	_, output, err := pipeline.Exec(ctx, h.logger, input)
	if err != nil {
		return spool.Intake{}, err
	}
	plan := map[internal.Receiver][]*template.Data{}
	if output != nil {
		var ok bool
		plan, ok = output.(map[internal.Receiver][]*template.Data)
		if !ok {
			return spool.Intake{}, errors.New("unexpected frozen plan output")
		}
	}
	snap, profiled := deliveryprofiles.Snapshot(parent)
	if h.notifierCtl.DeliveryProfiles != nil && !profiled {
		return spool.Intake{}, errors.New("profiled intake requires a server-bound lane")
	}
	if profiled {
		plan = notify.ApplyProfile(plan, *snap)
	}
	if receivers != nil {
		for _, groups := range plan {
			for _, data := range groups {
				data.JevCaptureExcluded = true
			}
		}
	}
	stageClass = "template_or_credential_freeze"
	targets, err := notify.Freeze(h.logger, h.notifierCtl, plan)
	if err != nil {
		return spool.Intake{}, err
	}
	if profiled {
		stageClass = "profile_binding"
		targets, err = notify.BindFrozenProfile(targets, *snap)
		if err != nil {
			return spool.Intake{}, err
		}
	}
	var history []spool.Target
	if !profiled || snap.ID != "test" {
		stageClass = "history_freeze"
		history, err = notify.FreezeHistory(h.logger, h.notifierCtl, targets)
	}
	if err != nil {
		return spool.Intake{}, err
	}
	if profiled {
		history, err = notify.BindFrozenProfile(history, *snap)
		if err != nil {
			return spool.Intake{}, err
		}
	}
	targets = append(targets, history...)
	terminal := ""
	if len(targets) == 0 {
		terminal = "filtered_silenced_or_no_route"
	}
	stageClass = "spool_commit"
	if profiled {
		window := time.Duration(0)
		if strings.Contains(key, ":am-group:") {
			window = h.notifierCtl.DeliveryProfiles.RepeatInterval
			intake, _, err := h.alerts.Durable.SubmitAMProfile(key, hash, targets, terminal, *snap, window)
			return intake, err
		}
		intake, _, err := h.alerts.Durable.SubmitProfile(key, hash, targets, terminal, *snap, window)
		return intake, err
	}
	intake, _, err := h.alerts.Durable.Submit(key, hash, targets, terminal)
	return intake, err
}

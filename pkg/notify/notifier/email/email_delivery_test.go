package email_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/kubesphere/notification-manager/apis/v2beta2"
	"github.com/kubesphere/notification-manager/pkg/constants"
	"github.com/kubesphere/notification-manager/pkg/controller"
	"github.com/kubesphere/notification-manager/pkg/internal"
	emailtype "github.com/kubesphere/notification-manager/pkg/internal/email"
	spool "github.com/kubesphere/notification-manager/pkg/notification_spool"
	"github.com/kubesphere/notification-manager/pkg/notify"
	"github.com/kubesphere/notification-manager/pkg/template"
	"github.com/kubesphere/notification-manager/pkg/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type smtpFixture struct {
	listener net.Listener
	gate     chan struct{}
	reached  chan struct{}
	done     chan struct{}
	mu       sync.Mutex
	commands []string
	body     string
}

func newSMTPFixture(t *testing.T, mode string) *smtpFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &smtpFixture{listener: listener, gate: make(chan struct{}), reached: make(chan struct{}), done: make(chan struct{})}
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-f.gate:
		default:
			close(f.gate)
		}
		select {
		case <-f.done:
		case <-time.After(2 * time.Second):
			t.Error("SMTP fixture did not stop")
		}
	})
	go func() {
		defer close(f.done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if mode == "greeting-hang" {
			close(f.reached)
			<-f.gate
		}
		if _, err := fmt.Fprint(conn, "220 fixture ESMTP\r\n"); err != nil {
			return
		}
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			command := strings.TrimSpace(line)
			f.mu.Lock()
			f.commands = append(f.commands, command)
			f.mu.Unlock()
			switch {
			case strings.HasPrefix(command, "EHLO "), strings.HasPrefix(command, "HELO "), strings.HasPrefix(command, "MAIL FROM:"):
				_, _ = fmt.Fprint(conn, "250 fixture\r\n")
			case strings.HasPrefix(command, "RCPT TO:"):
				if mode == "rcpt-hang" {
					close(f.reached)
					<-f.gate
				}
				_, _ = fmt.Fprint(conn, "250 recipient\r\n")
			case command == "DATA":
				_, _ = fmt.Fprint(conn, "354 data\r\n")
				var body strings.Builder
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if line == ".\r\n" {
						break
					}
					body.WriteString(line)
				}
				f.mu.Lock()
				f.body = body.String()
				f.mu.Unlock()
				if mode == "response-loss" {
					return // Message accepted; its final acknowledgement is lost.
				}
				_, _ = fmt.Fprint(conn, "250 queued\r\n")
			case command == "QUIT":
				_, _ = fmt.Fprint(conn, "221 bye\r\n")
				return
			default:
				_, _ = fmt.Fprint(conn, "500 unexpected command\r\n")
			}
		}
	}()
	return f
}

func frozenEmail(t *testing.T, address net.Addr) (*controller.Controller, spool.Target) {
	t.Helper()
	fixtureURL := "http://unused.invalid"
	ctl, err := controller.NewStatic(context.Background(), log.NewNopLogger(), controller.StaticConfiguration{
		Environment: "test", Cluster: "local-smtp", Template: `{{define "body"}}original-email{{end}}{{define "title"}}original-title{{end}}`,
		Receivers: []v2beta2.Receiver{{ObjectMeta: metav1.ObjectMeta{Name: "fixture"}, Spec: v2beta2.ReceiverSpec{Webhook: &v2beta2.WebhookReceiver{URL: &fixtureURL}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	addr := address.(*net.TCPAddr)
	r := &emailtype.Receiver{
		Common: &internal.Common{Name: "original-email", Type: constants.Email, Template: internal.Template{TmplName: "body", TitleTmplName: "title", TmplType: constants.Text}},
		To:     []string{"recipient@example.test"},
		Config: &emailtype.Config{From: "sender@example.test", SmartHost: v2beta2.HostPort{Host: "127.0.0.1", Port: addr.Port}},
	}
	data := &template.Data{Alerts: template.Alerts{{ID: "fixture", Labels: template.KV{"alertname": "EmailCompatibility"}}}}
	plan, err := notify.Freeze(log.NewNopLogger(), ctl, map[internal.Receiver][]*template.Data{r: {data}})
	if err != nil || len(plan) != 1 {
		t.Fatal("email plan", plan, err)
	}
	return ctl, plan[0]
}

func TestFrozenEmailCancellationBeforeDataCannotResumeOrphanSend(t *testing.T) {
	for _, tc := range []struct {
		mode     string
		deadline bool
	}{{"greeting-hang", false}, {"rcpt-hang", false}, {"greeting-hang", true}, {"rcpt-hang", true}} {
		t.Run(fmt.Sprintf("%s/deadline=%t", tc.mode, tc.deadline), func(t *testing.T) {
			f := newSMTPFixture(t, tc.mode)
			ctl, target := frozenEmail(t, f.listener.Addr())
			var ctx context.Context
			var cancel context.CancelFunc
			if tc.deadline {
				ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
			} else {
				// No deadline: cancellation itself must close a connected socket.
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- notify.SendFrozen(ctx, log.NewNopLogger(), ctl, target) }()
			select {
			case <-f.reached:
			case <-time.After(time.Second):
				t.Fatal("SMTP did not reach the injected pre-DATA stall")
			}
			if !tc.deadline {
				cancel()
			}
			select {
			case err := <-result:
				state, _ := utils.DeliveryOutcome(err)
				if err == nil || state != spool.Retryable {
					t.Fatal("known unsent cancellation was not safely retryable", state, err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancelled SMTP worker did not finish")
			}
			close(f.gate) // The stalled SMTP server resumes after the attempt returned.
			select {
			case <-f.done:
			case <-time.After(time.Second):
				t.Fatal("orphan SMTP connection remained after cancellation")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			for _, command := range f.commands {
				if command == "DATA" {
					t.Fatal("cancelled worker sent DATA after it became retryable")
				}
			}
			if f.body != "" {
				t.Fatal("cancelled worker transmitted message content")
			}
		})
	}
}

func TestFrozenEmailRequiresFinalDataAcknowledgement(t *testing.T) {
	for _, tc := range []struct{ mode, want string }{{"healthy", spool.Delivered}, {"response-loss", spool.Unknown}} {
		t.Run(tc.mode, func(t *testing.T) {
			f := newSMTPFixture(t, tc.mode)
			ctl, target := frozenEmail(t, f.listener.Addr())
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := notify.SendFrozen(ctx, log.NewNopLogger(), ctl, target)
			state, _ := utils.DeliveryOutcome(err)
			if state != tc.want {
				t.Fatal("SMTP final response misclassified", state, tc.want, err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if !strings.Contains(f.body, "original-email") || !strings.Contains(f.body, "Subject: original-title") {
				t.Fatal("original email content/title missing", f.body)
			}
		})
	}
}

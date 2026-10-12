package email

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

func TestImplicitTLSHandshakeSharesAttemptDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		buffer := make([]byte, 4096)
		for {
			// Accept TLS bytes but never send a ServerHello.
			if _, err := conn.Read(buffer); err != nil {
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	conn, cleanup, err := dialSMTP(ctx, listener.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	if cleanup != nil {
		cleanup()
	}
	if err == nil || conn != nil || time.Since(started) > time.Second {
		t.Fatal("implicit TLS escaped its attempt deadline", conn, err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed-out TLS socket was not closed")
	}
}

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestLocalAPIStreamPreservesPrefixAndReply(t *testing.T) {
	client, proxy := net.Pipe()
	remote, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	const destination = "100.103.104.103"
	telemetry := newWANTelemetry()
	dial := func(ctx context.Context, host string, port uint16) (net.Conn, error) {
		if host != destination || port != 2252 {
			t.Errorf("wrong target %s:%d", host, port)
		}
		return remote, nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveLocalAPIConn(proxy, dial, destination, "2252", []byte("W1"), telemetry, 1)
	}()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		defer peer.Close()
		payload := make([]byte, len("W1hello"))
		if _, err := io.ReadFull(peer, payload); err != nil {
			t.Errorf("read remote payload: %v", err)
			return
		}
		if string(payload) != "W1hello" {
			t.Errorf("remote payload=%q", payload)
		}
		_, _ = peer.Write([]byte("world"))
	}()
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := client.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len("world"))
	if _, err := io.ReadFull(client, buffer); err != nil {
		t.Fatal(err)
	}
	if string(buffer) != "world" {
		t.Fatalf("reply=%q", buffer)
	}
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("LocalAPI stream did not close")
	}
	<-serverDone
	if got := telemetry.lanes[0].rxBytes.Load(); got != 5 {
		t.Errorf("WAN download accounting=%d", got)
	}
	if got := telemetry.lanes[0].txBytes.Load(); got != 5 {
		t.Errorf("WAN upload accounting=%d", got)
	}
}

func TestLocalAPIDialFailureDoesNotFallbackToSubprocess(t *testing.T) {
	client, proxy := net.Pipe()
	defer client.Close()
	telemetry := newWANTelemetry()
	dial := func(context.Context, string, uint16) (net.Conn, error) {
		return nil, errors.New("temporary peer failure")
	}
	serveLocalAPIConn(proxy, dial, "100.103.104.103", "2252", nil, telemetry, 2)
	if got := telemetry.lanes[1].fail.Load(); got != 1 {
		t.Fatalf("expected one failed dial; got %d", got)
	}
}

func TestLocalAPIRejectsInvalidPort(t *testing.T) {
	client, proxy := net.Pipe()
	defer client.Close()
	dial := func(context.Context, string, uint16) (net.Conn, error) {
		t.Fatal("dial should not run with an invalid port")
		return nil, nil
	}
	telemetry := newWANTelemetry()
	serveLocalAPIConn(proxy, dial, "100.103.104.103", strings.Repeat("9", 9), nil, telemetry, 1)
	if telemetry.lanes[0].fail.Load() != 1 {
		t.Fatal("invalid port must be counted as failure")
	}
}

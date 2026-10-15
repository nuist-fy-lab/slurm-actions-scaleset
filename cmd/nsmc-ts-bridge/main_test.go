package main

import (
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestChannelPrefix(t *testing.T) {
	for i := 0; i < 7; i++ {
		got, err := channelIndex([]byte{'W', byte('0' + i)})
		if err != nil || got != i {
			t.Fatalf("index %d => %d, %v", i, got, err)
		}
	}
	for _, invalid := range [][]byte{{}, {'X', '0'}, {'W', '7'}, {'W', '0', 'x'}} {
		if _, err := channelIndex(invalid); err == nil {
			t.Fatalf("accepted %q", invalid)
		}
	}
}

func TestServerDemultiplexesToPrivateGOST(t *testing.T) {
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	_, portStr, _ := net.SplitHostPort(target.Addr().String())
	base, _ := strconv.Atoi(portStr)
	out := make(chan string, 1)
	go func() {
		c, err := target.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 4)
		_, _ = io.ReadFull(c, buf)
		out <- string(buf)
		_, _ = c.Write([]byte("PONG"))
	}()
	frontend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer frontend.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := frontend.Accept()
		if err == nil {
			serveServerConn(c, "127.0.0.1", "127.0.0.1", base)
		}
	}()
	conn, err := net.Dial("tcp", frontend.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
	if _, err := conn.Write([]byte("W0PING")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 4)
	if _, err := io.ReadFull(conn, response); err != nil {
		t.Fatal(err)
	}
	if string(response) != "PONG" {
		t.Fatalf("response=%q", response)
	}
	if got := <-out; got != "PING" {
		t.Fatalf("unexpected payload %q", got)
	}
	_ = conn.Close()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("bridge did not terminate")
	}
}

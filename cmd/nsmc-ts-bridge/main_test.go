package main

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestOptionalWANPeersAndFallback(t *testing.T) {
	const main = "100.103.104.103:2252"
	filename := filepath.Join(t.TempDir(), "wan-peers.conf")
	check := func(channel int, selector uint64, wantAddr string, wantPrefix []byte) {
		t.Helper()
		addr, prefix := chooseRoute(channel, main, filename, selector)
		if addr != wantAddr || !reflect.DeepEqual(prefix, wantPrefix) {
			t.Fatalf("channel=%d selector=%d: addr=%q prefix=%q expected=%q %q", channel, selector, addr, prefix, wantAddr, wantPrefix)
		}
	}
	check(0, 0, main, []byte("W0")) // absent file preserves production behavior
	config := main + "\n100.76.5.2:2252\n-\n-\n-\n-\n"
	if err := os.WriteFile(filename, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	check(0, 0, main, []byte("W1"))
	check(0, 1, "100.76.5.2:2252", nil)
	check(0, 2, main, []byte("W1"))
	check(1, 0, main, []byte("W1"))
	check(2, 0, "100.76.5.2:2252", nil)
	check(3, 0, main, []byte("W3"))
	if err := os.WriteFile(filename, []byte(main+"\n198.51.100.2:2252\n-\n-\n-\n-\n"), 0600); err != nil {
		t.Fatal(err)
	}
	check(2, 0, main, []byte("W2")) // reject non-Tailscale endpoint
	if err := os.WriteFile(filename, []byte("broken\n100.76.5.2:2252\n-\n-\n-\n-\n"), 0600); err != nil {
		t.Fatal(err)
	}
	check(2, 0, main, []byte("W2")) // reject unexpected primary identity
}

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

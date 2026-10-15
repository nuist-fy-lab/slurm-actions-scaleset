// nsmc-ts-bridge joins a loopback SOCKS5 endpoint to a NanoPi GOST proxy.
// It transports TCP using the existing user-mode "tailscale nc" connection.
// The NanoPi server selects one of seven private GOST listeners using a
// two-byte prefix. No routing table or Tailscale daemon settings are changed.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const channels = 7

func main() {
	mode := flag.String("mode", "", "client on c2ln1 or server on NanoPi")
	localIP := flag.String("local-ip", "127.0.0.1", "client listening address")
	localBase := flag.Int("local-base", 12880, "client starting SOCKS5 port")
	remote := flag.String("remote", "100.103.104.103:2252", "NanoPi Tailscale destination")
	tailscale := flag.String("tailscale", "", "path to existing tailscale CLI")
	listen := flag.String("listen", "100.103.104.103:2252", "NanoPi listening address")
	allowFrom := flag.String("allow-from", "100.97.137.1", "allowed Tailscale client IP")
	gostHost := flag.String("gost-host", "127.0.0.1", "NanoPi GOST loopback address")
	gostBase := flag.Int("gost-base", 11880, "NanoPi GOST starting SOCKS5 port")
	flag.Parse()

	var err error
	switch *mode {
	case "client":
		if *tailscale == "" {
			log.Fatal("--tailscale is required on c2ln1")
		}
		if net.ParseIP(*localIP) == nil || !net.ParseIP(*localIP).IsLoopback() {
			log.Fatal("client must listen on a loopback IP")
		}
		err = runClient(*localIP, *localBase, *remote, *tailscale)
	case "server":
		if net.ParseIP(*allowFrom) == nil {
			log.Fatal("invalid allow-from IP")
		}
		err = runServer(*listen, *allowFrom, *gostHost, *gostBase)
	default:
		log.Fatal("use --mode=client or --mode=server")
	}
	if err != nil {
		log.Fatal(err)
	}
}

func channelIndex(prefix []byte) (int, error) {
	if len(prefix) != 2 || prefix[0] != 'W' || prefix[1] < '0' || prefix[1] > '6' {
		return 0, errors.New("invalid bridge channel")
	}
	return int(prefix[1] - '0'), nil
}

func copyBoth(client, upstream net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(upstream, client)
		if v, ok := upstream.(interface{ CloseWrite() error }); ok {
			_ = v.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, upstream)
		if v, ok := client.(interface{ CloseWrite() error }); ok {
			_ = v.CloseWrite()
		}
	}()
	wg.Wait()
}

func serveServerConn(client net.Conn, allowIP, gostHost string, gostBase int) {
	defer client.Close()
	host, _, err := net.SplitHostPort(client.RemoteAddr().String())
	if err != nil || !net.ParseIP(host).Equal(net.ParseIP(allowIP)) {
		return
	}
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	var prefix [2]byte
	if _, err := io.ReadFull(client, prefix[:]); err != nil {
		return
	}
	idx, err := channelIndex(prefix[:])
	if err != nil {
		return
	}
	dest := net.JoinHostPort(gostHost, strconv.Itoa(gostBase+idx))
	upstream, err := net.DialTimeout("tcp", dest, 8*time.Second)
	if err != nil {
		log.Printf("GOST channel %d unavailable: %v", idx, err)
		return
	}
	defer upstream.Close()
	_ = client.SetReadDeadline(time.Time{})
	copyBoth(client, upstream)
}

func runServer(listen, allowed, gostHost string, gostBase int) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Printf("NanoPi Go bridge %s => GOST %s:%d..%d", listen, gostHost, gostBase, gostBase+6)
	sem := make(chan struct{}, 96)
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		select {
		case sem <- struct{}{}:
			go func() { defer func() { <-sem }(); serveServerConn(c, allowed, gostHost, gostBase) }()
		default:
			_ = c.Close()
		}
	}
}

func serveClientConn(client net.Conn, channel int, remote, tailscale string) {
	defer client.Close()
	host, port, err := net.SplitHostPort(remote)
	if err != nil {
		return
	}
	cmd := exec.Command(tailscale, "nc", host, port)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	if _, err := stdin.Write([]byte{'W', byte('0' + channel)}); err != nil {
		_ = stdin.Close()
		return
	}
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(stdin, client)
		_ = stdin.Close()
		close(done)
	}()
	_, _ = io.Copy(client, stdout)
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
}

func runClient(ip string, base int, remote, tailscale string) error {
	if _, _, err := net.SplitHostPort(remote); err != nil {
		return err
	}
	listeners := make([]net.Listener, 0, channels)
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	sem := make(chan struct{}, 48)
	for i := 0; i < channels; i++ {
		ln, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(base+i)))
		if err != nil {
			return err
		}
		listeners = append(listeners, ln)
		go func(index int, ln net.Listener) {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				select {
				case sem <- struct{}{}:
					go func() { defer func() { <-sem }(); serveClientConn(c, index, remote, tailscale) }()
				default:
					_ = c.Close()
				}
			}
		}(i, ln)
	}
	log.Printf("c2ln1 Go bridge %s ports %d..%d via Tailscale to %s", ip, base, base+6, remote)
	// Server mode uses the accept loop; client mode remains alive indefinitely.
	if strings.TrimSpace(remote) == "" {
		return fmt.Errorf("empty NanoPi destination")
	}
	select {}
}

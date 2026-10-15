// nsmc-ts-bridge joins a loopback SOCKS5 endpoint to a NanoPi GOST proxy.
// It transports TCP using the existing user-mode "tailscale nc" connection.
// The NanoPi server selects one of seven private GOST listeners using a
// two-byte prefix. No routing table or Tailscale daemon settings are changed.
package main

import (
	"bytes"
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
	"sync/atomic"
	"time"
)

const channels = 7

var balancedPeerIndex atomic.Uint64

func main() {
	mode := flag.String("mode", "", "client on c2ln1 or server on NanoPi")
	localIP := flag.String("local-ip", "127.0.0.1", "client listening address")
	localBase := flag.Int("local-base", 12880, "client starting SOCKS5 port")
	remote := flag.String("remote", "100.103.104.103:2252", "NanoPi Tailscale destination")
	wanPeersFile := flag.String("wan-peers-file", "", "optional six-line WAN-to-Tailscale-peer file; read on each new connection")
	statsFile := flag.String("stats-file", "", "optional owner-only JSON file recording per-WAN transport telemetry")
	adaptive := flag.Bool("adaptive", false, "adaptively distribute channel 0 traffic based on measured WAN throughput and failures")
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
		err = runClient(*localIP, *localBase, *remote, *tailscale, *wanPeersFile, *statsFile, *adaptive)
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
	gate := newBridgeAdmission(maxServerSessions, 0, maxServerWaiters, admissionWait)
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() {
			release, ok := gate.acquire(false)
			if !ok {
				_ = c.Close()
				return
			}
			defer release()
			serveServerConn(c, allowed, gostHost, gostBase)
		}()
	}
}

// Peer 1 is the existing NanoPi Tailscale node and expects the W-prefix.
// Peer 2..6 are separate userspace Tailscale identities whose TCP Serve
// endpoints forward directly to their corresponding GOST SOCKS5 listener.
// The optional file is re-read on EVERY new TCP session. A missing or
// malformed file safely restores original single-path forwarding.
func chooseRoute(channel int, remote, peersFile string, rr uint64) (string, []byte) {
	fallback := func(index int) (string, []byte) {
		return remote, []byte{'W', byte('0' + index)}
	}
	if peersFile == "" {
		return fallback(channel)
	}
	data, err := os.ReadFile(peersFile)
	if err != nil {
		return fallback(channel)
	}
	entries := strings.Fields(string(data))
	if len(entries) != 6 || entries[0] != remote {
		return fallback(channel)
	}
	active := []int{1}
	for i := 1; i < 6; i++ {
		if entries[i] == "-" {
			continue
		}
		host, port, err := net.SplitHostPort(entries[i])
		ip := net.ParseIP(host)
		if err != nil || port != "2252" || ip == nil || ip.To4() == nil || !netipInTailnet(ip) {
			return fallback(channel)
		}
		active = append(active, i+1)
	}
	wan := channel
	if wan == 0 {
		wan = active[rr%uint64(len(active))]
	}
	if wan == 1 || entries[wan-1] == "-" {
		return fallback(wan)
	}
	return entries[wan-1], nil
}

func netipInTailnet(ip net.IP) bool {
	v4 := ip.To4()
	return v4 != nil && v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127
}

func serveClientConn(client net.Conn, channel int, remote, tailscale, peersFile string, telemetry *wanTelemetry, adaptive bool) {
	defer client.Close()
	seq := balancedPeerIndex.Add(1) - 1
	chosen, prefix := chooseRoute(channel, remote, peersFile, seq)
	if channel == 0 && adaptive && telemetry != nil {
		chosen, prefix = telemetry.pickRoute(remote, peersFile, seq)
	}
	wan := resolveWAN(channel, chosen, prefix, remote, peersFile)
	if telemetry != nil && wan >= 1 && wan <= 6 {
		telemetry.begin(wan)
		defer telemetry.end(wan)
	}
	host, port, err := net.SplitHostPort(chosen)
	if err != nil {
		if telemetry != nil {
			telemetry.failed(wan)
		}
		return
	}
	cmd := exec.Command(tailscale, "nc", host, port)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		if telemetry != nil {
			telemetry.failed(wan)
		}
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		if telemetry != nil {
			telemetry.failed(wan)
		}
		return
	}
	var ncStderr bytes.Buffer
	cmd.Stderr = &ncStderr
	started := time.Now()
	if err := cmd.Start(); err != nil {
		log.Printf("tailscale nc WAN %d failed to start: %v", wan, err)
		if telemetry != nil {
			telemetry.failed(wan)
		}
		return
	}
	gotReply := false
	defer func() {
		_ = cmd.Process.Kill()
		err := cmd.Wait()
		if !gotReply && err != nil {
			// The child only connects to known Tailscale peers; never print
			// user-supplied SOCKS requests, credentials or remote content.
			log.Printf("tailscale nc WAN %d failed before SOCKS reply: %v stderr=%q", wan, err, truncateDiagnostic(ncStderr.String()))
		}
	}()
	if len(prefix) != 0 {
		if _, err := stdin.Write(prefix); err != nil {
			if telemetry != nil {
				telemetry.failed(wan)
			}
			_ = stdin.Close()
			return
		}
	}
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&wanByteWriter{Writer: stdin, telemetry: telemetry, wan: wan, inbound: false}, client)
		_ = stdin.Close()
		close(done)
	}()
	reply := &wanByteWriter{Writer: client, telemetry: telemetry, wan: wan, inbound: true, started: started}
	bytes, _ := io.Copy(reply, stdout)
	gotReply = bytes > 0
	if telemetry != nil {
		if bytes > 0 {
			telemetry.succeeded(wan, reply.firstDelay)
		} else {
			telemetry.failed(wan)
		}
	}
	_ = client.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
}

func truncateDiagnostic(s string) string {
	s = strings.ReplaceAll(strings.TrimSpace(s), "\n", " ")
	if len(s) > 256 {
		s = s[:256]
	}
	return s
}

func runClient(ip string, base int, remote, tailscale, peersFile, statsFile string, adaptive bool) error {
	if _, _, err := net.SplitHostPort(remote); err != nil {
		return err
	}
	listeners := make([]net.Listener, 0, channels)
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	gate := newBridgeAdmission(maxClientSessions, maxOrdinarySessions, maxClientWaiters, admissionWait)
	var telemetry *wanTelemetry
	if statsFile != "" {
		telemetry = newWANTelemetry()
		telemetry.admission = gate
		go telemetry.publish(statsFile, peersFile, remote)
	}
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
				go func() {
					// The NSMC portal is pinned to vwan2 / port 12882.
					// Reserve 16 of 128 slots for this listener; its
					// regular transfers still obey the shared WAN gate.
					release, ok := gate.acquire(index == 2)
					if !ok {
						_ = c.Close()
						return
					}
					defer release()
					serveClientConn(c, index, remote, tailscale, peersFile, telemetry, adaptive)
				}()
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

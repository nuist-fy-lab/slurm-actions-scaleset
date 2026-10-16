// nsmc-ts-bridge joins a loopback SOCKS5 endpoint to a NanoPi GOST proxy.
// It transports TCP using the existing user-mode "tailscale nc" connection.
// The NanoPi server selects one of seven private GOST listeners using a
// two-byte prefix. No routing table or Tailscale daemon settings are changed.
package main

import (
	"bytes"
	"context"
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
	"syscall"
	"time"

	"tailscale.com/client/local"
)

const channels = 7

// Give a half-closed stream time to receive its final response, but never
// retain its upstream TCP socket indefinitely after the local SOCKS client
// has already gone away. This was exhausting the bridge connection pool.
const bridgeDrainGrace = 15 * time.Second

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
	transport := flag.String("transport", "nc", "Tailnet dial method: nc or localapi (client only)")
	tailscaleSocket := flag.String("tailscale-socket", "", "tailscaled LocalAPI Unix socket (required for localapi)")
	clientSessions := flag.Int("client-sessions", maxClientSessions, "maximum active SOCKS bridge sessions (32..256)")
	clientReserved := flag.Int("client-reserved", maxClientSessions-maxOrdinarySessions, "active session slots reserved for portal channel 2 (8..64)")
	clientWaiters := flag.Int("client-waiters", maxClientWaiters, "maximum queued sessions (16..128)")
	clientAdmissionTimeout := flag.Duration("client-admission-timeout", admissionWait, "maximum time queued before admission (5s..30s)")
	listen := flag.String("listen", "100.103.104.103:2252", "NanoPi listening address")
	allowFrom := flag.String("allow-from", "100.97.137.1", "allowed Tailscale client IP")
	gostHost := flag.String("gost-host", "127.0.0.1", "NanoPi GOST loopback address")
	gostBase := flag.Int("gost-base", 11880, "NanoPi GOST starting SOCKS5 port")
	tcpNotSentLowat := flag.Int("tcp-notsent-lowat", 0, "optional NanoPi bridge TCP_NOTSENT_LOWAT bytes (0 disables pacing; Linux only)")
	flag.Parse()

	var err error
	switch *mode {
	case "client":
		if *transport != "nc" && *transport != "localapi" {
			log.Fatal("--transport must be nc or localapi")
		}
		if *transport == "nc" && *tailscale == "" {
			log.Fatal("--tailscale is required for nc transport")
		}
		if *transport == "localapi" && *tailscaleSocket == "" {
			log.Fatal("--tailscale-socket is required for localapi transport")
		}
		if net.ParseIP(*localIP) == nil || !net.ParseIP(*localIP).IsLoopback() {
			log.Fatal("client must listen on a loopback IP")
		}
		capacity := clientAdmissionConfig{sessions: *clientSessions, reserved: *clientReserved, waiters: *clientWaiters, timeout: *clientAdmissionTimeout}
		if err := capacity.validate(); err != nil {
			log.Fatal(err)
		}
		err = runClient(*localIP, *localBase, *remote, *tailscale, *wanPeersFile, *statsFile, *adaptive, *transport, *tailscaleSocket, capacity)
	case "server":
		if net.ParseIP(*allowFrom) == nil {
			log.Fatal("invalid allow-from IP")
		}
		if *tcpNotSentLowat < 0 || *tcpNotSentLowat > 1024*1024 {
			log.Fatal("--tcp-notsent-lowat must be between 0 and 1048576")
		}
		err = runServer(*listen, *allowFrom, *gostHost, *gostBase, *tcpNotSentLowat)
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
	copyBothWithGrace(client, upstream, bridgeDrainGrace)
}

func copyBothWithGrace(client, upstream net.Conn, grace time.Duration) {
	var wg sync.WaitGroup
	finished := make(chan struct{}, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer func() { finished <- struct{}{} }()
		_, _ = io.Copy(upstream, client)
		if v, ok := upstream.(interface{ CloseWrite() error }); ok {
			_ = v.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		defer func() { finished <- struct{}{} }()
		_, _ = io.Copy(client, upstream)
		if v, ok := client.(interface{ CloseWrite() error }); ok {
			_ = v.CloseWrite()
		}
	}()
	<-finished
	select {
	case <-finished:
	case <-time.After(grace):
		// net.Conn.Close is safe from a different goroutine and unblocks
		// both io.Copy directions even if the peer never closes its stream.
		_ = client.Close()
		_ = upstream.Close()
		<-finished
	}
	wg.Wait()
}

// TCP_NOTSENT_LOWAT controls how far a kernel TCP writer can enqueue unsent
// bytes before it waits for progress. It does not set a throughput ceiling
// and does not alter Tailscale, WAN routing or GOST configuration.
// Linux TCP_NOTSENT_LOWAT is socket option 25 at IPPROTO_TCP.
const tcpNotSentLowatOption = 25

func tuneBridgeSendQueue(conn net.Conn, lowat int) error {
	if lowat == 0 {
		return nil
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return errors.New("bridge client is not TCP")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return err
	}
	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		sockErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotSentLowatOption, lowat)
	}); err != nil {
		return err
	}
	return sockErr
}

func serveServerConn(client net.Conn, allowIP, gostHost string, gostBase, tcpNotSentLowat int) {
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
	if err := tuneBridgeSendQueue(client, tcpNotSentLowat); err != nil {
		// Keep the connection alive if the kernel lacks this optional feature.
		log.Printf("bridge TCP pacing unavailable: %v", err)
	}
	copyBoth(client, upstream)
}

func runServer(listen, allowed, gostHost string, gostBase, tcpNotSentLowat int) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Printf("NanoPi Go bridge %s => GOST %s:%d..%d TCP_NOTSENT_LOWAT=%d", listen, gostHost, gostBase, gostBase+6, tcpNotSentLowat)
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
			serveServerConn(c, allowed, gostHost, gostBase, tcpNotSentLowat)
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

func serveClientConn(client net.Conn, channel int, remote, tailscale, peersFile string, telemetry *wanTelemetry, adaptive bool, transport string, lc *local.Client) {
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
	if transport == "localapi" {
		serveLocalAPIConn(client, lc.DialTCP, host, port, prefix, telemetry, wan)
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

// serveLocalAPIConn preserves the existing SOCKS byte stream, WAN accounting
// and W-prefix protocol without launching a separate tailscale nc process for
// every TCP connection. DialTCP is the stable Tailscale LocalAPI and uses the
// already authenticated userspace daemon and its existing WireGuard identity.
type tailnetDialer func(context.Context, string, uint16) (net.Conn, error)

func serveLocalAPIConn(client net.Conn, dial tailnetDialer, host, port string, prefix []byte, telemetry *wanTelemetry, wan int) {
	serveLocalAPIConnWithGrace(client, dial, host, port, prefix, telemetry, wan, bridgeDrainGrace)
}

func serveLocalAPIConnWithGrace(client net.Conn, dial tailnetDialer, host, port string, prefix []byte, telemetry *wanTelemetry, wan int, grace time.Duration) {
	portNum, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNum == 0 {
		if telemetry != nil {
			telemetry.failed(wan)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	started := time.Now()
	upstream, err := dial(ctx, host, uint16(portNum))
	if err != nil {
		if telemetry != nil {
			telemetry.failed(wan)
		}
		log.Printf("LocalAPI DialTCP WAN %d failed: %v", wan, err)
		return
	}
	defer upstream.Close()
	if len(prefix) != 0 {
		if _, err := upstream.Write(prefix); err != nil {
			if telemetry != nil {
				telemetry.failed(wan)
			}
			return
		}
	}
	done := make(chan struct{})
	responseDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(&wanByteWriter{Writer: upstream, telemetry: telemetry, wan: wan, inbound: false}, client)
		if half, ok := upstream.(interface{ CloseWrite() error }); ok {
			_ = half.CloseWrite()
		}
		// A SOCKS client may disconnect without the remote GOST/Tailnet
		// stream ever delivering EOF. Without a bounded drain the proxy
		// goroutine, WAN active slot and remote TCP session all leak.
		select {
		case <-responseDone:
		case <-time.After(grace):
			_ = upstream.Close()
		}
		close(done)
	}()
	reply := &wanByteWriter{Writer: client, telemetry: telemetry, wan: wan, inbound: true, started: started}
	n, _ := io.Copy(reply, upstream)
	close(responseDone)
	if telemetry != nil {
		if n > 0 {
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

func runClient(ip string, base int, remote, tailscale, peersFile, statsFile string, adaptive bool, transport, socket string, capacity clientAdmissionConfig) error {
	if _, _, err := net.SplitHostPort(remote); err != nil {
		return err
	}
	listeners := make([]net.Listener, 0, channels)
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	gate := newBridgeAdmission(capacity.sessions, capacity.sessions-capacity.reserved, capacity.waiters, capacity.timeout)
	var lc *local.Client
	if transport == "localapi" {
		// Never fall back to a machine-wide or GUI Tailscale socket.
		lc = &local.Client{Socket: socket, UseSocketOnly: true}
	}
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
					serveClientConn(c, index, remote, tailscale, peersFile, telemetry, adaptive, transport, lc)
				}()
			}
		}(i, ln)
	}
	log.Printf("c2ln1 Go bridge %s ports %d..%d via Tailscale %s to %s: sessions=%d reserved=%d waiters=%d wait=%v", ip, base, base+6, transport, remote, capacity.sessions, capacity.reserved, capacity.waiters, capacity.timeout)
	// Server mode uses the accept loop; client mode remains alive indefinitely.
	if strings.TrimSpace(remote) == "" {
		return fmt.Errorf("empty NanoPi destination")
	}
	select {}
}

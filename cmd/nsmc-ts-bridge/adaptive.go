package main

import (
	"encoding/json"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type wanCounter struct {
	active  atomic.Int64
	rxBytes atomic.Uint64 // NanoPi -> c2ln1: downloading
	txBytes atomic.Uint64
	ok      atomic.Uint64
	fail    atomic.Uint64
	setupMS atomic.Uint64
}

type WANStats struct {
	WAN        int     `json:"wan"`
	Peer       string  `json:"peer"`
	Active     int64   `json:"active"`
	RXBytes    uint64  `json:"rx_bytes"`
	TXBytes    uint64  `json:"tx_bytes"`
	RXMbps     float64 `json:"rx_mbps"`
	EWMAMbps   float64 `json:"ewma_mbps"`
	Weight     int     `json:"weight"`
	Successes  uint64  `json:"successes"`
	Failures   uint64  `json:"failures"`
	AvgSetupMS uint64  `json:"avg_setup_ms"`
}

type WANReport struct {
	UpdatedAt string     `json:"updated_at"`
	Mode      string     `json:"mode"`
	Lanes     []WANStats `json:"lanes"`
	Admission *admissionSnapshot `json:"admission,omitempty"`
}

type wanTelemetry struct {
	lanes      [6]wanCounter
	admission *bridgeAdmission
	mu         sync.RWMutex
	weights    [6]int
	ewma       [6]float64
	instant    [6]float64
	lastRX     [6]uint64
	lastFail   [6]uint64
	lastSample time.Time
}

func newWANTelemetry() *wanTelemetry {
	t := &wanTelemetry{lastSample: time.Now()}
	for i := 0; i < 6; i++ {
		t.weights[i] = 1
	}
	return t
}

func (t *wanTelemetry) begin(wan int) {
	if wan >= 1 && wan <= 6 {
		t.lanes[wan-1].active.Add(1)
	}
}
func (t *wanTelemetry) end(wan int) {
	if wan >= 1 && wan <= 6 {
		t.lanes[wan-1].active.Add(-1)
	}
}
func (t *wanTelemetry) succeeded(wan int, duration time.Duration) {
	if wan < 1 || wan > 6 {
		return
	}
	lane := &t.lanes[wan-1]
	lane.ok.Add(1)
	lane.setupMS.Add(uint64(duration.Milliseconds()))
}
func (t *wanTelemetry) failed(wan int) {
	if wan >= 1 && wan <= 6 {
		t.lanes[wan-1].fail.Add(1)
	}
}

type wanByteWriter struct {
	Writer     io.Writer
	telemetry  *wanTelemetry
	wan        int
	inbound    bool
	started    time.Time
	firstByte  bool
	firstDelay time.Duration
}

func (w *wanByteWriter) Write(b []byte) (int, error) {
	n, err := w.Writer.Write(b)
	if w.telemetry != nil && w.wan >= 1 && w.wan <= 6 && n > 0 {
		if w.inbound && !w.firstByte {
			w.firstByte = true
			w.firstDelay = time.Since(w.started)
		}
		if w.inbound {
			w.telemetry.lanes[w.wan-1].rxBytes.Add(uint64(n))
		} else {
			w.telemetry.lanes[w.wan-1].txBytes.Add(uint64(n))
		}
	}
	return n, err
}

func loadPeers(path, primary string) []string {
	if path == "" {
		return nil
	}
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	entries := strings.Fields(string(bytes))
	if len(entries) != 6 || entries[0] != primary {
		return nil
	}
	for _, entry := range entries[1:] {
		if entry == "-" {
			continue
		}
		host, port, err := net.SplitHostPort(entry)
		if err != nil || port != "2252" || !netipInTailnet(net.ParseIP(host)) {
			return nil
		}
	}
	return entries
}

func resolveWAN(channel int, chosen string, prefix []byte, primary, peersFile string) int {
	if channel >= 1 && channel <= 6 {
		return channel
	}
	if len(prefix) == 2 && prefix[0] == 'W' && prefix[1] >= '1' && prefix[1] <= '6' {
		return int(prefix[1] - '0')
	}
	if entries := loadPeers(peersFile, primary); entries != nil {
		for i, entry := range entries {
			if entry == chosen {
				return i + 1
			}
		}
	}
	return 1
}

func weightedWAN(entries []string, weights [6]int, rr uint64) int {
	total := uint64(0)
	for i, entry := range entries {
		if entry == "-" {
			continue
		}
		if weights[i] > 0 {
			total += uint64(weights[i])
		}
	}
	if total == 0 {
		return 1
	}
	n := rr % total
	for i, entry := range entries {
		if entry == "-" || weights[i] <= 0 {
			continue
		}
		if n < uint64(weights[i]) {
			return i + 1
		}
		n -= uint64(weights[i])
	}
	return 1
}

func (t *wanTelemetry) pickRoute(primary, peersFile string, rr uint64) (string, []byte) {
	entries := loadPeers(peersFile, primary)
	if entries == nil {
		return chooseRoute(0, primary, peersFile, rr)
	}
	t.mu.RLock()
	w := t.weights
	t.mu.RUnlock()
	index := weightedWAN(entries, w, rr)
	if index == 1 {
		return primary, []byte("W1")
	}
	return entries[index-1], nil
}

func (t *wanTelemetry) sample(now time.Time, peers []string) WANReport {
	t.mu.Lock()
	defer t.mu.Unlock()
	dt := now.Sub(t.lastSample).Seconds()
	if dt < 0.001 {
		dt = 15
	}
	results := make([]WANStats, 0, 6)
	for i := 0; i < 6; i++ {
		lane := &t.lanes[i]
		rx := lane.rxBytes.Load()
		instant := math.Max(0, float64(rx-t.lastRX[i])*8/(dt*1e6))
		if rx < t.lastRX[i] {
			instant = 0
		}
		active := lane.active.Load()
		if active > 0 || instant > 0 {
			t.ewma[i] = t.ewma[i]*0.65 + instant*0.35
		} else {
			t.ewma[i] *= 0.99
		}
		fail := lane.fail.Load()
		recentFailures := fail - t.lastFail[i]
		weight := 1 + int(math.Min(7, math.Round(t.ewma[i]/8)))
		if recentFailures > 0 {
			weight /= 1 + int(math.Min(4, float64(recentFailures)))
			if weight < 1 {
				weight = 1
			}
		}
		if active >= 8 && weight > 1 {
			weight = 1 + (weight-1)/2
		}
		if peers != nil && peers[i] == "-" {
			weight = 0
		}
		t.weights[i] = weight
		t.instant[i] = instant
		t.lastRX[i] = rx
		t.lastFail[i] = fail
		peer := ""
		if peers != nil {
			peer = peers[i]
		}
		success := lane.ok.Load()
		average := uint64(0)
		if success > 0 {
			average = lane.setupMS.Load() / success
		}
		results = append(results, WANStats{WAN: i + 1, Peer: peer, Active: active, RXBytes: rx, TXBytes: lane.txBytes.Load(), RXMbps: instant, EWMAMbps: t.ewma[i], Weight: weight, Successes: success, Failures: fail, AvgSetupMS: average})
	}
	t.lastSample = now
	report := WANReport{UpdatedAt: now.UTC().Format(time.RFC3339Nano), Mode: "adaptive-ewma-weighted", Lanes: results}
	if t.admission != nil {
		snapshot := t.admission.snapshot()
		report.Admission = &snapshot
	}
	return report
}

func saveReport(path string, report WANReport) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".wan-stats-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := f.Chmod(0600); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func (t *wanTelemetry) publish(path, peersFile, primary string) {
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for now := range tick.C {
		if err := saveReport(path, t.sample(now, loadPeers(peersFile, primary))); err != nil {
			// Path only: do not log remote metadata.
			_ = err
		}
	}
}

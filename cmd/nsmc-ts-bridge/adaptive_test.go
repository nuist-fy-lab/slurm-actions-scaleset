package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAdaptiveWeightedSelection(t *testing.T) {
	peers := []string{"100.103.104.103:2252", "100.115.22.10:2252", "100.66.158.24:2252", "100.99.222.81:2252", "100.85.170.120:2252", "100.89.200.5:2252"}
	weights := [6]int{1, 4, 1, 1, 1, 1}
	seen := map[int]int{}
	for i := uint64(0); i < 9; i++ {
		seen[weightedWAN(peers, weights, i)]++
	}
	if seen[2] != 4 || seen[1] != 1 || len(seen) != 6 {
		t.Fatalf("unexpected weighted distribution: %v", seen)
	}
	peers[2] = "-"
	for i := uint64(0); i < 60; i++ {
		if weightedWAN(peers, weights, i) == 3 {
			t.Fatal("disabled lane was selected")
		}
	}
	file := filepath.Join(t.TempDir(), "peers")
	if err := os.WriteFile(file, []byte("100.103.104.103:2252\n100.115.22.10:2252\n-\n-\n-\n-\n"), 0600); err != nil {
		t.Fatal(err)
	}
	stats := newWANTelemetry()
	if got, prefix := stats.pickRoute("100.103.104.103:2252", file, 1); got != "100.115.22.10:2252" || len(prefix) != 0 {
		t.Fatalf("adaptive routing failed: %s %q", got, prefix)
	}
}

func TestMeasuredWANRateAndCounters(t *testing.T) {
	stats := newWANTelemetry()
	stats.lastSample = time.Now().Add(-15 * time.Second)
	stats.begin(2)
	stats.lanes[1].rxBytes.Add(30000000)
	stats.lanes[1].txBytes.Add(500000)
	stats.succeeded(2, 90*time.Millisecond)
	report := stats.sample(time.Now(), []string{"main", "wan2", "-", "-", "-", "-"})
	if len(report.Lanes) != 6 || report.Lanes[1].RXMbps < 15 || report.Lanes[1].Weight <= 1 || report.Lanes[1].Active != 1 {
		t.Fatalf("invalid throughput sample: %+v", report.Lanes[1])
	}
	if report.Lanes[1].AvgSetupMS != 90 || report.Lanes[2].Weight != 0 {
		t.Fatalf("invalid latency/disable: %+v", report.Lanes)
	}
	stats.failed(2)
	stats.end(2)
	report = stats.sample(time.Now().Add(15*time.Second), nil)
	if report.Lanes[1].Failures != 1 || report.Lanes[1].Active != 0 {
		t.Fatalf("invalid failure counters: %+v", report.Lanes[1])
	}
	file := filepath.Join(t.TempDir(), "stats.json")
	if err := saveReport(file, report); err != nil {
		t.Fatal(err)
	}
	mode, _ := os.Stat(file)
	if mode.Mode().Perm() != 0600 {
		t.Fatal("stats file permissions too wide")
	}
	data, _ := os.ReadFile(file)
	var reloaded WANReport
	if err := json.Unmarshal(data, &reloaded); err != nil || len(reloaded.Lanes) != 6 {
		t.Fatalf("invalid JSON stats: %v", err)
	}
}

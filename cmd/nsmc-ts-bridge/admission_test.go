package main

import (
	"testing"
	"time"
)

func TestClientAdmissionConfigValidation(t *testing.T) {
	base := clientAdmissionConfig{sessions: 160, reserved: 16, waiters: 96, timeout: 20 * time.Second}
	if err := base.validate(); err != nil {
		t.Fatalf("valid capacity probe: %v", err)
	}
	invalid := []clientAdmissionConfig{
		{sessions: 0, reserved: 16, waiters: 96, timeout: 20 * time.Second},
		{sessions: 257, reserved: 16, waiters: 96, timeout: 20 * time.Second},
		{sessions: 160, reserved: 0, waiters: 96, timeout: 20 * time.Second},
		{sessions: 160, reserved: 160, waiters: 96, timeout: 20 * time.Second},
		{sessions: 160, reserved: 16, waiters: 200, timeout: 20 * time.Second},
		{sessions: 160, reserved: 16, waiters: 96, timeout: 45 * time.Second},
	}
	for _, config := range invalid {
		if err := config.validate(); err == nil {
			t.Fatalf("accepted invalid capacity: %+v", config)
		}
	}
}

func TestCapacityProbePreservesPortalReservation(t *testing.T) {
	c := clientAdmissionConfig{sessions: 160, reserved: 16, waiters: 96, timeout: 5 * time.Second}
	g := newBridgeAdmission(c.sessions, c.sessions-c.reserved, c.waiters, c.timeout)
	release := make([]func(), 0, 160)
	for i := 0; i < c.sessions-c.reserved; i++ {
		unlock, ok := g.acquire(false)
		if !ok {
			t.Fatalf("ordinary admission rejected at %d", i)
		}
		release = append(release, unlock)
	}
	for i := 0; i < c.reserved; i++ {
		unlock, ok := g.acquire(true)
		if !ok {
			t.Fatalf("reserved priority slot unavailable at %d", i)
		}
		release = append(release, unlock)
	}
	if snap := g.snapshot(); snap.Active != 160 || snap.LimitOrdinary != 144 {
		t.Fatalf("probe limits are not applied: %+v", snap)
	}
	for _, f := range release {
		f()
	}
	if g.snapshot().Active != 0 {
		t.Fatal("leaked active connection slots")
	}
}

func TestBridgeAdmissionReservesPriorityCapacity(t *testing.T) {
	g := newBridgeAdmission(3, 2, 1, 25*time.Millisecond)
	first, ok := g.acquire(false)
	if !ok {
		t.Fatal("first ordinary connection rejected")
	}
	defer first()
	second, ok := g.acquire(false)
	if !ok {
		t.Fatal("second ordinary connection rejected")
	}
	defer second()
	if _, ok := g.acquire(false); ok {
		t.Fatal("ordinary took reserved capacity")
	}
	priority, ok := g.acquire(true)
	if !ok {
		t.Fatal("priority connection could not take reserved capacity")
	}
	defer priority()
	s := g.snapshot()
	if s.Active != 3 || s.LimitOrdinary != 2 || s.TimedOut != 1 {
		t.Fatalf("unexpected snapshot: %+v", s)
	}
}

func TestBridgeAdmissionBoundedQueueAndRelease(t *testing.T) {
	g := newBridgeAdmission(1, 0, 1, 500*time.Millisecond)
	release, ok := g.acquire(false)
	if !ok {
		t.Fatal("first slot unavailable")
	}
	result := make(chan bool, 1)
	go func() {
		unlock, admitted := g.acquire(false)
		if admitted {
			unlock()
		}
		result <- admitted
	}()
	deadline := time.After(time.Second)
	for g.snapshot().Queued != 1 {
		select {
		case <-deadline:
			t.Fatal("waiting client not queued")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if _, ok := g.acquire(false); ok {
		t.Fatal("queue overflow was accepted")
	}
	if g.snapshot().Rejected != 1 {
		t.Fatal("missing queue overflow count")
	}
	release()
	select {
	case admitted := <-result:
		if !admitted {
			t.Fatal("waiting client not admitted after release")
		}
	case <-time.After(time.Second):
		t.Fatal("waiting client blocked")
	}
	if g.snapshot().Active != 0 || g.snapshot().Queued != 0 {
		t.Fatal("admission tokens leaked")
	}
}

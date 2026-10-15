package main

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

// Keep conservative defaults for older installations and allow bounded,
// explicitly opted-in capacity probes without changing download A/C policy.
type clientAdmissionConfig struct {
	sessions int
	reserved int
	waiters  int
	timeout  time.Duration
}

func (c clientAdmissionConfig) validate() error {
	if c.sessions < 32 || c.sessions > 256 {
		return fmt.Errorf("client-sessions must be between 32 and 256")
	}
	if c.reserved < 8 || c.reserved > 64 || c.reserved >= c.sessions {
		return fmt.Errorf("client-reserved must be between 8 and 64 and less than client-sessions")
	}
	if c.waiters < 16 || c.waiters > 128 {
		return fmt.Errorf("client-waiters must be between 16 and 128")
	}
	if c.timeout < 5*time.Second || c.timeout > 30*time.Second {
		return fmt.Errorf("client-admission-timeout must be between 5s and 30s")
	}
	return nil
}

const (
	// FTP control/data sessions, HTTPS downloads and probes can overlap.
	// The old 48-slot hard cap closed new SOCKS clients immediately.
	maxClientSessions   = 128
	maxOrdinarySessions = 112
	maxClientWaiters    = 64
	maxServerSessions   = 160
	maxServerWaiters    = 64
	admissionWait       = 12 * time.Second
)

type bridgeAdmission struct {
	active   chan struct{}
	ordinary chan struct{}
	waiting  chan struct{}
	timeout  time.Duration
	rejected atomic.Uint64
	timedOut atomic.Uint64
}

type admissionSnapshot struct {
	Active        int    `json:"active"`
	Queued        int    `json:"queued"`
	LimitActive   int    `json:"limit_active"`
	LimitOrdinary int    `json:"limit_ordinary,omitempty"`
	LimitQueued   int    `json:"limit_queued"`
	Rejected      uint64 `json:"rejected"`
	TimedOut      uint64 `json:"timed_out"`
}

func newBridgeAdmission(active, ordinary, waiting int, timeout time.Duration) *bridgeAdmission {
	g := &bridgeAdmission{
		active:  make(chan struct{}, active),
		waiting: make(chan struct{}, waiting),
		timeout: timeout,
	}
	if ordinary > 0 {
		g.ordinary = make(chan struct{}, ordinary)
	}
	return g
}

// take never creates an unbounded number of pending goroutines. A token may
// wait for a bounded interval; overflow is explicitly counted and logged.
func (g *bridgeAdmission) take(token chan struct{}, deadline time.Time) (string, bool) {
	select {
	case token <- struct{}{}:
		return "", true
	default:
	}
	select {
	case g.waiting <- struct{}{}:
		defer func() { <-g.waiting }()
	default:
		g.rejected.Add(1)
		return "queue_full", false
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		g.timedOut.Add(1)
		return "queue_timeout", false
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case token <- struct{}{}:
		return "", true
	case <-timer.C:
		g.timedOut.Add(1)
		return "queue_timeout", false
	}
}

func (g *bridgeAdmission) acquire(priority bool) (func(), bool) {
	deadline := time.Now().Add(g.timeout)
	if !priority && g.ordinary != nil {
		if reason, ok := g.take(g.ordinary, deadline); !ok {
			log.Printf("bridge admission denied: %s ordinary=true", reason)
			return nil, false
		}
	}
	if reason, ok := g.take(g.active, deadline); !ok {
		if !priority && g.ordinary != nil {
			<-g.ordinary
		}
		log.Printf("bridge admission denied: %s priority=%t", reason, priority)
		return nil, false
	}
	return func() {
		<-g.active
		if !priority && g.ordinary != nil {
			<-g.ordinary
		}
	}, true
}

func (g *bridgeAdmission) snapshot() admissionSnapshot {
	a := admissionSnapshot{
		Active: len(g.active), Queued: len(g.waiting),
		LimitActive: cap(g.active), LimitQueued: cap(g.waiting),
		Rejected: g.rejected.Load(), TimedOut: g.timedOut.Load(),
	}
	if g.ordinary != nil {
		a.LimitOrdinary = cap(g.ordinary)
	}
	return a
}

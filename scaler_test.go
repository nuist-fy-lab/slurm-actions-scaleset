package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/actions/scaleset"
)

type completionSlurm struct {
	active      map[string]slurmJob
	cancelCalls int
}

func (s *completionSlurm) Submit(context.Context, string, string) (int, error) {
	panic("unexpected Slurm submission")
}

func (s *completionSlurm) Cancel(context.Context, int) error {
	s.cancelCalls++
	return nil
}

func (s *completionSlurm) Active(context.Context) (map[string]slurmJob, error) {
	return s.active, nil
}

func TestCompletedRunnerExitsNaturallyWithoutScancel(t *testing.T) {
	ctx := context.Background()
	slurm := &completionSlurm{
		active: map[string]slurmJob{"runner-one": {ID: 123, RunnerName: "runner-one"}},
	}
	s := &scaler{
		cfg:       config{StateDir: t.TempDir(), MaxRunners: 1},
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		slurm:     slurm,
		runners:   map[string]runnerState{"runner-one": {JobID: 123, Busy: true}},
		heartbeat: func() error { return nil },
	}
	if err := s.HandleJobCompleted(ctx, &scaleset.JobCompleted{RunnerName: "runner-one"}); err != nil {
		t.Fatal(err)
	}
	if slurm.cancelCalls != 0 {
		t.Fatal("completed GitHub runner was cancelled instead of exiting naturally")
	}
	if state, ok := s.runners["runner-one"]; !ok || !state.Busy || state.JobID != 123 {
		t.Fatalf("completed runner must retain the Slurm slot until exit: %#v", s.runners)
	}
	if count, err := s.HandleDesiredRunnerCount(ctx, 0); err != nil || count != 1 {
		t.Fatalf("active completed runner was prematurely released: count=%d err=%v", count, err)
	}
	if slurm.cancelCalls != 0 {
		t.Fatal("normal completion used the idle scale-down cancellation path")
	}
	// Once Slurm confirms process exit, the controller is free to reuse
	// capacity, without producing any scancel invocation.
	slurm.active = map[string]slurmJob{}
	if count, err := s.HandleDesiredRunnerCount(ctx, 0); err != nil || count != 0 {
		t.Fatalf("exited runner was not reaped: count=%d err=%v", count, err)
	}
	if slurm.cancelCalls != 0 {
		t.Fatal("naturally exited runner was cancelled")
	}
}

func TestIdleScaleDownStillCancelsUnusedRunner(t *testing.T) {
	ctx := context.Background()
	slurm := &completionSlurm{
		active: map[string]slurmJob{"idle-runner": {ID: 456, RunnerName: "idle-runner"}},
	}
	s := &scaler{
		cfg:       config{StateDir: t.TempDir(), MaxRunners: 1},
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		slurm:     slurm,
		runners:   map[string]runnerState{"idle-runner": {JobID: 456}},
		heartbeat: func() error { return nil },
	}
	if count, err := s.HandleDesiredRunnerCount(ctx, 0); err != nil || count != 0 {
		t.Fatalf("idle scale-down failed: count=%d err=%v", count, err)
	}
	if slurm.cancelCalls != 1 {
		t.Fatalf("unused runner should still be cancelled, got %d calls", slurm.cancelCalls)
	}
}

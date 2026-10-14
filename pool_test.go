package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPoolReusesOneSlurmAllocationForMultipleRunners(t *testing.T) {
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(tmp, "slurm-state")
	calls := filepath.Join(tmp, "sbatch-calls")
	t.Setenv("SQUEUE_STATE", state)
	t.Setenv("SBATCH_CALLS", calls)
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	for name, script := range map[string]string{
		"squeue": "#!/bin/sh\nif test -f \"$SQUEUE_STATE\"; then cat \"$SQUEUE_STATE\"; fi\n",
		"sbatch": "#!/bin/sh\nprintf 'call\\n' >> \"$SBATCH_CALLS\"\nprintf '123|ghpool-ghss-c2ln1-org-c2ln1\\n' > \"$SQUEUE_STATE\"\nprintf '123\\n'\n",
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := config{StateDir: filepath.Join(tmp, "state"), ScaleSetName: "c2ln1-org-c2ln1", Partition: "p1shr", CPUs: 4, Memory: "8G", PoolIdleHours: 72, PoolTimeLimit: "UNLIMITED", PoolScript: "/bin/false", RunnerScript: "/bin/true", RunnerArchive: "/tmp/archive"}
	p := &poolSlurm{cfg: cfg, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx := context.Background()
	if active, err := p.Active(ctx); err != nil || len(active) != 0 {
		t.Fatalf("no CI request must not allocate a warm Slurm slot: %v, %v", active, err)
	}
	if _, err := os.Stat(calls); !os.IsNotExist(err) {
		t.Fatalf("idle controller unexpectedly submitted Slurm job: %v", err)
	}
	if err := p.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"c2ln1-aabbccdd", "c2ln1-12345678"} {
		jitFile := filepath.Join(cfg.StateDir, "jit", name+".jit")
		if jobID, err := p.Submit(ctx, name, jitFile); err != nil || jobID != 123 {
			t.Fatalf("submit %s: id=%d err=%v", name, jobID, err)
		}
		active, err := p.Active(ctx)
		if err != nil || len(active) != 1 || active[name].ID != 123 {
			t.Fatalf("unexpected active runner: %v, %v", active, err)
		}
		request := filepath.Join(p.dir(), "requests", name+".req")
		running := strings.TrimSuffix(request, ".req") + ".running"
		if err := os.Rename(request, running); err != nil {
			t.Fatal(err)
		}
		if active, err := p.Active(ctx); err != nil || len(active) != 1 {
			t.Fatalf("running runner disappeared: %v %v", active, err)
		}
		if err := os.Remove(running); err != nil {
			t.Fatal(err)
		}
		if active, err := p.Active(ctx); err != nil || len(active) != 0 {
			t.Fatalf("completed runner retained capacity: %v %v", active, err)
		}
	}
	data, err := os.ReadFile(calls)
	if err != nil || strings.Count(string(data), "call") != 1 {
		t.Fatalf("expected exactly one sbatch allocation: %q %v", data, err)
	}
	if err := p.Cancel(ctx, 123); err == nil {
		t.Fatal("runner cancellation must never cancel a shared Slurm allocation")
	}
	if err := os.Remove(state); err != nil {
		t.Fatal(err)
	}
	if active, err := p.Active(ctx); err != nil || len(active) != 0 {
		t.Fatalf("expired pool must remain released without CI: %v %v", active, err)
	}
	data, err = os.ReadFile(calls)
	if err != nil || strings.Count(string(data), "call") != 1 {
		t.Fatalf("idle pool was automatically resubmitted: %q %v", data, err)
	}
	if _, err := p.Submit(ctx, "c2ln1-1234abcd", filepath.Join(cfg.StateDir, "jit", "c2ln1-1234abcd.jit")); err != nil {
		t.Fatalf("new CI after expiry should allocate another pool: %v", err)
	}
	data, err = os.ReadFile(calls)
	if err != nil || strings.Count(string(data), "call") != 2 {
		t.Fatalf("new CI must cause exactly one new allocation: %q %v", data, err)
	}
	if err := os.WriteFile(state, []byte("999|ghss-c2ln1-org-c2ln1-c2ln1-oldrunner\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := p.Ensure(ctx); err == nil {
		t.Fatal("a pending legacy one-shot allocation must block the pool cutover")
	}
}

func TestPoolEnforcesAuthorizedCapacity(t *testing.T) {
	cfg := config{RegistrationURL: "https://github.com/test", ScaleSetName: "pool", MaxRunners: 1, Partition: "p1shr", CPUs: 4, Memory: "8G", TimeLimit: "00:30:00", RunnerScript: "runner", PoolScript: "pool", PoolIdleHours: 72, PoolTimeLimit: "UNLIMITED", RunnerArchive: "runner.tar.gz", PoolMode: true}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Partition = "debug"
	if err := cfg.validate(); err == nil {
		t.Fatal("unauthorized partition was accepted")
	}
	cfg.Partition = "p1shr"
	cfg.MaxRunners = 2
	if err := cfg.validate(); err == nil {
		t.Fatal("pool must reject concurrent workers")
	}
}

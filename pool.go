package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// poolSlurm reserves one compute node allocation and dispatches ephemeral
// GitHub runners sequentially inside it. No CI steps run on the login node.
type poolSlurm struct {
	cfg    config
	logger *slog.Logger
	mu     sync.Mutex
}

func (p *poolSlurm) dir() string { return filepath.Join(p.cfg.StateDir, "pool") }

func (p *poolSlurm) jobName() string {
	return "ghpool-" + strings.TrimSuffix(slurmJobPrefix(p.cfg.ScaleSetName), "-")
}

func (p *poolSlurm) find(ctx context.Context) (int, error) {
	out, err := exec.CommandContext(ctx, "squeue", "-h", "-u", currentUser(), "-o", "%A|%j").Output()
	if err != nil {
		return 0, fmt.Errorf("list Slurm pool allocations: %w", err)
	}
	id := 0
	legacy := false
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.SplitN(line, "|", 2)
		if len(fields) != 2 {
			continue
		}
		if strings.HasPrefix(fields[1], slurmJobPrefix(p.cfg.ScaleSetName)) {
			legacy = true
		}
		if fields[1] != p.jobName() {
			continue
		}
		found, err := strconv.Atoi(fields[0])
		if err != nil {
			return 0, fmt.Errorf("invalid Slurm allocation ID: %w", err)
		}
		if id != 0 {
			return 0, errors.New("multiple pool allocations found; refusing to allocate more")
		}
		id = found
	}
	if legacy {
		return 0, errors.New("legacy one-shot Slurm runner still allocated; drain before enabling the pool")
	}
	return id, nil
}

func (p *poolSlurm) Ensure(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := p.ensureLocked(ctx)
	return err
}

func (p *poolSlurm) ensureLocked(ctx context.Context) (int, error) {
	id, err := p.find(ctx)
	if err != nil || id != 0 {
		return id, err
	}
	if err := os.MkdirAll(filepath.Join(p.dir(), "requests"), 0o700); err != nil {
		return 0, err
	}
	logDir := filepath.Join(p.cfg.StateDir, "slurm")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return 0, err
	}
	args := []string{
		"--parsable", "--job-name=" + p.jobName(),
		"--partition=" + p.cfg.Partition, "--nodes=1", "--ntasks=1",
		fmt.Sprintf("--cpus-per-task=%d", p.cfg.CPUs),
		"--mem=" + p.cfg.Memory, "--time=" + p.cfg.PoolTimeLimit,
		"--output=" + filepath.Join(logDir, "%j.out"),
		"--error=" + filepath.Join(logDir, "%j.err"),
		"--export=NONE", p.cfg.PoolScript,
		"--pool-dir", p.dir(), "--runner-script", p.cfg.RunnerScript,
		"--idle-seconds", strconv.Itoa(p.cfg.PoolIdleHours * 3600),
		"--runner-archive", p.cfg.RunnerArchive, "--state-dir", p.cfg.StateDir,
	}
	out, err := exec.CommandContext(ctx, "sbatch", args...).Output()
	if err != nil {
		return 0, fmt.Errorf("submit pool allocation: %w", err)
	}
	value := strings.TrimSpace(string(out))
	if before, _, ok := strings.Cut(value, ";"); ok {
		value = before
	}
	id, err = strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse pool allocation ID: %w", err)
	}
	p.logger.Info("pool allocation submitted", "slurmJobID", id, "partition", p.cfg.Partition, "cpus", p.cfg.CPUs, "memory", p.cfg.Memory)
	return id, nil
}

func (p *poolSlurm) Submit(ctx context.Context, name, jitFile string) (int, error) {
	if !strings.HasPrefix(name, "c2ln1-") || strings.ContainsAny(name, "/\\.") {
		return 0, fmt.Errorf("invalid runner name %q", name)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	id, err := p.ensureLocked(ctx)
	if err != nil {
		return 0, err
	}
	if filepath.Clean(jitFile) != filepath.Join(p.cfg.StateDir, "jit", name+".jit") {
		return 0, errors.New("runner JIT configuration has an unexpected path")
	}
	request := filepath.Join(p.dir(), "requests", name+".req")
	f, err := os.OpenFile(request, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, fmt.Errorf("queue runner in pool: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(request)
		return 0, err
	}
	p.logger.Info("queued ephemeral runner within pool", "runner", name, "slurmJobID", id)
	return id, nil
}

func (p *poolSlurm) Active(ctx context.Context) (map[string]slurmJob, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	id, err := p.find(ctx)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(p.dir(), "requests"))
	if os.IsNotExist(err) {
		return map[string]slurmJob{}, nil
	}
	if err != nil {
		return nil, err
	}
	active := make(map[string]slurmJob)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		var suffix string
		switch {
		case strings.HasSuffix(name, ".req"):
			suffix = ".req"
		case strings.HasSuffix(name, ".running"):
			suffix = ".running"
		default:
			continue
		}
		name = strings.TrimSuffix(name, suffix)
		if strings.HasPrefix(name, "c2ln1-") {
			active[name] = slurmJob{ID: id, RunnerName: name}
		}
	}
	// Recover a queued request if an allocation exited unexpectedly. In the
	// ordinary idle case (no work), do not allocate a replacement.
	if id == 0 && len(active) > 0 {
		id, err = p.ensureLocked(ctx)
		if err != nil {
			return nil, err
		}
		for name := range active {
			active[name] = slurmJob{ID: id, RunnerName: name}
		}
	}
	return active, nil
}

// Cancelling an individual GitHub runner must never cancel the shared pool.
// Newly submitted runners are busy until they exit, so this is defensive.
func (p *poolSlurm) Cancel(context.Context, int) error {
	return errors.New("cannot cancel a shared Slurm pool allocation via runner scale-down")
}

var _ slurmClient = (*poolSlurm)(nil)

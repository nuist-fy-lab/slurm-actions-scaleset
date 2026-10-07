package main

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

type slurmJob struct {
	ID         int
	RunnerName string
}

type slurmClient interface {
	Submit(ctx context.Context, runnerName, jitFile string) (int, error)
	Cancel(ctx context.Context, jobID int) error
	Active(ctx context.Context) (map[string]slurmJob, error)
}

type commandSlurm struct {
	cfg config
}

func (s *commandSlurm) Submit(ctx context.Context, runnerName, jitFile string) (int, error) {
	jobName := "ghss-" + runnerName
	args := []string{
		"--parsable",
		"--job-name=" + jobName,
		"--partition=" + s.cfg.Partition,
		"--nodes=1",
		"--ntasks=1",
		fmt.Sprintf("--cpus-per-task=%d", s.cfg.CPUs),
		"--mem=" + s.cfg.Memory,
		"--export=NONE",
		s.cfg.RunnerScript,
		"--runner-name", runnerName,
		"--jit-file", jitFile,
		"--runner-archive", s.cfg.RunnerArchive,
	}
	out, err := exec.CommandContext(ctx, "sbatch", args...).Output()
	if err != nil {
		return 0, fmt.Errorf("sbatch runner %s: %w", runnerName, err)
	}
	value := strings.TrimSpace(string(out))
	if before, _, ok := strings.Cut(value, ";"); ok {
		value = before
	}
	jobID, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse sbatch job id %q: %w", value, err)
	}
	return jobID, nil
}

func (s *commandSlurm) Cancel(ctx context.Context, jobID int) error {
	if jobID <= 0 {
		return nil
	}
	if err := exec.CommandContext(ctx, "scancel", strconv.Itoa(jobID)).Run(); err != nil {
		return fmt.Errorf("scancel %d: %w", jobID, err)
	}
	return nil
}

func (s *commandSlurm) Active(ctx context.Context) (map[string]slurmJob, error) {
	out, err := exec.CommandContext(ctx, "squeue", "-h", "-u", currentUser(), "-o", "%A|%j").Output()
	if err != nil {
		return nil, fmt.Errorf("list Slurm jobs: %w", err)
	}
	return parseActiveJobs(string(out)), nil
}

func parseActiveJobs(input string) map[string]slurmJob {
	jobs := make(map[string]slurmJob)
	scanner := bufio.NewScanner(strings.NewReader(input))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 2)
		if len(parts) != 2 || !strings.HasPrefix(parts[1], "ghss-") {
			continue
		}
		jobID, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		runnerName := strings.TrimPrefix(parts[1], "ghss-")
		jobs[runnerName] = slurmJob{ID: jobID, RunnerName: runnerName}
	}
	return jobs
}

func currentUser() string {
	if out, err := exec.Command("id", "-un").Output(); err == nil {
		return strings.TrimSpace(string(out))
	}
	return ""
}

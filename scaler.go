package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/google/uuid"
)

type runnerState struct {
	JobID int
	Busy  bool
}

type scaler struct {
	mu         sync.Mutex
	cfg        config
	logger     *slog.Logger
	scaleSetID int
	client     *scaleset.Client
	slurm      slurmClient
	runners    map[string]runnerState
	heartbeat  func() error
}

func newScaler(ctx context.Context, cfg config, logger *slog.Logger, scaleSetID int, client *scaleset.Client, slurm slurmClient, heartbeat func() error) (*scaler, error) {
	active, err := slurm.Active(ctx)
	if err != nil {
		return nil, err
	}
	runners := make(map[string]runnerState, len(active))
	for name, job := range active {
		runners[name] = runnerState{JobID: job.ID}
	}
	return &scaler{
		cfg:        cfg,
		logger:     logger,
		scaleSetID: scaleSetID,
		client:     client,
		slurm:      slurm,
		runners:    runners,
		heartbeat:  heartbeat,
	}, nil
}

func (s *scaler) HandleJobStarted(ctx context.Context, started *scaleset.JobStarted) error {
	if err := s.heartbeat(); err != nil {
		s.logger.Warn("heartbeat update failed", "error", err)
	}
	s.mu.Lock()
	state := s.runners[started.RunnerName]
	state.Busy = true
	s.runners[started.RunnerName] = state
	s.mu.Unlock()
	s.logger.Info("job started", "runner", started.RunnerName, "jobID", started.JobID)
	return nil
}

func (s *scaler) HandleJobCompleted(ctx context.Context, completed *scaleset.JobCompleted) error {
	if err := s.heartbeat(); err != nil {
		s.logger.Warn("heartbeat update failed", "error", err)
	}
	s.mu.Lock()
	state, ok := s.runners[completed.RunnerName]
	delete(s.runners, completed.RunnerName)
	s.mu.Unlock()
	s.logger.Info("job completed", "runner", completed.RunnerName, "jobID", completed.JobID)
	if ok && state.JobID > 0 {
		if err := s.slurm.Cancel(context.WithoutCancel(ctx), state.JobID); err != nil {
			s.logger.Warn("failed to cancel completed Slurm runner", "slurmJobID", state.JobID, "error", err)
		}
	}
	_ = os.Remove(filepath.Join(s.cfg.StateDir, "jit", completed.RunnerName+".jit"))
	return nil
}

func (s *scaler) HandleDesiredRunnerCount(ctx context.Context, desired int) (int, error) {
	if err := s.heartbeat(); err != nil {
		s.logger.Warn("heartbeat update failed", "error", err)
	}
	active, err := s.slurm.Active(ctx)
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for name, state := range s.runners {
		if _, ok := active[name]; !ok && !state.Busy {
			delete(s.runners, name)
			s.logger.Warn("pruned missing idle runner", "runner", name, "slurmJobID", state.JobID)
		}
	}
	for name, job := range active {
		if _, ok := s.runners[name]; !ok {
			s.runners[name] = runnerState{JobID: job.ID}
			s.logger.Info("recovered active Slurm runner", "runner", name, "slurmJobID", job.ID)
		}
	}

	if desired > s.cfg.MaxRunners {
		desired = s.cfg.MaxRunners
	}
	if desired < 0 {
		desired = 0
	}
	current := len(s.runners)
	if current > desired {
		for name, state := range s.runners {
			if current <= desired {
				break
			}
			if state.Busy {
				continue
			}
			if err := s.slurm.Cancel(ctx, state.JobID); err != nil {
				s.logger.Warn("failed to scale down idle runner", "runner", name, "slurmJobID", state.JobID, "error", err)
				continue
			}
			delete(s.runners, name)
			_ = os.Remove(filepath.Join(s.cfg.StateDir, "jit", name+".jit"))
			current--
			s.logger.Info("scaled down idle runner", "runner", name, "slurmJobID", state.JobID)
		}
	}
	if current >= desired {
		return current, nil
	}

	for i := current; i < desired; i++ {
		if err := s.startRunnerLocked(ctx); err != nil {
			return len(s.runners), err
		}
	}
	return len(s.runners), nil
}

func (s *scaler) startRunnerLocked(ctx context.Context) error {
	name := "c2ln1-" + uuid.NewString()[:8]
	jit, err := s.client.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{Name: name}, s.scaleSetID)
	if err != nil {
		return fmt.Errorf("generate JIT config for %s: %w", name, err)
	}

	jitDir := filepath.Join(s.cfg.StateDir, "jit")
	if err := os.MkdirAll(jitDir, 0o700); err != nil {
		return fmt.Errorf("create JIT directory: %w", err)
	}
	jitFile := filepath.Join(jitDir, name+".jit")
	if err := os.WriteFile(jitFile, []byte(jit.EncodedJITConfig), 0o600); err != nil {
		return fmt.Errorf("write JIT config: %w", err)
	}

	jobID, err := s.slurm.Submit(ctx, name, jitFile)
	if err != nil {
		_ = os.Remove(jitFile)
		return err
	}
	s.runners[name] = runnerState{JobID: jobID}
	s.logger.Info("submitted runner", "runner", name, "slurmJobID", jobID, "active", len(s.runners), "max", s.cfg.MaxRunners)
	return nil
}

var _ listener.Scaler = (*scaler)(nil)

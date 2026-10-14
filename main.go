package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
)

var version = "dev"

func main() {
	cfg, err := parseConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	logger := newLogger(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	go func() {
		select {
		case sig := <-signals:
			logger.Warn("received shutdown signal", "signal", sig.String())
			cancel()
		case <-ctx.Done():
		}
	}()

	err = run(ctx, cfg, logger)
	if errors.Is(err, context.Canceled) {
		logger.Info("controller stopped after context cancellation")
		return
	}
	if err != nil {
		logger.Error("controller exited", "error", err)
		os.Exit(1)
	}
	logger.Warn("controller run loop returned without cancellation")
}

func newLogger(cfg config) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	options := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(cfg.LogFormat, "text") {
		return slog.New(slog.NewTextHandler(os.Stdout, options))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, options))
}

func run(ctx context.Context, cfg config, logger *slog.Logger) error {
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	go heartbeatLoop(ctx, cfg.HeartbeatFile)

	token, err := cfg.token()
	if err != nil {
		return err
	}
	client, err := scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     cfg.RegistrationURL,
		PersonalAccessToken: token,
		SystemInfo: scaleset.SystemInfo{
			System:    "slurm-actions-scaleset",
			Subsystem: "controller",
			Version:   version,
		},
	})
	if err != nil {
		return fmt.Errorf("create scale set client: %w", err)
	}
	token = ""

	runnerGroupID := 1
	if cfg.RunnerGroup != scaleset.DefaultRunnerGroup {
		group, err := client.GetRunnerGroupByName(ctx, cfg.RunnerGroup)
		if err != nil {
			return fmt.Errorf("resolve runner group: %w", err)
		}
		runnerGroupID = group.ID
	}

	labels := make([]scaleset.Label, 0, len(cfg.Labels))
	for _, name := range cfg.Labels {
		labels = append(labels, scaleset.Label{Name: name})
	}

	set, err := client.GetRunnerScaleSet(ctx, runnerGroupID, cfg.ScaleSetName)
	if err != nil {
		return fmt.Errorf("get runner scale set: %w", err)
	}
	desiredSet := &scaleset.RunnerScaleSet{
		Name:          cfg.ScaleSetName,
		RunnerGroupID: runnerGroupID,
		Labels:        labels,
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true},
	}
	if set == nil {
		set, err = client.CreateRunnerScaleSet(ctx, desiredSet)
		if err != nil {
			return fmt.Errorf("create runner scale set: %w", err)
		}
		logger.Info("created runner scale set", "id", set.ID, "name", set.Name)
	} else {
		set, err = client.UpdateRunnerScaleSet(ctx, set.ID, desiredSet)
		if err != nil {
			return fmt.Errorf("update runner scale set: %w", err)
		}
		logger.Info("reused runner scale set", "id", set.ID, "name", set.Name)
	}

	client.SetSystemInfo(scaleset.SystemInfo{
		System:     "slurm-actions-scaleset",
		Subsystem:  "controller",
		Version:    version,
		ScaleSetID: set.ID,
	})

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "c2ln1"
	}
	session, err := client.MessageSessionClient(ctx, set.ID, hostname)
	if err != nil {
		return fmt.Errorf("create message session: %w", err)
	}
	defer session.Close(context.Background())

	listen, err := listener.New(session, listener.Config{
		ScaleSetID: set.ID,
		MaxRunners: cfg.MaxRunners,
		Logger:     logger.WithGroup("listener"),
	})
	if err != nil {
		return fmt.Errorf("create listener: %w", err)
	}

	var slurm slurmClient = &commandSlurm{cfg: cfg}
	if cfg.PoolMode {
		pool := &poolSlurm{cfg: cfg, logger: logger.WithGroup("pool")}
		slurm = pool
	}
	scaler, err := newScaler(ctx, cfg, logger.WithGroup("scaler"), set.ID, client, slurm, func() error {
		return writeHeartbeat(cfg.HeartbeatFile)
	})
	if err != nil {
		return fmt.Errorf("initialize scaler: %w", err)
	}

	logger.Info("starting scale set listener", "scaleSetID", set.ID, "maxRunners", cfg.MaxRunners)
	return listen.Run(ctx, scaler)
}

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

type config struct {
	// Runtime configuration for the Slurm adapter.
	RegistrationURL string
	ScaleSetName    string
	Labels          stringList
	RunnerGroup     string
	MaxRunners      int
	Partition       string
	CPUs            int
	Memory          string
	TimeLimit       string
	RunnerArchive   string
	RunnerScript    string
	StateDir        string
	HeartbeatFile   string
	TokenCommand    string
	LogFormat       string
	LogLevel        string
	StartupTimeout  time.Duration
	PoolMode        bool
	PoolScript      string
	PoolTimeLimit   string
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(value string) error {
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			*s = append(*s, item)
		}
	}
	return nil
}

func parseConfig() (config, error) {
	var cfg config
	flag.StringVar(&cfg.RegistrationURL, "url", "", "GitHub repository URL used to register the scale set")
	flag.StringVar(&cfg.ScaleSetName, "name", "c2ln1-p1shr", "runner scale set name")
	flag.Var(&cfg.Labels, "labels", "comma-separated runner labels")
	flag.StringVar(&cfg.RunnerGroup, "runner-group", "Default", "runner group name")
	flag.IntVar(&cfg.MaxRunners, "max-runners", 4, "maximum concurrent runners")
	flag.StringVar(&cfg.Partition, "partition", "p1shr", "Slurm partition")
	flag.IntVar(&cfg.CPUs, "cpus", 4, "CPUs per runner")
	flag.StringVar(&cfg.Memory, "memory", "8G", "memory per runner")
	flag.StringVar(&cfg.TimeLimit, "time-limit", "00:30:00", "Slurm job time limit (enables backfill for short CI jobs)")
	flag.StringVar(&cfg.RunnerArchive, "runner-archive", "", "path to the verified GitHub Actions runner archive")
	flag.StringVar(&cfg.RunnerScript, "runner-script", "", "compute-node runner bootstrap script")
	flag.StringVar(&cfg.StateDir, "state-dir", "", "state directory")
	flag.StringVar(&cfg.HeartbeatFile, "heartbeat-file", "", "heartbeat file")
	flag.StringVar(&cfg.TokenCommand, "token-command", "gh auth token", "command used to obtain the GitHub token")
	flag.StringVar(&cfg.LogFormat, "log-format", "json", "log format: json or text")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "log level: debug, info, warn, error")
	flag.DurationVar(&cfg.StartupTimeout, "startup-timeout", 3*time.Minute, "maximum time for a submitted runner to remain pending before reconciliation")
	flag.BoolVar(&cfg.PoolMode, "pool-mode", false, "reuse one Slurm allocation for serial runners")
	flag.StringVar(&cfg.PoolScript, "pool-script", "", "compute-node pool supervisor")
	flag.StringVar(&cfg.PoolTimeLimit, "pool-time-limit", "3-00:00:00", "pool allocation wall-time (72h by default)")
	flag.Parse()

	home, err := os.UserHomeDir()
	if err != nil {
		return config{}, fmt.Errorf("resolve home directory: %w", err)
	}
	if cfg.RunnerArchive == "" {
		cfg.RunnerArchive = home + "/.cache/github-actions-runner/actions-runner-linux-x64-2.338.0.tar.gz"
	}
	if cfg.RunnerScript == "" {
		cfg.RunnerScript = home + "/.local/lib/slurm-actions-scaleset/compute-runner.sh"
	}
	if cfg.StateDir == "" {
		cfg.StateDir = home + "/.local/state/slurm-actions-scaleset"
	}
	if cfg.HeartbeatFile == "" {
		cfg.HeartbeatFile = cfg.StateDir + "/heartbeat"
	}
	if cfg.PoolScript == "" {
		cfg.PoolScript = home + "/.local/lib/slurm-actions-scaleset/pool-worker.sh"
	}
	if len(cfg.Labels) == 0 {
		cfg.Labels = stringList{"self-hosted", "linux", "x64", cfg.ScaleSetName}
	}

	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (c config) validate() error {
	switch {
	case c.RegistrationURL == "":
		return errors.New("--url is required")
	case c.ScaleSetName == "":
		return errors.New("--name is required")
	case c.MaxRunners < 1:
		return errors.New("--max-runners must be at least 1")
	case c.CPUs < 1:
		return errors.New("--cpus must be at least 1")
	case c.Partition == "":
		return errors.New("--partition is required")
	case c.Memory == "":
		return errors.New("--memory is required")
	case c.TimeLimit == "":
		return errors.New("--time-limit is required")
	case c.RunnerArchive == "":
		return errors.New("--runner-archive is required")
	case c.RunnerScript == "":
		return errors.New("--runner-script is required")
	case c.PoolMode && c.MaxRunners != 1:
		return errors.New("pool requires max-runners=1")
	case c.PoolMode && c.Partition != "p1shr":
		return errors.New("pool requires authorized p1shr partition")
	case c.PoolMode && (c.CPUs != 4 || c.Memory != "8G"):
		return errors.New("pool requires 4 CPUs and 8G memory")
	case c.PoolMode && c.PoolTimeLimit == "":
		return errors.New("pool requires a walltime")
	}
	return nil
}

func (c config) token() (string, error) {
	parts := strings.Fields(c.TokenCommand)
	if len(parts) == 0 {
		return "", errors.New("token command is empty")
	}
	out, err := exec.Command(parts[0], parts[1:]...).Output()
	if err != nil {
		return "", fmt.Errorf("token command failed: %w", err)
	}
	token := strings.TrimSpace(string(out))
	if token == "" {
		return "", errors.New("token command returned an empty token")
	}
	return token, nil
}

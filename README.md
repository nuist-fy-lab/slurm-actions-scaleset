# slurm-actions-scaleset

Thin Slurm adapter for the official GitHub Actions Runner Scale Set client.

GitHub owns queueing, long polling, acknowledgements, JIT runner configuration,
and scale-set capacity. This project only translates desired runner capacity
into ephemeral Slurm jobs.

Default c2ln1 policy (one-shot mode):

- partition: `p1shr`
- CPU per runner: `4`
- memory per runner: `8G`
- Slurm wall-time limit: `00:30:00` (allows shorter backfill windows, instead of unlimited time)
- maximum concurrent runners: `1` (serial Slurm allocations)
- runner mode: ephemeral JIT

The controller is intended to run on the c2ln1 login node. Each runner executes
on a Slurm compute node and consumes one job only.

## Persistent Slurm allocation (opt-in)

For frequent short CI jobs, set `POOL_MODE='true'` in the organization
controller configuration, install `scripts/pool-worker.sh`, and set
`POOL_SCRIPT` to its absolute path. Keep `MAX_RUNNERS=1`, `PARTITION=p1shr`,
`CPUS=4`, and `MEMORY=8G`. `POOL_IDLE_HOURS='72'` is a rolling idle lease:
after each GitHub CI job completes the full 72-hour inactivity window starts
again. When 72 hours pass without another CI job, the worker exits naturally
and releases the reserved 4C8G allocation. With no CI pending, the controller
does **not** allocate a replacement. The next CI automatically submits a new
allocation, which may wait in the Slurm queue. Only enable persistent idle
allocations where this is permitted by the HPC center's rules.

The login-node controller submits at most one Slurm pool job on demand.
When admitted, the compute-node pool processes `.req` markers from a private
directory in shared storage and starts a fresh one-job GitHub JIT runner for
each request. After the runner and its cleanup exit, the shared allocation
remains alive for the next request. `POOL_TIME_LIMIT='UNLIMITED'` (the default)
separates Slurm's hard walltime from the rolling idle lease, if the partition
and account policy permit unlimited allocation duration. Set a finite hard
walltime only if required by the HPC administrators; hitting that hard limit
still ends the allocation even with recent CI activity. Worker admission
reserves at least 35 minutes before a finite hard deadline for the 30-minute
CI job plus cleanup. A Slurm allocation is never guaranteed against node
failure, administrator intervention, or preemption.

Stop the controller before changing modes. Do not enable the pool while an
old one-shot CI Slurm allocation remains active: drain or explicitly handle
that allocation first to avoid running both kinds of jobs. Keep the old
binary and configuration for immediate rollback. Disabling pool mode does
not cancel an already-running pool allocation; explicitly drain and cancel
the pool job using the recorded Slurm ID if rolling back.

The pool worker runs only under Slurm (`SLURM_JOB_ID` must be set). It does
not run CI test/build steps on c2ln1. The pool request directory is mode
0700; per-job GitHub JIT credentials remain in the existing mode-0600 files.

Repository CI runs on GitHub-hosted Ubuntu workers only for pushes to branches
in the organization-owned repository. Write collaborators must be organization
members. Pull requests, including external forks, do not trigger CI. The
c2ln1 cluster is only a deployment target for this repository, not a CI runner.

Runtime values are separated from scripts:

- `controller.conf` owns repository/scale-set/Slurm capacity settings.
- `runtime.conf` owns local proxy, Git egress, toolchain, and cache paths.

The compute runner writes Git and GOST credentials only to owner-readable,
per-job temporary configuration files. Credentials are not passed in process
arguments.

Runner `NO_PROXY` is intentionally separate from the login-node environment.
The c2ln1 compute nodes cannot directly reach the fyzure egress endpoints, so
Runner jobs keep only loopback addresses in `NO_PROXY`; pkg/git/gh/docker/ghcr
remain behind the local GOST chain.

JavaScript actions receive a local HTTP CONNECT proxy provided by GOST, which
forwards through the existing `proxy set direct` SOCKS5 tunnel on each compute
node. Production currently pins GOST v3.3.0 (linux/amd64).

Runtime hardening:

- the controller runs as a user systemd service with `KillMode=process`, so a
  controller restart does not kill already-submitted Slurm jobs;
- a one-minute watchdog checks controller heartbeat and enforces the hard
  one-runner Slurm cap;
- after a GitHub job completes, the ephemeral runner exits naturally so Slurm
  records `COMPLETED`; explicit idle scale-down still uses cancellation;
- CI never writes to the shared production `$HOME` on compute nodes;
- compute runners share the immutable toolchain under a shared file lock, while
  each job gets its own Git global config, Mamba root, and Conda package cache;
- the HTTP CONNECT-to-SOCKS bridge binds only to loopback and uses an ephemeral
  local port per runner.

# slurm-actions-scaleset

Thin Slurm adapter for the official GitHub Actions Runner Scale Set client.

GitHub owns queueing, long polling, acknowledgements, JIT runner configuration,
and scale-set capacity. This project only translates desired runner capacity
into ephemeral Slurm jobs.

Default c2ln1 policy:

- partition: `p1shr`
- CPU per runner: `4`
- memory per runner: `8G`
- Slurm wall-time limit: `00:30:00` (allows shorter backfill windows, instead of unlimited time)
- maximum concurrent runners: `1` (serial Slurm allocations)
- runner mode: ephemeral JIT

The controller is intended to run on the c2ln1 login node. Each runner executes
on a Slurm compute node and consumes one job only.

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

# slurm-actions-scaleset

Thin Slurm adapter for the official GitHub Actions Runner Scale Set client.

GitHub owns queueing, long polling, acknowledgements, JIT runner configuration,
and scale-set capacity. This project only translates desired runner capacity
into ephemeral Slurm jobs.

Default c2ln1 policy:

- partition: `p1shr`
- CPU per runner: `40`
- memory per runner: `16G`
- maximum concurrent runners: `4`
- runner mode: ephemeral JIT

The controller is intended to run on the c2ln1 login node. Each runner executes
on a Slurm compute node and consumes one job only.

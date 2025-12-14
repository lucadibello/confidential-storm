# DP-SQLP Synthetic Histogram Example

Implements the synthetic experiment from Section 5.1 of the paper with the full Confidential Storm stack (common/host/enclave), encrypted tuples, and enclaved DP logic. It generates Zipfian synthetic data, runs it through `StreamingDPMechanism`, and computes the paper’s utility metrics against ground truth.

## Parameters (paper defaults)
- Privacy: `epsilon=6.0`, `delta=1e-9`, split 50/50 between key selection and histogram.
- Bounding: `C=32`, `L_m=1`, `mu=50`.
- Workload: Zipf key space 100k, 100k users, 20k records per tick, seed 42.
- Micro-batches: `dp.max.time.steps` (100 or 1000). Tick frequency is auto-set so the runtime spans the requested number of micro-batches.

## Build & Run (local Storm)
```bash
# 1) Ensure framework artifacts are installed locally
make -C examples install-framework

# 2) Run the synthetic topology locally (writes data/synthetic-report.txt)
make -C examples/synthetic-dp-histogram run
```

What you get:
- `data/synthetic-report.txt` updated on each tick with `keys_retained`, `l_inf`, `l_1`, and `l_2` versus ground truth.
- All tuples are sealed with the demo stream key and processed inside the enclave implementation of `SyntheticHistogramService`.

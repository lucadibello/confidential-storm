#!/usr/bin/env python3
"""
analyze-results.py -- Analyze Synthetic DP Histogram benchmark results.

Parses report files produced by the topology, extracts final-tick metrics
from each run, averages across runs, and compares with the paper's Table 1.

Usage:
    python3 scripts/analyze-results.py                    # auto-detect all runs
    python3 scripts/analyze-results.py --config 100       # only 100 micro-batches
    python3 scripts/analyze-results.py --config 1000      # only 1000 micro-batches
    python3 scripts/analyze-results.py --config both      # both configs
    python3 scripts/analyze-results.py --data-dir ./data  # custom data directory
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path


# Paper Table 1 reference values (DP-SQLP column)
PAPER_REFERENCE = {
    100: {"keys": 28338, "l_inf": 1391, "l_1": 17741225, "l_2": 50039},
    1000: {"keys": 22280, "l_inf": 1563, "l_1": 19395721, "l_2": 58237},
}

DEVIATION_THRESHOLD = 0.20  # Flag deviations > 20%


def parse_report(filepath):
    """Parse a single report file and return the final tick's metrics."""
    ticks = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) < 8:
                continue
            try:
                tick = {
                    "tick": parts[0],
                    "timestamp": parts[1],
                    "l0": int(parts[2]),
                    "l_inf": int(parts[3]),
                    "l_1": int(parts[4]),
                    "l_2": float(parts[5]),
                    "dp_keys": int(parts[6]),
                    "gt_keys": int(parts[7]),
                }
                ticks.append(tick)
            except (ValueError, IndexError):
                continue

    if not ticks:
        return None

    # Return the last tick (final snapshot = most complete data)
    return ticks[-1]


def detect_config(filepath):
    """Try to detect whether a run used 100 or 1000 micro-batches from tick count."""
    tick_count = 0
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                tick_count += 1

    # Heuristic: if > 200 ticks, it was likely a 1000-step run
    if tick_count > 200:
        return 1000
    return 100


def format_number(n):
    """Format a number with thousand separators."""
    if isinstance(n, float):
        return f"{n:,.2f}"
    return f"{n:,}"


def main():
    parser = argparse.ArgumentParser(description="Analyze Synthetic DP Histogram results")
    parser.add_argument(
        "--config",
        choices=["100", "1000", "both", "auto"],
        default="auto",
        help="Which micro-batch configuration to analyze (default: auto-detect)",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing report files (default: data)",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"ERROR: Data directory '{data_dir}' not found.")
        sys.exit(1)

    # Find all report files
    report_files = sorted(data_dir.glob("synthetic-report-run*.txt"))
    if not report_files:
        print(f"No report files found in '{data_dir}'.")
        print("Run experiments first: ./scripts/run-quick-test.sh")
        sys.exit(1)

    # Parse all reports and group by detected config
    results_by_config = defaultdict(list)
    for filepath in report_files:
        metrics = parse_report(filepath)
        if metrics is None:
            print(f"  WARNING: No valid data in {filepath.name}, skipping.")
            continue

        config = detect_config(filepath)
        run_id = filepath.stem.replace("synthetic-report-run", "")

        results_by_config[config].append(
            {"run_id": run_id, "file": filepath.name, "metrics": metrics}
        )

    # Determine which configs to show
    configs_to_show = []
    if args.config == "auto" or args.config == "both":
        configs_to_show = sorted(results_by_config.keys())
    else:
        requested = int(args.config)
        if requested in results_by_config:
            configs_to_show = [requested]
        else:
            print(f"No runs found for {requested} micro-batches.")
            sys.exit(1)

    if not configs_to_show:
        print("No valid results found.")
        sys.exit(1)

    # Print results
    print()
    print("=" * 72)
    print(" Synthetic DP Histogram -- Results Analysis")
    print("=" * 72)

    for config in configs_to_show:
        runs = results_by_config[config]
        print()
        print(f"--- {config} Micro-batches ({len(runs)} run(s)) ---")
        print()

        # Print individual runs
        header = f"{'Run':>5}  {'Keys (l0)':>12}  {'L_inf':>10}  {'L_1':>14}  {'L_2':>12}  {'DP Keys':>10}  {'GT Keys':>10}"
        print(header)
        print("-" * len(header))

        for run in runs:
            m = run["metrics"]
            print(
                f"{run['run_id']:>5}  "
                f"{format_number(m['l0']):>12}  "
                f"{format_number(m['l_inf']):>10}  "
                f"{format_number(m['l_1']):>14}  "
                f"{format_number(m['l_2']):>12}  "
                f"{format_number(m['dp_keys']):>10}  "
                f"{format_number(m['gt_keys']):>10}"
            )

        # Compute averages if multiple runs
        if len(runs) > 1:
            avg = {
                "l0": sum(r["metrics"]["l0"] for r in runs) / len(runs),
                "l_inf": sum(r["metrics"]["l_inf"] for r in runs) / len(runs),
                "l_1": sum(r["metrics"]["l_1"] for r in runs) / len(runs),
                "l_2": sum(r["metrics"]["l_2"] for r in runs) / len(runs),
                "dp_keys": sum(r["metrics"]["dp_keys"] for r in runs) / len(runs),
                "gt_keys": sum(r["metrics"]["gt_keys"] for r in runs) / len(runs),
            }
            print("-" * len(header))
            print(
                f"{'AVG':>5}  "
                f"{format_number(avg['l0']):>12}  "
                f"{format_number(avg['l_inf']):>10}  "
                f"{format_number(avg['l_1']):>14}  "
                f"{format_number(avg['l_2']):>12}  "
                f"{format_number(avg['dp_keys']):>10}  "
                f"{format_number(avg['gt_keys']):>10}"
            )
        else:
            avg = runs[0]["metrics"]

        # Compare with paper reference (if available and at paper scale)
        if config in PAPER_REFERENCE:
            ref = PAPER_REFERENCE[config]
            print()
            print(f"  Paper comparison (Table 1, {config} micro-batches):")
            print(f"  {'Metric':<10} {'Ours':>14} {'Paper':>14} {'Ratio':>8} {'Status':>8}")
            print(f"  {'-'*56}")

            comparisons = [
                ("Keys", avg["l0"], ref["keys"]),
                ("L_inf", avg["l_inf"], ref["l_inf"]),
                ("L_1", avg["l_1"], ref["l_1"]),
                ("L_2", avg["l_2"], ref["l_2"]),
            ]

            for name, ours, paper in comparisons:
                if paper > 0:
                    ratio = ours / paper
                    deviation = abs(ratio - 1.0)
                    status = "OK" if deviation <= DEVIATION_THRESHOLD else "WARN"
                    print(
                        f"  {name:<10} {format_number(ours):>14} {format_number(paper):>14} {ratio:>7.2f}x {'  ' + status:>8}"
                    )
                else:
                    print(
                        f"  {name:<10} {format_number(ours):>14} {format_number(paper):>14} {'N/A':>8} {'':>8}"
                    )

            print()
            print(
                f"  NOTE: Deviations are expected due to SGX overhead, different"
            )
            print(
                f"  parallelism (8 vs 150-600 workers), and randomness across runs."
            )

    print()
    print("=" * 72)


if __name__ == "__main__":
    main()

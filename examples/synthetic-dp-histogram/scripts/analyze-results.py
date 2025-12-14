#!/usr/bin/env python3
"""
Analyze synthetic DP histogram experiment results.
Computes average metrics across runs to reproduce Table 1 from the paper.
"""

import argparse
import glob
import sys
from pathlib import Path
from typing import Dict, List, Tuple


def parse_report_file(filepath: str) -> List[Dict]:
    """Parse a single report file and return list of tick records."""
    records = []
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split(',')
            if len(parts) >= 8:
                try:
                    record = {
                        'tick': parts[0],
                        'timestamp': parts[1],
                        'l0': int(parts[2]),
                        'l_inf': int(parts[3]),
                        'l1': int(parts[4]),
                        'l2': float(parts[5]),
                        'dp_keys': int(parts[6]),
                        'gt_keys': int(parts[7])
                    }
                    records.append(record)
                except (ValueError, IndexError) as e:
                    print(f"Warning: Could not parse line in {filepath}: {line}", file=sys.stderr)
                    continue
    return records


def compute_final_metrics(records: List[Dict]) -> Dict:
    """Get metrics from the final tick."""
    if not records:
        return None
    final = records[-1]
    return {
        'keys': final['l0'],
        'l_inf': final['l_inf'],
        'l1': final['l1'],
        'l2': final['l2']
    }


def average_metrics(metrics_list: List[Dict]) -> Dict:
    """Compute average across multiple runs."""
    if not metrics_list:
        return None

    n = len(metrics_list)
    avg = {
        'keys': sum(m['keys'] for m in metrics_list) / n,
        'l_inf': sum(m['l_inf'] for m in metrics_list) / n,
        'l1': sum(m['l1'] for m in metrics_list) / n,
        'l2': sum(m['l2'] for m in metrics_list) / n,
    }
    return avg


def format_comparison_table(config: str, avg_metrics: Dict, paper_metrics: Dict) -> str:
    """Format comparison table between implementation and paper."""
    output = []
    output.append(f"\n{'='*70}")
    output.append(f"Results for {config}")
    output.append(f"{'='*70}")
    output.append(f"{'Metric':<15} {'Implementation':<20} {'Paper (DP-SQLP)':<20} {'Match':<10}")
    output.append(f"{'-'*70}")

    # Keys retained
    impl_keys = f"{avg_metrics['keys']:.0f}"
    paper_keys = str(paper_metrics['keys'])
    match = "✓" if abs(avg_metrics['keys'] - paper_metrics['keys']) / paper_metrics['keys'] < 0.2 else "?"
    output.append(f"{'Keys':<15} {impl_keys:<20} {paper_keys:<20} {match:<10}")

    # L∞ norm
    impl_linf = f"{avg_metrics['l_inf']:.0f}"
    paper_linf = f"{paper_metrics['l_inf']:,}"
    match = "✓" if abs(avg_metrics['l_inf'] - paper_metrics['l_inf']) / paper_metrics['l_inf'] < 0.2 else "?"
    output.append(f"{'L∞ Norm':<15} {impl_linf:<20} {paper_linf:<20} {match:<10}")

    # L₁ norm
    impl_l1 = f"{avg_metrics['l1']:,.0f}"
    paper_l1 = f"{paper_metrics['l1']:,}"
    match = "✓" if abs(avg_metrics['l1'] - paper_metrics['l1']) / paper_metrics['l1'] < 0.2 else "?"
    output.append(f"{'L₁ Norm':<15} {impl_l1:<20} {paper_l1:<20} {match:<10}")

    # L₂ norm
    impl_l2 = f"{avg_metrics['l2']:,.0f}"
    paper_l2 = f"{paper_metrics['l2']:,}"
    match = "✓" if abs(avg_metrics['l2'] - paper_metrics['l2']) / paper_metrics['l2'] < 0.2 else "?"
    output.append(f"{'L₂ Norm':<15} {impl_l2:<20} {paper_l2:<20} {match:<10}")

    output.append(f"{'='*70}\n")
    return '\n'.join(output)


def main():
    parser = argparse.ArgumentParser(description='Analyze synthetic DP histogram results')
    parser.add_argument('--data-dir', default='data', help='Directory containing report files')
    parser.add_argument('--config', choices=['100', '1000', 'both'], default='both',
                       help='Which configuration to analyze')
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Error: Data directory {data_dir} does not exist", file=sys.stderr)
        return 1

    # Paper reference values (Table 1, Section 5.1)
    paper_100 = {'keys': 28338, 'l_inf': 1391, 'l1': 17741225, 'l2': 50039}
    paper_1000 = {'keys': 22280, 'l_inf': 1563, 'l1': 19395721, 'l2': 58237}

    # Analyze 100 micro-batches (runs 1-3)
    if args.config in ['100', 'both']:
        runs_100 = []
        for run_id in [1, 2, 3]:
            filepath = data_dir / f"synthetic-report-run{run_id}.txt"
            if filepath.exists():
                records = parse_report_file(str(filepath))
                metrics = compute_final_metrics(records)
                if metrics:
                    runs_100.append(metrics)
                    print(f"Loaded run {run_id} (100 micro-batches): {len(records)} ticks")

        if runs_100:
            avg_100 = average_metrics(runs_100)
            print(format_comparison_table("100 Micro-batches", avg_100, paper_100))
        else:
            print("Warning: No data found for 100 micro-batches configuration", file=sys.stderr)

    # Analyze 1000 micro-batches (runs 4-6)
    if args.config in ['1000', 'both']:
        runs_1000 = []
        for run_id in [4, 5, 6]:
            filepath = data_dir / f"synthetic-report-run{run_id}.txt"
            if filepath.exists():
                records = parse_report_file(str(filepath))
                metrics = compute_final_metrics(records)
                if metrics:
                    runs_1000.append(metrics)
                    print(f"Loaded run {run_id} (1000 micro-batches): {len(records)} ticks")

        if runs_1000:
            avg_1000 = average_metrics(runs_1000)
            print(format_comparison_table("1000 Micro-batches", avg_1000, paper_1000))
        else:
            print("Warning: No data found for 1000 micro-batches configuration", file=sys.stderr)

    print("\n✓ Analysis complete")
    print("\nNote: '✓' indicates implementation within 20% of paper results")
    print("      '?' indicates potential deviation requiring investigation")
    return 0


if __name__ == '__main__':
    sys.exit(main())

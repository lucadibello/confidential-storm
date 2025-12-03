#!/usr/bin/env python3
"""
Compute a per-user word-count percentile for the plaintext jokes dataset.

Records must include a `user_id`; counts are aggregated per user to help pick a
contribution bound C close to a chosen percentile (per Section 3.2 heuristic).
"""

import argparse
import json
import math
import re
import statistics
from collections import defaultdict
from typing import Iterable, List


WORD_REGEX = re.compile(r"\b\w+\b", re.UNICODE)


def count_words(text: str) -> int:
    return len(WORD_REGEX.findall(text or ""))


def percentile(values: List[int], pct: float) -> float:
    if not values:
        return math.nan
    values = sorted(values)
    k = (len(values) - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(values[int(k)])
    d0 = values[int(f)] * (c - k)
    d1 = values[int(c)] * (k - f)
    return float(d0 + d1)


def aggregate_counts(records: Iterable[dict]) -> List[int]:
    per_user = defaultdict(int)
    for rec in records:
        if "user_id" not in rec:
            raise ValueError("Record is missing 'user_id'; per-user aggregation required.")
        uid = rec["user_id"]
        body = rec.get("body", "")
        wc = count_words(body)
        per_user[uid] += wc
    return list(per_user.values())


def main():
    parser = argparse.ArgumentParser(description="Compute word-count percentile for dataset.")
    parser.add_argument("dataset", help="Path to plaintext jokes dataset (JSON array).")
    parser.add_argument("-p", "--percentile", type=float, default=0.99,
                        help="Percentile to compute (0-1). Default: 0.99")
    args = parser.parse_args()

    with open(args.dataset, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise SystemExit("Dataset must be a JSON array.")

    counts = aggregate_counts(data)
    pct = percentile(counts, args.percentile)

    print(f"Records: {len(data)}; per-user aggregation")
    print(f"Percentile {args.percentile*100:.1f}%: {pct:.2f} words")
    print(f"Min: {min(counts)}; Max: {max(counts)}; Mean: {statistics.mean(counts):.2f}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Compare StreamCrab vs Flink benchmark results."""

import sys
import os
from collections import defaultdict

def parse_bench_line(line):
    """Parse BENCH|name|key=val|... format"""
    parts = line.strip().split("|")
    if len(parts) < 3:
        return None
    name = parts[1]
    metrics = {}
    for part in parts[2:]:
        if "=" in part:
            k, v = part.split("=", 1)
            try:
                metrics[k] = float(v)
            except ValueError:
                metrics[k] = v
    return name, metrics

def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "benchmarks/results/latest"

    sc_data = defaultdict(list)  # name -> [time_ms, ...]
    flink_data = defaultdict(list)

    # Parse StreamCrab
    sc_file = os.path.join(results_dir, "streamcrab_throughput.txt")
    if os.path.exists(sc_file):
        for line in open(sc_file):
            result = parse_bench_line(line)
            if result:
                name, metrics = result
                sc_data[name].append(metrics)

    # Parse Flink
    flink_file = os.path.join(results_dir, "flink_throughput.txt")
    if os.path.exists(flink_file):
        for line in open(flink_file):
            result = parse_bench_line(line)
            if result:
                name, metrics = result
                flink_data[name].append(metrics)

    # Print comparison
    print("\n" + "=" * 80)
    print("STREAMCRAB vs FLINK BENCHMARK COMPARISON")
    print("=" * 80)

    all_tests = sorted(set(list(sc_data.keys()) + list(flink_data.keys())))

    print(f"\n{'Test':<40} {'StreamCrab (ms)':<18} {'Flink (ms)':<18} {'Speedup':<10}")
    print("-" * 86)

    for test in all_tests:
        sc_times = [m.get('time_ms', 0) for m in sc_data.get(test, [])]
        fl_times = [m.get('time_ms', 0) for m in flink_data.get(test, [])]

        sc_median = sorted(sc_times)[len(sc_times)//2] if sc_times else 0
        fl_median = sorted(fl_times)[len(fl_times)//2] if fl_times else 0

        speedup = f"{fl_median/sc_median:.1f}x" if sc_median > 0 and fl_median > 0 else "N/A"

        sc_str = f"{sc_median:.0f}" if sc_median else "N/A"
        fl_str = f"{fl_median:.0f}" if fl_median else "N/A"

        print(f"{test:<40} {sc_str:<18} {fl_str:<18} {speedup:<10}")

    # Latency
    latency_file = os.path.join(results_dir, "streamcrab_latency.txt")
    if os.path.exists(latency_file):
        print(f"\n{'='*80}")
        print("STREAMCRAB LATENCY (microseconds)")
        print(f"{'='*80}")
        for line in open(latency_file):
            print(f"  {line.strip()}")

    # Memory
    memory_file = os.path.join(results_dir, "streamcrab_memory.txt")
    if os.path.exists(memory_file):
        print(f"\n{'='*80}")
        print("STREAMCRAB MEMORY")
        print(f"{'='*80}")
        for line in open(memory_file):
            print(f"  {line.strip()}")

if __name__ == "__main__":
    main()

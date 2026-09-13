#!/bin/bash

# Exit on error
set -e

# Configuration
RECORD_COUNT=${1:-1000000}
OPERATION_COUNT=${2:-1000000}
PORT=3009
REPORT_PATH="benchmark_report.md"

echo "===================================================="
echo "YCSB Relational SQL Benchmark Runner"
echo "===================================================="
echo "Record Count:    $RECORD_COUNT"
echo "Operation Count: $OPERATION_COUNT"
echo "Port:            $PORT"
echo "Report Path:     $REPORT_PATH"
echo "===================================================="

# Ensure executables exist
if [ ! -f "./zig-out/bin/btree" ]; then
    echo "Error: ./zig-out/bin/btree not found. Please build it first."
    exit 1
fi

if [ ! -f "./benchmark/ycsb/zig-out/bin/ycsb" ]; then
    echo "Error: ./benchmark/ycsb/zig-out/bin/ycsb not found. Please build it first."
    exit 1
fi

# Run 10 iterations
for i in {1..5}; do
    echo ""
    echo "===================================================="
    echo "Starting Iteration $i / 10"
    echo "===================================================="

    # Clean up old database files for a clean iteration run
    rm -f nova.db
    rm -f db.nova
    rm -rf wal/

    # Start the server in the background
    ./zig-out/bin/btree > btree_server.log 2>&1 &
    ITER_PID=$!

    # Ensure we shut down the server if the script exits prematurely
    trap "kill $ITER_PID 2>/dev/null || true" INT TERM EXIT

    # Wait for server to start
    sleep 2

    # Check if server is running
    if ! kill -0 $ITER_PID 2>/dev/null; then
        echo "Error: Database server failed to start. Check btree_server.log for details."
        cat btree_server.log
        exit 1
    fi

    # Run the benchmark for this iteration
    ./benchmark/ycsb/zig-out/bin/shinydb-ycsb workload-all \
        --record_count=$RECORD_COUNT \
        --operation_count=$OPERATION_COUNT \
        --port=$PORT \
        --export_path="run_${i}.md"

    # Shut down the server for this iteration
    kill $ITER_PID 2>/dev/null || true
    wait $ITER_PID 2>/dev/null || true
done

# Reset trap
trap - INT TERM EXIT

echo ""
echo "===================================================="
echo "Calculating averages across 10 iterations..."
echo "===================================================="

python3 - "$RECORD_COUNT" "$OPERATION_COUNT" << 'EOF'
import sys
import os

record_count = sys.argv[1]
operation_count = sys.argv[2]

runs = []
for i in range(1, 11):
    filename = f"run_{i}.md"
    if os.path.exists(filename):
        runs.append(filename)

if not runs:
    print("Error: No iteration run files found.")
    sys.exit(1)

data = {}

for run in runs:
    with open(run, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('| Workload'):
                parts = [p.strip() for p in line.split('|')]
                if len(parts) >= 10 and parts[1] != 'Workload' and not parts[1].startswith('---'):
                    wl = parts[1]
                    desc = parts[2]
                    mix = parts[3]
                    try:
                        throughput = float(parts[4])
                        avg = float(parts[5])
                        min_val = float(parts[6])
                        max_val = float(parts[7])
                        p95 = float(parts[8])
                        p99 = float(parts[9])
                    except ValueError:
                        continue
                    
                    if wl not in data:
                        data[wl] = {
                            'desc': desc,
                            'mix': mix,
                            'throughput': [],
                            'avg': [],
                            'min': [],
                            'max': [],
                            'p95': [],
                            'p99': []
                        }
                    data[wl]['throughput'].append(throughput)
                    data[wl]['avg'].append(avg)
                    data[wl]['min'].append(min_val)
                    data[wl]['max'].append(max_val)
                    data[wl]['p95'].append(p95)
                    data[wl]['p99'].append(p99)

report_path = "benchmark_report.md"
with open(report_path, 'w') as f:
    f.write("# YCSB Benchmark Suite 10-Iteration Average Report\n\n")
    f.write(f"- **Record Count:** {record_count}\n")
    f.write(f"- **Operation Count:** {operation_count}\n")
    f.write(f"- **Iterations:** 10\n\n")
    f.write("| Workload | Description | Mix | Throughput (ops/sec) | Avg Latency (us) | Min Latency (us) | Max Latency (us) | p95 (us) | p99 (us) |\n")
    f.write("|---|---|---|---|---|---|---|---|---|\n")
    
    for wl in sorted(data.keys()):
        d = data[wl]
        avg_throughput = sum(d['throughput']) / len(d['throughput'])
        avg_latency = sum(d['avg']) / len(d['avg'])
        avg_min = sum(d['min']) / len(d['min'])
        avg_max = sum(d['max']) / len(d['max'])
        avg_p95 = sum(d['p95']) / len(d['p95'])
        avg_p99 = sum(d['p99']) / len(d['p99'])
        
        f.write(f"| {wl} | {d['desc']} | {d['mix']} | {avg_throughput:.1f} | {avg_latency:.1f} | {avg_min:.1f} | {avg_max:.1f} | {avg_p95:.1f} | {avg_p99:.1f} |\n")

print("Averages calculated successfully.")
EOF

# Clean up iteration files
rm -f run_*.md

echo "Benchmark run complete."
echo "10-iteration average Markdown report generated at: $REPORT_PATH"


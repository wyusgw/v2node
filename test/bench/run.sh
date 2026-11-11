#!/bin/bash
# 依次跑 T1..T5 五档配置。用法: test/bench/run.sh [v2node 二进制路径]
# v2node 与压测进程用 taskset 绑在不同核上，结果写到 test/bench/results/<档位>/
set -e
cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)
BIN=${1:-$ROOT/build_assets/v2node}
if [ ! -x "$BIN" ]; then
  (cd "$ROOT" && GOEXPERIMENT=jsonv2 go build -trimpath -o "$BIN" .)
fi
go build -o bench .
run() {
  name=$1; hc=$2; shift 2
  echo "=== $name $*"
  taskset -c "$hc" ./bench -bin "$BIN" -out "results/$name" "$@" 2>&1 | tail -7
  pkill -f "$BIN server" || true
  sleep 3
  for p in heap_idle heap_active heap_bulk heap_after; do
    go tool pprof -sample_index=inuse_space -top -nodecount=12 "$BIN" "results/$name/$p.pb.gz" > "results/$name/$p.top.txt" 2>/dev/null || true
  done
  go tool pprof -sample_index=alloc_space -top -nodecount=12 "$BIN" "results/$name/heap_active.pb.gz" > "results/$name/alloc_active.top.txt" 2>/dev/null || true
  for p in cpu_active cpu_bulk; do
    go tool pprof -top -nodecount=15 "$BIN" "results/$name/$p.pb.gz" > "results/$name/$p.top.txt" 2>/dev/null || true
  done
  go tool pprof -top -nodecount=8 "$BIN" "results/$name/goroutine_active.pb.gz" > "results/$name/goroutine.top.txt" 2>/dev/null || true
  rm -f "results/$name/config.json"
}
#   档位        压测核 参数
run T1_1C512M 1-3 -users 100   -conns 200  -cpus 0   -procs 1 -memlimit 450MiB  -bulk 8
run T2_1C1G   1-3 -users 1000  -conns 1000 -cpus 0   -procs 1 -memlimit 900MiB  -bulk 16
run T3_2C2G   2-3 -users 5000  -conns 3000 -cpus 0-1 -procs 2 -memlimit 1800MiB -bulk 16
run T4_2C4G   2-3 -users 20000 -conns 6000 -cpus 0-1 -procs 2 -memlimit 3600MiB -bulk 16
run T5_3C8G   3   -users 50000 -conns 8000 -cpus 0-2 -procs 3 -memlimit 7200MiB -bulk 16

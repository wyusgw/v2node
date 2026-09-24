#!/usr/bin/env bash
# Measures v2node's CPU and memory under many users, from low- to high-spec
# machine tiers, and captures pprof profiles for each run. See README.md.
#
# Needs root (memory cgroup, iptables) on Linux with cgroup v1 or v2.
#
#   bench/run.sh                  # everything
#   ONLY=idle bench/run.sh        # user-count scaling only
#   ONLY=load bench/run.sh        # traffic runs only
#   PROTOCOLS="vless" TIERS="1:512" LOADS="500" bench/run.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-$ROOT/bench/results}
BIN=${BIN:-$(mktemp -d)}
PROTOCOLS=${PROTOCOLS:-"vless vmess trojan shadowsocks"}
TIERS=${TIERS:-"1:512 1:1024 2:2048 3:4096"}   # cores:memory-MB given to v2node
IDLE_USERS=${IDLE_USERS:-"1000 10000 50000 100000"}
IDLE_TIER=${IDLE_TIER:-"1:512"}
LOAD_USERS=${LOAD_USERS:-10000}
LOADS=${LOADS:-"500 2000 5000"}                # concurrent connections
DOWN=${DOWN:-32768}                            # per-connection bytes/s
UP=${UP:-4096}
LIFE=${LIFE:-20s}
LG_FLAGS=${LG_FLAGS:-}                         # extra loadgen flags, e.g. "-sticky -src-ips"
RAMP=${RAMP:-10}                               # seconds
STEADY=${STEADY:-30}                           # seconds
ONLY=${ONLY:-}
NCPU=$(nproc)
PANEL=127.0.0.1:18080
NODE_PORT=20443
PPROF=16060
SINK=127.0.0.1:19000
TARGET_IP=11.11.11.11                          # DNAT'd to the sink; see below

export GOEXPERIMENT=jsonv2
if [ -z "${SKIP_BUILD:-}" ]; then # SKIP_BUILD=1 runs the binaries already in $BIN
	echo "building into $BIN"
	(cd "$ROOT" && go build -trimpath -o "$BIN/v2node" . && go build -o "$BIN/" ./bench/mockpanel ./bench/loadgen)
fi

# xray's freedom outbound refuses private destinations for vless/vmess/
# trojan/shadowsocks inbounds, so the node is pointed at a public-looking
# address that the kernel rewrites to the local sink.
iptables -t nat -C OUTPUT -d $TARGET_IP/32 -p tcp -j DNAT --to-destination 127.0.0.1 2>/dev/null ||
	iptables -t nat -A OUTPUT -d $TARGET_IP/32 -p tcp -j DNAT --to-destination 127.0.0.1
ulimit -n "$(ulimit -Hn)"
sysctl -qw net.ipv4.ip_local_port_range="10000 65000" 2>/dev/null || true

# memory cgroup the node runs in
if [ -d /sys/fs/cgroup/memory ]; then
	CG=/sys/fs/cgroup/memory/v2node-bench; mkdir -p $CG
	cg_limit() { echo $(($1 << 20)) >$CG/memory.limit_in_bytes; echo 0 >$CG/memory.max_usage_in_bytes; OOM0=$(cg_oom); }
	cg_peak() { cat $CG/memory.max_usage_in_bytes; }
	cg_oom() { awk '/oom_kill /{print $2}' $CG/memory.oom_control 2>/dev/null || echo 0; }
else
	CG=/sys/fs/cgroup/v2node-bench; mkdir -p $CG
	echo +memory >/sys/fs/cgroup/cgroup.subtree_control 2>/dev/null || true
	cg_limit() { echo $(($1 << 20)) >$CG/memory.max; echo 0 >$CG/memory.swap.max 2>/dev/null || true; echo reset >$CG/memory.peak 2>/dev/null || true; OOM0=$(cg_oom); }
	cg_peak() { cat $CG/memory.peak 2>/dev/null || cat $CG/memory.current; }
	cg_oom() { awk '/oom_kill /{print $2}' $CG/memory.events; }
fi

OOM0=0
ooms() { echo $(( $(cg_oom) - OOM0 )); }
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; PIDS=(); }
trap cleanup EXIT

# start_node <protocol> <users> <cores> <memMB> <dir>
start_node() {
	local proto=$1 users=$2 cores=$3 mem=$4 dir=$5
	local others="$cores-$((NCPU - 1))"
	[ "$cores" -ge "$NCPU" ] && others="0-$((NCPU - 1))"
	LG_CPUS=$others
	taskset -c "$LG_CPUS" "$BIN/mockpanel" -listen $PANEL -protocol "$proto" -port $NODE_PORT -users "$users" >"$dir/panel.log" 2>&1 &
	PIDS+=($!)
	cat >"$dir/config.json" <<-EOF
		{"Log":{"Level":"error","Access":"none"},"PprofPort":$PPROF,
		 "Nodes":[{"ApiHost":"http://$PANEL","NodeID":1,"ApiKey":"bench","Timeout":30}]}
	EOF
	for _ in $(seq 50); do curl -sf http://$PANEL/stats >/dev/null && break; sleep 0.1; done
	cg_limit "$mem"
	# The node joins the memory cgroup, then execs pinned to its cores.
	sh -c "echo \$\$ >$CG/cgroup.procs && exec taskset -c 0-$((cores - 1)) $BIN/v2node server -c $dir/config.json -w=false" >"$dir/node.log" 2>&1 &
	NODE_PID=$!
	PIDS+=($NODE_PID)
	for _ in $(seq 300); do
		(exec 3<>/dev/tcp/127.0.0.1/$NODE_PORT) 2>/dev/null && return 0
		kill -0 $NODE_PID 2>/dev/null || { echo "node died during startup"; return 1; }
		sleep 0.1
	done
	echo "node never listened"; return 1
}

cpu_ticks() { awk '{print $14 + $15}' /proc/$1/stat 2>/dev/null || echo 0; }
rss_kb() { awk '/VmRSS/{print $2}' /proc/$1/status 2>/dev/null || echo 0; }
anon_kb() { awk '/RssAnon/{print $2}' /proc/$1/status 2>/dev/null || echo 0; } # RSS minus the mapped binary

# sample_rss <pid> <seconds> -> "avgKB peakKB"
sample_rss() {
	local pid=$1 n=$2 sum=0 peak=0 cnt=0 r
	for _ in $(seq "$n"); do
		r=$(rss_kb "$pid"); [ "$r" -gt 0 ] || break
		sum=$((sum + r)); cnt=$((cnt + 1)); [ "$r" -gt "$peak" ] && peak=$r
		sleep 1
	done
	echo "$(( cnt > 0 ? sum / cnt : 0 )) $peak"
}

runtime_stats() { # heap_inuse sys goroutines, in MB/MB/count
	local h g
	h=$(curl -sf "http://127.0.0.1:$PPROF/debug/pprof/heap?debug=1" | tail -40)
	g=$(curl -sf "http://127.0.0.1:$PPROF/debug/pprof/goroutine?debug=1" | head -1 | awk '{print $NF}')
	echo "$(echo "$h" | awk '/# HeapInuse/{printf "%.1f", $4/1048576}') $(echo "$h" | awk '/# Sys /{printf "%.1f", $4/1048576}') ${g:-0}"
}

profile_text() { # <dir>
	local d=$1
	[ -s "$d/cpu.pprof" ] && go tool pprof -top -nodecount=30 "$BIN/v2node" "$d/cpu.pprof" >"$d/cpu_top.txt" 2>/dev/null || true
	[ -s "$d/heap.pprof" ] && go tool pprof -top -nodecount=30 -sample_index=inuse_space "$BIN/v2node" "$d/heap.pprof" >"$d/heap_top.txt" 2>/dev/null || true
	[ -s "$d/heap.pprof" ] && go tool pprof -top -nodecount=30 -sample_index=alloc_space "$BIN/v2node" "$d/heap.pprof" >"$d/alloc_top.txt" 2>/dev/null || true
}

mkdir -p "$OUT"
IDLE_CSV=$OUT/idle.csv
LOAD_CSV=$OUT/load.csv

# 1) Memory for N users with no traffic.
if [ -z "$ONLY" ] || [ "$ONLY" = idle ]; then
	echo "protocol,users,cores,mem_mb,rss_mb,rss_anon_mb,heap_inuse_mb,go_sys_mb,goroutines,startup_s,cpu_idle_pct" >"$IDLE_CSV"
	IFS=: read -r cores mem <<<"$IDLE_TIER"
	for proto in $PROTOCOLS; do
		for users in $IDLE_USERS; do
			d=$OUT/idle/$proto/u$users; mkdir -p "$d"
			echo "== idle $proto users=$users tier=${cores}C/${mem}M"
			t0=$(date +%s.%N)
			if ! start_node "$proto" "$users" "$cores" "$mem" "$d"; then
				echo "$proto,$users,$cores,$mem,FAILED,,,,,,oom=$(ooms)" >>"$IDLE_CSV"; cleanup; continue
			fi
			t1=$(date +%s.%N)
			sleep 10 # let the first traffic push and GC cycle happen
			c0=$(cpu_ticks $NODE_PID); sleep 10; c1=$(cpu_ticks $NODE_PID)
			read -r heap sys gor <<<"$(runtime_stats)"
			curl -sf "http://127.0.0.1:$PPROF/debug/pprof/heap" -o "$d/heap.pprof" || true
			rss=$(rss_kb $NODE_PID); anon=$(anon_kb $NODE_PID)
			echo "$proto,$users,$cores,$mem,$((rss / 1024)),$((anon / 1024)),$heap,$sys,$gor,$(awk "BEGIN{printf \"%.1f\", $t1 - $t0}"),$(( (c1 - c0) * 100 / $(getconf CLK_TCK) / 10 ))" | tee -a "$IDLE_CSV"
			cleanup; profile_text "$d"
		done
	done
fi

# 2) Fixed traffic load, per tier.
if [ -z "$ONLY" ] || [ "$ONLY" = load ]; then
	echo "protocol,users,cores,mem_mb,conns,target_down_mbps,down_mbps,up_mbps,cpu_pct,rss_avg_mb,rss_peak_mb,rss_anon_mb,cg_peak_mb,heap_inuse_mb,go_sys_mb,goroutines,ttfb_p50_ms,ttfb_p99_ms,dial_err,io_err,oom_kills,loadgen_cpus" >"$LOAD_CSV"
	for proto in $PROTOCOLS; do
		for tier in $TIERS; do
			IFS=: read -r cores mem <<<"$tier"
			for conns in $LOADS; do
				d=$OUT/load/$proto/${cores}c${mem}m/c$conns; mkdir -p "$d"
				echo "== load $proto tier=${cores}C/${mem}M conns=$conns"
				if ! start_node "$proto" "$LOAD_USERS" "$cores" "$mem" "$d"; then
					echo "$proto,$LOAD_USERS,$cores,$mem,$conns,FAILED" >>"$LOAD_CSV"; cleanup; continue
				fi
				taskset -c "$LG_CPUS" "$BIN/loadgen" -protocol "$proto" -server 127.0.0.1:$NODE_PORT -sink $SINK \
					-target $TARGET_IP:19000 -users "$LOAD_USERS" -conns "$conns" -down "$DOWN" -up "$UP" \
					-life "$LIFE" ${LG_FLAGS:-} -ramp ${RAMP}s -duration $((RAMP + STEADY))s -out "$d/loadgen.json" \
					>/dev/null 2>"$d/loadgen.log" &
				LG=$!; PIDS+=($LG)
				sleep "$RAMP"
				curl -sf "http://127.0.0.1:$PPROF/debug/pprof/profile?seconds=$((STEADY - 8))" -o "$d/cpu.pprof" &
				PROF=$!
				c0=$(cpu_ticks $NODE_PID)
				read -r rss_avg rss_peak <<<"$(sample_rss $NODE_PID $((STEADY - 4)))"
				c1=$(cpu_ticks $NODE_PID); anon=$(anon_kb $NODE_PID)
				read -r heap sys gor <<<"$(runtime_stats)"
				curl -sf "http://127.0.0.1:$PPROF/debug/pprof/heap" -o "$d/heap.pprof" || true
				wait $PROF || true
				wait $LG || true
				alive=1; kill -0 $NODE_PID 2>/dev/null || alive=0
				cpu=$(( (c1 - c0) * 100 / $(getconf CLK_TCK) / (STEADY - 4) ))
				lg=$(cat "$d/loadgen.json" 2>/dev/null || echo '{}')
				j() { echo "$lg" | python3 -c "import json,sys; v=json.load(sys.stdin).get('$1',0); print(round(v*${2:-1},1) if isinstance(v,float) else v)"; }
				echo "$proto,$LOAD_USERS,$cores,$mem,$conns,$(j target_down_MBps 8),$(j down_MBps 8),$(j up_MBps 8),$cpu,$((rss_avg / 1024)),$((rss_peak / 1024)),$((anon / 1024)),$(( $(cg_peak) >> 20 )),$heap,$sys,$gor,$(j ttfb_p50_ms),$(j ttfb_p99_ms),$(j dial_errors),$(j io_errors),$(ooms)$([ $alive = 1 ] || echo ' (node died)'),$LG_CPUS" | tee -a "$LOAD_CSV"
				cleanup; profile_text "$d"
				sleep 2
			done
		done
	done
fi
echo "results in $OUT"

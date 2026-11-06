#!/bin/bash
# Attaches `sample` for 5s (1ms interval) after warm-up, renders on-CPU flamegraphs.
cd "$(dirname "$0")"
FG=/Users/haojiang/Desktop/oltp_bench_study/FlameGraph
mkdir -p profiles svg
profile() {
	local build=$1 c=$2 iters=$3
	local name=${c}_t1_$build
	rm -f /tmp/index_bench_ready
	READY_FILE=/tmp/index_bench_ready ./bench_$build run bench.duckdb $c $iters 1 >>profiles/bench_during_profile.csv &
	local bpid=$!
	while [ ! -f /tmp/index_bench_ready ]; do sleep 0.1; done
	sleep 1
	sample $bpid 5 1 -file profiles/$name.sample >/dev/null 2>&1
	wait $bpid
	awk -f $FG/stackcollapse-sample.awk profiles/$name.sample >profiles/$name.collapsed
	rm -f profiles/$name.sample
	grep -v -E 'semaphore_wait|__psynch_cvwait|__ulock_wait|semaphore_timedwait|__semwait_signal|nanosleep' \
		profiles/$name.collapsed >profiles/$name.busy
	$FG/flamegraph.pl --title "$c, 1 thread ($build)" --width 1800 profiles/$name.busy >svg/$name.svg
	echo "$name samples=$(awk '{s+=$NF} END {print s+0}' profiles/$name.busy)"
}
profile main k_point_other_col 2000
profile pr k_point_other_col 200000
profile main id_range_other_col 40000
profile pr id_range_other_col 200000
echo PROFILES_DONE

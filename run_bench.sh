#!/bin/bash
# Interleaved main/PR runs: 5 runs per case and thread setting. Output: results/raw.csv
cd "$(dirname "$0")"
mkdir -p results
ITERS=${ITERS:-2000}
OUT=results/raw.csv
echo "threads,build,run,case,us_per_query,checksum" >"$OUT"
for threads in 0 1; do
	for c in k_range_ne k_point_other_col id_point_other_col id_range_other_col k_range_exact; do
		for run in 1 2 3 4 5; do
			for b in main pr; do
				if [ "$threads" = 0 ]; then
					line=$(./bench_$b run bench.duckdb $c $ITERS)
				else
					line=$(./bench_$b run bench.duckdb $c $ITERS $threads)
				fi
				echo "$threads,$b,$run,$line" >>"$OUT"
			done
		done
	done
done
echo BENCH_DONE

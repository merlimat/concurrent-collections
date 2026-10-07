#!/bin/bash
# Runs the latency benchmark for every queue, producer count and rate, each in a fresh JVM, and
# appends the results to a CSV file. Then prints the median of the rounds.
#
#   benchmark/run.sh [rounds]        3 rounds by default, about 15 minutes
#
# Environment overrides: QUEUES, PRODUCERS, RATES, WARMUP and MEASURE (seconds), OUT (the CSV file,
# benchmark/results.csv by default) and JAVA_OPTS.
set -u
D=$(cd "$(dirname "$0")" && pwd)
ROUNDS=${1:-3}
QUEUES=${QUEUES:-"lock unbounded bounded"}
PRODUCERS=${PRODUCERS:-"1 4 8 16 32"}
RATES=${RATES:-"1000000 4000000"}
WARMUP=${WARMUP:-2}
MEASURE=${MEASURE:-5}
OUT=${OUT:-"$D/results.csv"}
JAVA_OPTS=${JAVA_OPTS:-"-XX:+UseZGC -Xms1g -Xmx1g"}

"$D/../gradlew" -p "$D/.." -q :benchmark:installDist || exit 1
CP="$D/build/install/benchmark/lib/*"

[ -f "$OUT" ] || echo "round,queue,producers,target,achieved,put_mean,put_p50,put_p99,put_p999,put_max,e2e_mean,e2e_p50,e2e_p99,e2e_p999,e2e_max,batch,prod_ns_op,cons_ns_op,proc_ns_op,prod_cores,cons_cores" > "$OUT"
for round in $(seq 1 "$ROUNDS"); do
  for producers in $PRODUCERS; do
    for rate in $RATES; do
      # Alternate the order of the queues between rounds
      order=$QUEUES
      if (( round % 2 == 0 )); then order=""; for q in $QUEUES; do order="$q $order"; done; fi
      for queue in $order; do
        line=$(java $JAVA_OPTS -cp "$CP" io.github.merlimat.concurrent.benchmark.QueueLatencyBenchmark \
          "$queue" "$producers" "$rate" "$WARMUP" "$MEASURE")
        echo "$round,${line:-$queue,$producers,$rate,ERROR}" | tee -a "$OUT"
      done
    done
  done
done
python3 "$D/report.py" "$OUT"

#!/bin/sh
# Built with: zig cc -target aarch64-linux-musl -static -O2 -o counters scripts/board/counters.c
# Runs on a board, from /tmp/dd-outview only. User space, nothing written outside it.
D=/tmp/dd-outview
cd $D || exit 1
echo $$ > $D/run.pid
out=$D/results.txt
: > $out
T=./daedalus-process-timings
W=./daedalus-sweep-worker
for round in 1 2 3; do
  echo "== fanout round $round" >> $out
  $T fanout $W 301 >> $out 2>&1
done
# Counters per tick: each graph at two run counts; the difference over the extra runs is per tick.
for round in 1 2 3; do
  for producer in vec view; do
    for graph in producer host worker fan; do
      for first in 0 1; do
        if [ $first = 1 ] && [ $graph != fan ]; then continue; fi
        for runs in 201 1201; do
          echo "== counters round $round producer $producer graph $graph worker_first $first runs $runs" >> $out
          if [ $first = 1 ]; then
            DAEDALUS_FAN_WORKER_FIRST=1 DAEDALUS_FAN_ONLY=f32:$producer:$graph:1048576 ./counters $T fanout $W $runs >> $out 2>&1
          else
            DAEDALUS_FAN_ONLY=f32:$producer:$graph:1048576 ./counters $T fanout $W $runs >> $out 2>&1
          fi
        done
      done
    done
  done
done
echo "== done $?" >> $out

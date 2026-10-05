# Benchmark

`QueueLatencyBenchmark` measures a queue the way a batching thread uses it: N producer threads insert at a fixed total rate, and one consumer takes everything available in a loop with `takeAll`.

## What it measures

- **Paced producers.** Each producer parks until its next element is due, then inserts what it owes in a burst of at most 4,096 elements. It parks for at least 50 µs, so that the pacing itself doesn't use most of the CPU. Producers stop inserting if the consumer falls 2M elements behind.
- **`put()` latency:** the time each `put()` call takes.
- **End-to-end latency:** from just before `put()` to the consumer taking the element.
- **CPU:** the CPU time of the producers and the consumer while measuring, per element and in cores.

Each configuration runs in a fresh JVM (ZGC, 1 GB heap): 2 s of warm-up, then 5 s measured. Each round runs every configuration, and the order of the queues alternates between rounds.

The queues:

- `lock`: `BatchedArrayBlockingQueue` with a capacity of 10,000. It is the lock-based queue that the Oxia Java client's batchers used before, copied here as the baseline.
- `unbounded`: `MpscUnboundedBatchedQueue`.
- `bounded`: `MpscBoundedBatchedQueue` with a capacity of 10,000.

## Running

```bash
benchmark/run.sh
```

By default it runs 3 rounds of every queue with 1, 4, 8, 16 and 32 producers, at 1M and 4M ops/s, in about 15 minutes. It appends each result to `benchmark/results.csv`, then prints the median of the rounds. The environment variables `QUEUES`, `PRODUCERS`, `RATES`, `WARMUP`, `MEASURE`, `OUT` and `JAVA_OPTS` override the defaults, for example:

```bash
QUEUES="lock bounded" PRODUCERS="8 16" benchmark/run.sh 5
```

`benchmark/report.py <results.csv>` prints the summary of an existing file.

Run it on Linux. On macOS, HotSpot's `System.nanoTime()` updates a global variable with a compare-and-swap to stay monotonic, so each call costs 0.4–0.8 µs once 8–16 threads call it. The benchmark calls it around every `put()`, so on a Mac it mostly measures that contention.

## Results

### Linux VM, 2026-10-04

Ubuntu 26.04 (Linux 7.0, aarch64) in a QEMU VM with 8 vCPUs, on an Apple M1 Max, with OpenJDK 26.0.2. The tables show the median of 3 rounds; the raw data is in [`results/2026-10-04-linux-vm.csv`](results/2026-10-04-linux-vm.csv).

This run predates the library, so it has no `bounded` column. The `unbounded` queue was measured with the same algorithm as `MpscUnboundedBatchedQueue`, while it was still part of the Oxia client.

`lock` → `unbounded`:

| Producers | Rate | e2e p50 (µs) | e2e p99.9 (µs) | `put()` mean (ns) | `put()` p99.9 (µs) |
|---|---|---|---|---|---|
| 1 | 1M | 26.4 → 26.9 | 406 → 478 | 97 → 88 | 8.5 → 8.5 |
| 4 | 1M | 111.6 → 101.2 | 1,286 → 945 | 326 → 161 | 57.8 → 24.6 |
| 8 | 1M | 362.5 → 277.0 | 2,568 → 2,220 | 611 → 160 | 19.3 → 14.8 |
| 16 | 1M | 526.8 → 337.2 | 6,668 → 12,550 | 1,128 → 148 | 14.8 → 9.9 |
| 32 | 1M | 6,135.8 → 356.6 | 16,974 → 4,186 | 23,263 → 170 | 6,881.3 → 7.7 |
| 1 | 4M | 26.7 → 8.7 | 551 → 393 | 157 → 67 | 53.2 → 5.2 |
| 4 | 4M | 155.1 → 67.6 | 1,333 → 1,135 | 358 → 138 | 28.4 → 8.8 |
| 8 | 4M | 644.6 → 215.3 | 3,084 → 2,738 | 1,168 → 151 | 17.5 → 3.1 |
| 16 | 4M | 1,577.0 → 310.3 | 4,760 → 4,960 | 3,607 → 179 | 1,749.0 → 3.1 |
| 32 | 4M | 2,916.4* → 351.0 | 9,912* → 20,906 | 11,369 → 249 | 47.0 → 3.2 |

\* With 32 producers the lock queue sustained only 2.74–2.80M of the 4M ops/s, so its latency there was measured under a lighter load. The unbounded queue hit every target rate.

- **`put()` stays flat.** The unbounded queue's mean is 67–249 ns everywhere and its p99.9 at most 25 µs. The lock queue's mean climbs to 11–23 µs with 32 producers, and its p99.9 reaches 6.9 ms.
- **Median end-to-end latency is lower** at every producer count at 4M, by 2.3–8.3×, and from 8 producers up at 1M, by 1.3–17×: 357 µs instead of 6.1 ms with 32 producers.
- **The e2e tails are mixed, and noisy.** Single rounds vary up to 10×: the unbounded queue with 8 producers at 1M measured 2,156, 2,220 and 34,079 µs. The one tail result that held in every round is 32 producers at 1M: 16–33 ms for the lock queue, 2.9–4.3 ms for the unbounded queue.

In a VM, waking a thread parked on an idle vCPU goes through the hypervisor, which adds tens to hundreds of µs. That inflates every absolute latency here: compare the queues with each other, not with bare-metal numbers.

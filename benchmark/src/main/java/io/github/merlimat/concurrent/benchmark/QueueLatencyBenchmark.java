/*
 * Copyright 2026 Matteo Merli <matteo.merli@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.merlimat.concurrent.benchmark;

import io.github.merlimat.concurrent.queue.BatchedBlockingQueue;
import io.github.merlimat.concurrent.queue.MpscBoundedBatchedQueue;
import io.github.merlimat.concurrent.queue.MpscUnboundedBatchedQueue;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.locks.LockSupport;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

/**
 * The latency of a queue under paced producers: N threads insert at a fixed total rate, and one
 * consumer takes everything available, in a loop.
 *
 * <p>It prints one CSV line: the queue, the producers, the target and achieved rates, the {@code
 * put()} latency (mean, p50, p99, p99.9 and max, in ns), the end-to-end latency from just before
 * {@code put()} to the consumer taking the element (same statistics, in µs), the mean batch the
 * consumer takes, the CPU time per element of the producers, the consumer and the whole process
 * (ns), and the cores that the producers and the consumer used.
 *
 * <p>Usage: {@code QueueLatencyBenchmark <lock|unbounded|bounded> <producers> <ops/s> <warmup s>
 * <measure s>}
 */
public class QueueLatencyBenchmark {

    // The capacity of the bounded queues
    static final int CAPACITY = 10_000;

    // Producers park until their next element is due, but at least this long: frequent wake-ups
    // would make the pacing itself use most of the CPU
    static final long MIN_PARK_NANOS = 50_000;

    // After a park, a producer inserts the elements it owes in a burst of at most this many
    static final long MAX_BURST = 4096;

    // Producers stop inserting when the consumer falls this far behind
    static final long MAX_BACKLOG = 2_000_000;

    record Task(long enqueuedAt) {}

    static volatile boolean stop;
    static volatile long consumed;
    static volatile long drains;

    @SuppressWarnings("deprecation") // Thread.getId(): threadId() needs Java 19
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println(
                    "Usage: QueueLatencyBenchmark <lock|unbounded|bounded> <producers> <ops/s> <warmup s>"
                            + " <measure s>");
            System.exit(1);
        }
        String queueType = args[0];
        int producers = Integer.parseInt(args[1]);
        double rate = Double.parseDouble(args[2]);
        int warmup = Integer.parseInt(args[3]);
        int measure = Integer.parseInt(args[4]);

        // Never let a run hang the whole matrix
        Thread watchdog =
                new Thread(
                        () -> {
                            try {
                                Thread.sleep((warmup + measure + 20) * 1000L);
                            } catch (InterruptedException e) {
                                return;
                            }
                            System.out.printf("%s,%d,%.0f,TIMEOUT%n", queueType, producers, rate);
                            Runtime.getRuntime().halt(3);
                        });
        watchdog.setDaemon(true);
        watchdog.start();

        BatchedBlockingQueue<Task> queue =
                switch (queueType) {
                    case "lock" -> new BatchedArrayBlockingQueue<>(CAPACITY);
                    case "unbounded" -> new MpscUnboundedBatchedQueue<>();
                    case "bounded" -> new MpscBoundedBatchedQueue<>(CAPACITY);
                    default -> throw new IllegalArgumentException("Unknown queue: " + queueType);
                };

        SingleWriterRecorder[] putLatency = new SingleWriterRecorder[producers];
        Thread[] threads = new Thread[producers];
        for (int p = 0; p < producers; p++) {
            SingleWriterRecorder recorder = putLatency[p] = new SingleWriterRecorder(3);
            threads[p] =
                    new Thread(() -> produce(queue, recorder, rate / producers, producers), "producer-" + p);
            threads[p].setDaemon(true);
        }
        SingleWriterRecorder e2eLatency = new SingleWriterRecorder(3);
        Thread consumer = new Thread(() -> consume(queue, e2eLatency), "consumer");
        consumer.setDaemon(true);

        consumer.start();
        for (Thread t : threads) {
            t.start();
        }
        Thread.sleep(warmup * 1000L);

        ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        long[] producerCpu0 = new long[producers];
        for (int p = 0; p < producers; p++) {
            producerCpu0[p] = mx.getThreadCpuTime(threads[p].getId());
        }
        long consumerCpu0 = mx.getThreadCpuTime(consumer.getId());
        long processCpu0 = os.getProcessCpuTime();
        long consumed0 = consumed;
        long drains0 = drains;
        for (SingleWriterRecorder r : putLatency) {
            r.getIntervalHistogram();
        }
        e2eLatency.getIntervalHistogram();
        long t0 = System.nanoTime();

        Thread.sleep(measure * 1000L);

        long elapsed = System.nanoTime() - t0;
        long producerCpu = 0;
        for (int p = 0; p < producers; p++) {
            producerCpu += mx.getThreadCpuTime(threads[p].getId()) - producerCpu0[p];
        }
        long consumerCpu = mx.getThreadCpuTime(consumer.getId()) - consumerCpu0;
        long processCpu = os.getProcessCpuTime() - processCpu0;
        long ops = consumed - consumed0;
        long batches = drains - drains0;
        Histogram put = new Histogram(3);
        for (SingleWriterRecorder r : putLatency) {
            put.add(r.getIntervalHistogram());
        }
        Histogram e2e = e2eLatency.getIntervalHistogram();

        double secs = elapsed / 1e9;
        System.out.printf(
                "%s,%d,%.0f,%.0f,%.0f,%d,%d,%d,%d,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.0f,%.0f,%.0f,%.2f,%.2f%n",
                queueType,
                producers,
                rate,
                ops / secs,
                put.getMean(),
                put.getValueAtPercentile(50),
                put.getValueAtPercentile(99),
                put.getValueAtPercentile(99.9),
                put.getMaxValue(),
                e2e.getMean() / 1e3,
                e2e.getValueAtPercentile(50) / 1e3,
                e2e.getValueAtPercentile(99) / 1e3,
                e2e.getValueAtPercentile(99.9) / 1e3,
                e2e.getMaxValue() / 1e3,
                (double) ops / Math.max(1, batches),
                (double) producerCpu / ops,
                (double) consumerCpu / ops,
                (double) processCpu / ops,
                producerCpu / (double) elapsed,
                consumerCpu / (double) elapsed);
        System.out.flush();
        // Producers may be blocked on a full bounded queue: don't wait for them
        Runtime.getRuntime().halt(0);
    }

    static void produce(
            BatchedBlockingQueue<Task> queue, SingleWriterRecorder recorder, double rate, int producers) {
        double perNano = rate / 1e9;
        long start = System.nanoTime();
        long emitted = 0;
        try {
            while (!stop) {
                long now = System.nanoTime();
                long due = (long) ((now - start) * perNano);
                if (emitted >= due || emitted * producers - consumed > MAX_BACKLOG) {
                    long nextDue = start + (long) ((emitted + 1) / perNano);
                    LockSupport.parkNanos(Math.max(MIN_PARK_NANOS, nextDue - now));
                    continue;
                }
                // Catch up with the schedule: after a park the producer emits its backlog in a burst
                long burst = Math.min(due - emitted, MAX_BURST);
                for (long n = 0; n < burst; n++) {
                    long before = System.nanoTime();
                    queue.put(new Task(before));
                    recorder.recordValue(System.nanoTime() - before);
                }
                emitted += burst;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static void consume(BatchedBlockingQueue<Task> queue, SingleWriterRecorder recorder) {
        Task[] batch = new Task[CAPACITY];
        try {
            while (!stop) {
                int count = queue.takeAll(batch);
                long now = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    recorder.recordValue(now - batch[i].enqueuedAt());
                    batch[i] = null;
                }
                consumed += count;
                drains++;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

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
package io.github.merlimat.concurrent.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class MpscBoundedBatchedQueueTest extends BatchedQueueContractTest {

    @Override
    <T> BatchedBlockingQueue<T> newQueue(int chunkSize) {
        // Small enough that the producers in the concurrent tests wait for the consumer
        return new MpscBoundedBatchedQueue<>(1024, chunkSize);
    }

    @Test
    void capacityMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new MpscBoundedBatchedQueue<>(0));
        assertThrows(IllegalArgumentException.class, () -> new MpscBoundedBatchedQueue<>(-1));
    }

    @Test
    void offerFailsWhenFull() throws Exception {
        var queue = new MpscBoundedBatchedQueue<Integer>(4, 2);
        for (int i = 0; i < 4; i++) {
            assertEquals(4 - i, queue.remainingCapacity());
            assertTrue(queue.offer(i));
        }
        assertEquals(0, queue.remainingCapacity());
        assertFalse(queue.offer(4));
        assertThrows(IllegalStateException.class, () -> queue.add(4));
        assertThrows(NullPointerException.class, () -> queue.offer(null));
        assertEquals(4, queue.size());

        assertEquals(0, queue.take());
        assertTrue(queue.offer(4));
        assertFalse(queue.offer(5));
        assertEquals(List.of(1, 2, 3, 4), drain(queue));
    }

    @Test
    @Timeout(10)
    void putWaitsWhileBeyondCapacity() throws Exception {
        var queue = new MpscBoundedBatchedQueue<Integer>(2);
        queue.put(0);
        queue.put(1);

        var done = new CompletableFuture<Void>();
        startDaemon(
                () -> {
                    queue.put(2);
                    done.complete(null);
                });

        // The element is inserted right away, then its producer waits
        waitUntil(() -> queue.size() == 3);
        Thread.sleep(100);
        assertFalse(done.isDone());
        assertEquals(0, queue.remainingCapacity());

        assertEquals(0, queue.take());
        done.get(5, TimeUnit.SECONDS);
        assertEquals(List.of(1, 2), drain(queue));
    }

    @Test
    void timedOfferTimesOut() throws Exception {
        var queue = new MpscBoundedBatchedQueue<Integer>(1);
        assertTrue(queue.offer(0, 0, TimeUnit.MILLISECONDS));
        assertFalse(queue.offer(1, 0, TimeUnit.MILLISECONDS));

        long start = System.nanoTime();
        assertFalse(queue.offer(1, 50, TimeUnit.MILLISECONDS));
        assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(50));
        assertEquals(List.of(0), drain(queue));
    }

    @Test
    @Timeout(10)
    void timedOfferWaitsForRoom() throws Exception {
        var queue = new MpscBoundedBatchedQueue<Integer>(1);
        queue.put(0);

        var offered = new CompletableFuture<Boolean>();
        startDaemon(
                () -> {
                    try {
                        offered.complete(queue.offer(1, 1, TimeUnit.MINUTES));
                    } catch (Throwable t) {
                        offered.completeExceptionally(t);
                    }
                });

        // Unlike put, it waits before inserting
        Thread.sleep(100);
        assertFalse(offered.isDone());
        assertEquals(1, queue.size());

        assertEquals(0, queue.take());
        assertTrue(offered.get(5, TimeUnit.SECONDS));
        assertEquals(1, queue.take());
    }

    @Test
    @Timeout(10)
    void interruptedPutKeepsItsElement() throws Exception {
        var queue = new MpscBoundedBatchedQueue<Integer>(1);
        queue.put(0);

        var interrupted = new CompletableFuture<Boolean>();
        Thread producer =
                startDaemon(
                        () -> {
                            queue.put(1);
                            interrupted.complete(Thread.currentThread().isInterrupted());
                        });

        waitUntil(() -> queue.size() == 2);
        producer.interrupt();
        assertTrue(interrupted.get(5, TimeUnit.SECONDS));
        assertEquals(List.of(0, 1), drain(queue));
    }

    @Test
    @Timeout(10)
    void interruptedTimedOfferInsertsNothing() throws Exception {
        var queue = new MpscBoundedBatchedQueue<Integer>(1);
        queue.put(0);

        var outcome = new CompletableFuture<Throwable>();
        Thread producer =
                startDaemon(
                        () -> {
                            try {
                                queue.offer(1, 1, TimeUnit.MINUTES);
                                outcome.complete(null);
                            } catch (Throwable t) {
                                outcome.complete(t);
                            }
                        });

        Thread.sleep(100);
        producer.interrupt();
        assertInstanceOf(InterruptedException.class, outcome.get(5, TimeUnit.SECONDS));
        assertEquals(List.of(0), drain(queue));
    }

    @Test
    @Timeout(60)
    void producersStayWithinCapacity() throws Exception {
        // A small capacity and small batches: the producers wait for the consumer most of the time
        int capacity = 16;
        int producers = 8;
        int perProducer = 50_000;
        var queue = new MpscBoundedBatchedQueue<Long>(capacity, 4);

        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            long id = p;
            threads.add(startDaemon(() -> produce(queue, id, perProducer)));
        }

        // Each producer has at most one element, or one batch of 3, over the capacity
        int bound = capacity + producers * 3;
        int[] nextSequence = new int[producers];
        Long[] batch = new Long[8];
        int received = 0;
        int maxSize = 0;
        while (received < producers * perProducer) {
            maxSize = Math.max(maxSize, queue.size());
            int count = queue.takeAll(batch);
            for (int i = 0; i < count; i++) {
                int id = (int) (batch[i] >>> 32);
                int sequence = (int) (long) batch[i];
                assertEquals(nextSequence[id]++, sequence, "producer " + id);
            }
            received += count;
        }
        assertTrue(maxSize <= bound, "the queue held " + maxSize + " elements, over " + bound);

        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(0, queue.size());
    }

    @Test
    @Timeout(60)
    void noLostProducerWakeUps() throws Exception {
        // With a capacity of 1, every put waits for the consumer to take the element before it
        var queue = new MpscBoundedBatchedQueue<Integer>(1);
        int elements = 50_000;
        startDaemon(
                () -> {
                    for (int i = 0; i < elements; i++) {
                        queue.put(i);
                    }
                });

        for (int i = 0; i < elements; i++) {
            assertEquals(i, queue.poll(5, TimeUnit.SECONDS), "lost wake-up at element " + i);
        }
    }

    private static List<Integer> drain(BatchedBlockingQueue<Integer> queue) {
        List<Integer> list = new ArrayList<>();
        queue.drainTo(list);
        return list;
    }

    private static void waitUntil(BooleanSupplier condition) throws InterruptedException {
        while (!condition.getAsBoolean()) {
            Thread.sleep(1);
        }
    }
}

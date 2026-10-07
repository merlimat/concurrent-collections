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

import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A lock-free, multi-producer single-consumer queue that the consumer drains in batches, with a
 * soft capacity. It is {@link MpscUnboundedBatchedQueue} plus admission control, so that producers
 * wait when the consumer falls behind.
 *
 * <p>{@link #offer(Object)} fails when the queue holds {@code capacity} elements. {@link #put}
 * never waits to insert: it publishes its element right away, then blocks while that element is
 * beyond the capacity, until the consumer has taken enough of the elements before it. Producers
 * that race at the limit can each insert one element over it, so the queue holds at most {@code
 * capacity} elements plus one per producer, or one batch per producer with {@link #putAll}.
 *
 * <p>Inserting first keeps the producers' fast path a single atomic increment, and an interrupt can
 * never leave a claimed slot unpublished, which would stall the consumer: a blocked {@code put} that
 * is interrupted returns with its element inserted and the thread's interrupt status set.
 *
 * <p>Producers compare against a cached limit, so they only read the consumer's index when they get
 * close to the capacity. A producer that has to wait spins briefly, then blocks on a lock, which is
 * only taken by waiting producers, and by the consumer when there are producers to wake up.
 *
 * <p>Any number of threads may insert elements. The methods that take or inspect elements ({@link
 * #take}, {@link #takeAll}, {@link #poll}, {@link #pollAll}, {@link #peek}, {@link #drainTo}, and
 * the ones built on them, such as {@code remove()} and {@code clear()}) must only be called by one
 * thread at a time. Iterating, and the collection methods built on iteration such as {@code
 * contains} and {@code toArray}, are not supported.
 *
 * @param <T> the type of the elements
 */
public final class MpscBoundedBatchedQueue<T> extends AbstractMpscBatchedQueue<T> {

    private final int capacity;

    // A lower bound of the consumer index plus the capacity: an element below it is within the
    // capacity, without reading the consumer's index
    private volatile long producerLimit;

    // The producers blocked until the consumer takes elements. Only changed under the lock.
    private volatile int waitingProducers;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition consumed = lock.newCondition();

    /**
     * Creates a queue with chunks of 1024 slots.
     *
     * @param capacity how many elements the queue holds before producers have to wait
     */
    public MpscBoundedBatchedQueue(int capacity) {
        this(capacity, DEFAULT_CHUNK_SIZE);
    }

    /**
     * Creates a queue with the given chunk size.
     *
     * @param capacity how many elements the queue holds before producers have to wait
     * @param chunkSize the number of slots in each chunk, a power of 2
     */
    public MpscBoundedBatchedQueue(int capacity, int chunkSize) {
        super(chunkSize);
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.producerLimit = capacity;
    }

    /** Inserts the element, unless the queue holds {@code capacity} elements already. */
    @Override
    public boolean offer(T e) {
        Objects.requireNonNull(e);
        if (!withinCapacity(producerIndex)) {
            return false;
        }
        publish(e);
        return true;
    }

    /**
     * Inserts the element, waiting up to the given time for the queue to hold fewer than {@code
     * capacity} elements.
     */
    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(e);
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        long index;
        while (!withinCapacity(index = producerIndex)) {
            if (!awaitConsumer(index, true, deadline)) {
                return false;
            }
        }
        publish(e);
        return true;
    }

    /**
     * Inserts the element, then waits while it is beyond the capacity. If interrupted while waiting,
     * it returns with the element inserted and the interrupt status set.
     */
    @Override
    public void put(T e) {
        awaitWithinCapacity(publish(e));
    }

    /**
     * Inserts the elements, in order, then waits while the last one is beyond the capacity. If
     * interrupted while waiting, it returns with the elements inserted and the interrupt status set.
     */
    @Override
    public void putAll(T[] a, int offset, int len) {
        long last = publishAll(a, offset, len);
        if (last >= 0) {
            awaitWithinCapacity(last);
        }
    }

    /** The capacity minus the elements in the queue, or 0 if it holds more. */
    @Override
    public int remainingCapacity() {
        return Math.max(0, capacity - size());
    }

    // The consumer took elements: wake up the producers waiting for it, if any
    @Override
    void onConsumed() {
        // The consumer index was just written. Order it before reading the waiting producers, who
        // write their count before reading the consumer index: one side sees the other's write.
        VarHandle.fullFence();
        if (waitingProducers > 0) {
            lock.lock();
            try {
                consumed.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    // An inserted element waits while it is beyond the capacity
    private void awaitWithinCapacity(long index) {
        if (withinCapacity(index)) {
            return;
        }
        try {
            awaitConsumer(index, false, 0L);
        } catch (InterruptedException e) {
            // The element is inserted already: stop waiting, keeping the interrupt
            Thread.currentThread().interrupt();
        }
    }

    // Whether the element at index is within the capacity, that is, whether the consumer has taken
    // the elements up to index - capacity
    private boolean withinCapacity(long index) {
        if (index < producerLimit) {
            return true;
        }
        long limit = consumerIndexVolatile() + capacity;
        if (limit > producerLimit) {
            // Racing producers may store an older limit: it is still a lower bound
            producerLimit = limit;
        }
        return index < limit;
    }

    // Waits until the element at index is within the capacity: spins briefly, then blocks until
    // the consumer signals that it took elements. Returns false if the deadline passes first.
    private boolean awaitConsumer(long index, boolean timed, long deadline)
            throws InterruptedException {
        for (int spins = 0; spins < SPINS_BEFORE_PARK; spins++) {
            Thread.onSpinWait();
            if (withinCapacity(index)) {
                return true;
            }
        }
        lock.lockInterruptibly();
        try {
            waitingProducers++;
            try {
                // Re-check after announcing: from now on, the consumer signals after taking elements
                while (!withinCapacity(index)) {
                    if (!timed) {
                        consumed.await();
                    } else {
                        long nanos = deadline - System.nanoTime();
                        if (nanos <= 0) {
                            return false;
                        }
                        consumed.awaitNanos(nanos);
                    }
                }
                return true;
            } finally {
                waitingProducers--;
            }
        } finally {
            lock.unlock();
        }
    }
}

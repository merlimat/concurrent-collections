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
package io.github.merlimat.queues;

import java.util.concurrent.TimeUnit;

/**
 * An unbounded, lock-free, multi-producer single-consumer queue that the consumer drains in
 * batches.
 *
 * <p>A producer claims a slot with a single atomic increment of the producer index, so contending
 * producers never retry, and then publishes its element into the slot. Slots live in fixed-size
 * chunks linked together: the queue grows by appending a chunk, never by copying, and producers
 * never block. The next chunk is appended as soon as a chunk starts being used, so that producers
 * rarely allocate between claiming a slot and publishing into it. The consumer reads the producer
 * index to know how far it may go, takes every element up to it in one pass, stopping early at a
 * claimed slot whose element is not published yet, and advances the consumer index once for the
 * whole batch.
 *
 * <p>The consumer waits without a lock, and never yields: on macOS, {@code Thread.yield()} can
 * stall the thread for a whole 10 ms scheduler quantum. Whether the queue is empty or the next slot
 * is claimed but not published yet, the consumer spins briefly, then announces which slot it waits
 * for, re-checks it and parks. Only the producer that publishes that slot unparks it, so producers
 * pay for a wake-up only when the consumer is actually parked waiting for them.
 *
 * <p>The design follows JCTools' {@code MpscUnboundedXaddArrayQueue}, without chunk pooling and
 * with a lock-free chunk append.
 *
 * <p>Any number of threads may insert elements. The methods that take or inspect elements ({@link
 * #take}, {@link #takeAll}, {@link #poll}, {@link #pollAll}, {@link #peek}, {@link #drainTo}, and
 * the ones built on them, such as {@code remove()} and {@code clear()}) must only be called by one
 * thread at a time. Iterating, and the collection methods built on iteration such as {@code
 * contains} and {@code toArray}, are not supported.
 *
 * @param <T> the type of the elements
 */
public final class MpscUnboundedBatchedQueue<T> extends AbstractMpscBatchedQueue<T> {

    /** Creates a queue with chunks of 1024 slots. */
    public MpscUnboundedBatchedQueue() {
        this(DEFAULT_CHUNK_SIZE);
    }

    /**
     * Creates a queue with the given chunk size.
     *
     * @param chunkSize the number of slots in each chunk, a power of 2
     */
    public MpscUnboundedBatchedQueue(int chunkSize) {
        super(chunkSize);
    }

    /** Inserts the element. It never fails, nor waits. */
    @Override
    public boolean offer(T e) {
        publish(e);
        return true;
    }

    /** Inserts the element. It never fails, nor waits. */
    @Override
    public boolean offer(T e, long timeout, TimeUnit unit) {
        publish(e);
        return true;
    }

    /** Inserts the element. It never waits. */
    @Override
    public void put(T e) {
        publish(e);
    }

    /** Inserts the elements, in order. It never waits. */
    @Override
    public void putAll(T[] a, int offset, int len) {
        publishAll(a, offset, len);
    }

    /** Always {@link Integer#MAX_VALUE}: the queue is unbounded. */
    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }
}

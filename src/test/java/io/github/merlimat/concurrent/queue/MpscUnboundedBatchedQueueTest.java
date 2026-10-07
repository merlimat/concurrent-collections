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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class MpscUnboundedBatchedQueueTest extends BatchedQueueContractTest {

    @Override
    <T> BatchedBlockingQueue<T> newQueue(int chunkSize) {
        return new MpscUnboundedBatchedQueue<>(chunkSize);
    }

    @Test
    void unbounded() {
        var queue = new MpscUnboundedBatchedQueue<Integer>(4);
        assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
        for (int i = 0; i < 10_000; i++) {
            queue.put(i);
        }
        assertEquals(10_000, queue.size());
        assertEquals(Integer.MAX_VALUE, queue.remainingCapacity());
        assertEquals("MpscUnboundedBatchedQueue[size=10000]", queue.toString());
    }

    @Test
    void chunkSizeMustBeAPowerOf2() {
        assertThrows(IllegalArgumentException.class, () -> new MpscUnboundedBatchedQueue<>(0));
        assertThrows(IllegalArgumentException.class, () -> new MpscUnboundedBatchedQueue<>(3));
    }
}

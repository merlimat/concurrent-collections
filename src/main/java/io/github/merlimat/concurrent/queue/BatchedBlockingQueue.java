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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A {@link BlockingQueue} whose producers can insert several elements at once, and whose consumer
 * takes all the available elements in one call.
 *
 * @param <T> the type of the elements
 */
public interface BatchedBlockingQueue<T> extends BlockingQueue<T> {

    /**
     * Inserts {@code len} elements of {@code a}, starting at {@code offset}, keeping their order.
     *
     * @param a the elements
     * @param offset the index of the first element to insert
     * @param len how many elements to insert
     * @throws InterruptedException if interrupted while waiting for space
     * @throws NullPointerException if any of the elements is null
     */
    void putAll(T[] a, int offset, int len) throws InterruptedException;

    /**
     * Moves the available elements into {@code array}, waiting until there is at least one.
     *
     * @param array where to move the elements
     * @return how many elements were moved, at most {@code array.length}
     * @throws InterruptedException if interrupted while waiting
     */
    int takeAll(T[] array) throws InterruptedException;

    /**
     * Moves the available elements into {@code array}, waiting up to the given time for at least one
     * to be available.
     *
     * @param array where to move the elements
     * @param timeout how long to wait, in units of {@code unit}
     * @param unit the unit of {@code timeout}
     * @return how many elements were moved, at most {@code array.length}, or 0 if the time elapsed
     * @throws InterruptedException if interrupted while waiting
     */
    int pollAll(T[] array, long timeout, TimeUnit unit) throws InterruptedException;
}

# concurrent-collections

Concurrent data structures for Java. The library has no dependencies and needs Java 17 or later.

## Queues

Package `io.github.merlimat.concurrent.queue`: lock-free, multi-producer single-consumer queues, which the consumer drains in batches.

- **`MpscUnboundedBatchedQueue`**: unbounded. Producers never block.
- **`MpscBoundedBatchedQueue`**: the same design with a soft capacity. Producers wait when the consumer falls behind.

Both implement `BatchedBlockingQueue`: a `BlockingQueue` plus `putAll`, `takeAll` and `pollAll`, with the same signatures as BookKeeper's `BatchedBlockingQueue`.

## Usage

Gradle:

```kotlin
implementation("io.github.merlimat:concurrent-collections:$version")
```

Maven:

```xml
<dependency>
  <groupId>io.github.merlimat</groupId>
  <artifactId>concurrent-collections</artifactId>
  <version>${version}</version>
</dependency>
```

Any number of threads insert. A single thread takes everything that's available in one call:

```java
BatchedBlockingQueue<Task> queue = new MpscBoundedBatchedQueue<>(10_000);

// Producers, from any thread
queue.put(task);

// The consumer thread
Task[] batch = new Task[1024];
while (running) {
    int count = queue.takeAll(batch);
    for (int i = 0; i < count; i++) {
        process(batch[i]);
        batch[i] = null;
    }
}
```

Only one thread at a time may take or inspect elements: `take`, `takeAll`, `poll`, `pollAll`, `peek`, `drainTo`, and the methods built on them, such as `remove()` and `clear()`. Iterating isn't supported, and neither are the collection methods built on iteration, such as `contains` and `toArray`.

## How it works

The design follows JCTools' `MpscUnboundedXaddArrayQueue`.

- **Claim, then publish.** A producer claims a slot with one atomic increment of the producer index, so contending producers never retry, then stores its element in the slot.
- **Chunks.** Slots live in fixed-size chunks linked together. The queue grows by appending a chunk, never by copying. The next chunk is appended as soon as a chunk's first slot is taken, so producers rarely allocate between claiming and publishing.
- **Batch drain.** The consumer reads the producer index to know how far it may go. It takes every element up to there in one pass, stopping early at a slot that is claimed but not published yet, and advances the consumer index once for the whole batch.
- **Waiting without yields.** When the consumer runs out of elements, it spins briefly, then records the slot it waits for and parks. Only the producer that publishes that slot unparks it, so producers pay for a wake-up only when the consumer actually waits for them. It never calls `Thread.yield()`: on macOS a yield can stall the thread for a whole 10 ms scheduler quantum.

### Bounded variant

`MpscBoundedBatchedQueue` adds admission control on top of the same structure:

- `offer(e)` fails when the queue holds `capacity` elements.
- `offer(e, timeout, unit)` waits for room before inserting, and inserts nothing if it times out or is interrupted.
- `put(e)` and `putAll` insert right away, then block while their element is beyond the capacity, until the consumer has taken enough of the elements before it. That keeps the producers' fast path a single atomic increment. An interrupt can never leave a claimed slot unpublished, which would stall the consumer: an interrupted `put` returns with its element inserted and the interrupt status set.

The capacity is soft: producers that race at the limit can each go over it by one element, or by one `putAll` batch. Producers compare against a cached limit, so they only read the consumer's index near the capacity. A producer that has to wait spins briefly, then blocks on a lock. Only waiting producers take that lock, plus the consumer when it has producers to wake up.

## Performance

Compared with the lock-based batched array queue it replaced in the Oxia Java client, with paced producers on Linux (a VM with 8 vCPUs):

- `put()` stays at 67–249 ns from 1 to 32 producers, while the lock queue's climbs to 11–23 µs with 32 producers.
- Median end-to-end latency drops by up to 17× with many producers, and with 32 producers the lock queue can't sustain 4M ops/s.

The bounded queue hasn't been measured yet. See [benchmark](benchmark/README.md) for the method, the full results and how to run it.

## Building

```bash
./gradlew build
```

## License

Apache License, Version 2.0. See [LICENSE](LICENSE).

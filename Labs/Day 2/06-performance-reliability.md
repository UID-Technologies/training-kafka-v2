# Lab 06 – Producer Performance, Reliability, Compression, and Backpressure

**Lab Number:** 06  
**Day:** 2  
**Duration:** 60 minutes  
**Difficulty:** Intermediate  
**Environment:** Windows 10 / Windows 11  
**Mode:** Individual

---

## Description

This lab maps to the producer, broker, and consumer configuration topics in the Day 2 syllabus: `acks`, `buffer.memory`, `compression.type`, `retries`, `batch.size`, `linger.ms`, `client.id`, `max.in.flight.requests.per.connection`, timeout-related settings, and consumer lag / backpressure.

You will add reliability and batching settings, experiment with compression, set a meaningful client id, then slow Inventory and watch lag grow and shrink.

---

## Prerequisites

- Labs 02–05 are complete
- `OrderProducer` and `InventoryConsumer` compile
- `order-events` exists
- The three-broker cluster is running
- You can describe consumer groups from the CLI

---

## Business Use Case

QuickCart’s Order API cannot lose checkouts, but it also cannot stay at one-record-per-send under peak load.

Platform and development agree on producer settings for reliability and throughput, and they need a way to **see** backpressure when Inventory is slow.

---

## Architecture

```text
OrderProducer
  acks=all
  retries
  batch.size + linger.ms
  compression
  client.id=quickcart-order-api
        |
        v
   order-events
        |
        v
InventoryConsumer
  Thread.sleep (lab only)
        |
        v
consumer group LAG
```

```mermaid
flowchart LR
    P[Producer settings] --> T[order-events]
    T --> C[Slow inventory]
    C --> L[LAG grows]
    L --> R[Remove sleep]
    R --> F[LAG falls]
```

---

## Detailed Steps

### Step 0 – Initial Setup

Stop extra Inventory workers from Lab 05. Keep **one** `InventoryConsumer` for the backpressure steps.

Compile:

```powershell
cd C:\kafka-labs\quickcart-kafka
mvn -q compile
```

Open `OrderProducer.java`. You will add configuration properties. Keep the JSON serializer and custom partitioner from Lab 04.

---

### Step 1 – Reliability configuration

Add:

```java
properties.put(
        ProducerConfig.ACKS_CONFIG,
        "all"
);

properties.put(
        ProducerConfig.RETRIES_CONFIG,
        3
);
```

Discuss:

```text
acks=0
Producer does not wait for broker acknowledgement

acks=1
Leader acknowledgement

acks=all
Acknowledgement based on required in-sync replicas
```

Day 1 used `min.insync.replicas` on the multi-broker configs. With `acks=all`, the leader waits for that ISR requirement.

Write which `acks` value you would use for payments versus click tracking:

| Event type | acks | Why |
| --- | --- | --- |
| Payment / order | | |
| Website click | | |

Run `OrderProducer` once to confirm it still publishes.

---

### Step 2 – Batching

Configure:

```java
properties.put(
        ProducerConfig.BATCH_SIZE_CONFIG,
        32768
);

properties.put(
        ProducerConfig.LINGER_MS_CONFIG,
        10
);
```

Explain:

```text
Without intentional waiting:

Event → Send
Event → Send
Event → Send


With batching opportunity:

Event ─┐
Event ─┼──> Batch → Kafka
Event ─┘
```

Discuss latency-versus-throughput trade-offs rather than treating larger batches as automatically better.

**Checkpoint**

Send a loop of 20+ orders. The program should still print callbacks. You will not always *see* a batch in the console; the setting still matters under load.

---

### Step 3 – Compression

Configure:

```java
properties.put(
        ProducerConfig.COMPRESSION_TYPE_CONFIG,
        "gzip"
);
```

Then experiment with:

```text
none
gzip
```

If supported by the selected client/broker environment, also discuss:

```text
snappy
lz4
zstd
```

Students should understand that compression can reduce network and storage use at the cost of CPU and latency.

Produce one batch with `none` and one with `gzip`. You may not see a size difference in this small lab. The point is to know the switch and the trade-off.

Record:

| compression.type | Did produce succeed? | Note |
| --- | --- | --- |
| none | | |
| gzip | | |

Leave `gzip` or `none` set for the rest of the day. Either is acceptable.

---

### Step 4 – Client identity

Add:

```java
properties.put(
        ProducerConfig.CLIENT_ID_CONFIG,
        "quickcart-order-api"
);
```

Meaningful client ids improve troubleshooting and metrics. Broker logs and monitoring tools can show `quickcart-order-api` instead of an anonymous producer.

Optional related settings from the syllabus — add them and leave a comment so you can explain them in the capstone:

```java
properties.put(
        ProducerConfig.BUFFER_MEMORY_CONFIG,
        33554432
);
properties.put(
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        5
);
```

---

### Step 5 – Backpressure simulation

Modify **one** Inventory consumer’s processing loop:

```java
Thread.sleep(2000);
```

Place it **after** you print the record so you can see work happening slowly.

Start that consumer. Then produce events quickly (20–50 orders in a tight loop).

Inspect:

```powershell
cd C:\kafka-labs\kafka

.\bin\windows\kafka-consumer-groups.bat `
  --bootstrap-server localhost:9092 `
  --describe `
  --group inventory-service
```

Use `inventory-service-json` if that is your group.

Observe:

```text
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

Lag should increase when the producer outpaces consumer processing.

Complete **while the sleep is still in place**:

| Partition | CURRENT-OFFSET | LOG-END-OFFSET | LAG |
| ---: | ---: | ---: | ---: |
| 0 | | | |
| 1 | | | |
| 2 | | | |
| 3 | | | |

---

### Step 6 – Remove the delay and watch catch-up

Remove or reduce the `Thread.sleep(2000)`.

Restart Inventory. Describe the group again after it has been running for 20–30 seconds.

Students should see the consumer catch up. This makes **backpressure / consumer lag** tangible.

```text
Producer rate > Consumer rate
          ↓
       Lag grows
          ↓
Consumer catches up
          ↓
       Lag falls
```

Record lag after recovery:

| Partition | LAG after catch-up |
| ---: | ---: |
| 0 | |
| 1 | |
| 2 | |
| 3 | |

---

## Conclusion

You configured a production-shaped producer: `acks=all`, retries, batching, compression, and `client.id=quickcart-order-api`.

You also forced Inventory to be slow and watched Kafka report lag. That is the operational signal Day 1 introduced and Day 2 now controls from Java.

**You are ready for Lab 07 when:**

- You can explain `acks=0/1/all`
- Batching and compression properties are in the producer
- You recorded lag growing and then falling

Next lab: [07-topic-partition-management.md](07-topic-partition-management.md)

---

## Knowledge Check

1. What does `acks=all` wait for?
2. What do `batch.size` and `linger.ms` trade?
3. Why set `client.id`?
4. What is consumer lag in one sentence?

**Expected answers**

1. Acknowledgement from the required in-sync replicas, not only the leader’s local write.
2. Throughput versus added send latency.
3. So operators can identify this producer in logs and metrics.
4. How far the group’s current offset is behind the log end offset on a partition.

---

## Common Issues

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| `acks=all` produce fails | Not enough ISR / RF 1 topic | Use the 3-broker `order-events` |
| Lag stays 0 with sleep | Producer sent too few records | Send 20+ quickly |
| Wrong group in `--describe` | Lab 04 used a different group name | Describe the group in your consumer config |
| Sleep blocks the whole group | You have only one consumer | That is intended for this demo |

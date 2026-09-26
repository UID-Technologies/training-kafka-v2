# Lab 04 – Kafka Performance Tuning Workshop

**Lab Number:** 04  
**Day:** 3  
**Duration:** 35 minutes  
**Difficulty:** Intermediate  
**Environment:** Windows 10 / Windows 11  
**Mode:** Individual

---

## Description

The Day 3 syllabus calls for tuning brokers, OS, producers, consumers, cross-data-center communication, and best practices.

On a Windows developer laptop, hands-on work is **producer and consumer behavior**. OS and cross-DC tuning stay at architecture discussion.

You will produce 10,000 events as a baseline, change `batch.size` and `linger.ms`, compare `compression.type`, discuss `acks=1` versus `acks=all`, and review consumer fetch/poll settings.

---

## Prerequisites

- Lab 03 is complete and the cluster is healthy
- A working Java producer (`AvroOrderProducer` or a dedicated tuner)
- `order-events` or `avro-order-events` exists
- A stopwatch or `System.currentTimeMillis()` in code

---

## Business Use Case

QuickCart’s Order API must survive a flash sale. Leadership asked: “How fast can we publish, and what do we give up for durability?”

You will measure, not guess.

---

## Architecture

```text
TuningTestProducer
  10,000 events
  vary batch / linger / compression / acks
        |
        v
   Kafka topic
        |
        v
Consumer (discussion)
  fetch, poll, processing time
  partition count
  number of consumers
```

```mermaid
flowchart LR
    B[Baseline] --> T[Tune batch + linger]
    T --> C[Compression]
    C --> A[acks reliability]
    A --> R[Record throughput]
```

---

## Detailed Steps

### Step 0 – Initial Setup

Confirm brokers are up and Lab 03 sleep/bootstrap mistakes are gone.

Create `src\main\java\com\quickcart\kafka\producer\TuningTestProducer.java`. You may copy `AvroOrderProducer` and loop, or send simple strings to `order-events` to keep the test cheap.

String version is enough for this workshop:

```java
package com.quickcart.kafka.producer;

import com.quickcart.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TuningTestProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "quickcart-tuning");

        int count = 10000;
        long start = System.currentTimeMillis();

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(props)) {
            for (int i = 1; i <= count; i++) {
                producer.send(new ProducerRecord<>(
                        "order-events",
                        "TUNE" + i,
                        "TUNE" + i + ",C1,ITEM,1,100"
                ));
            }
            producer.flush();
        }

        long duration = System.currentTimeMillis() - start;
        double perSec = count * 1000.0 / Math.max(duration, 1);
        System.out.println("Start/end duration ms = " + duration);
        System.out.println("Messages/sec = " + perSec);
    }
}
```

If `order-events` is missing, create it with 4 partitions and RF 3, or change the topic to `avro-order-events` only if you send compatible values.

---

### Step 1 – Baseline producer

Produce:

```text
10,000 events
```

Capture:

```text
Start time
End time
Duration
Messages/sec
```

The program prints duration and messages/sec. Record **Test 1 – Baseline**:

| Test | batch.size | linger.ms | compression | acks | Time (ms) | Throughput (msg/s) |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| Baseline | 16384 | 0 | none | 1 | | |

---

### Step 2 – Tune batching

Experiment with:

```java
batch.size
linger.ms
```

Example:

```java
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
```

Run again.

Record:

| Test | batch.size | linger.ms | Time | Throughput |
| --- | ---: | ---: | ---: | ---: |
| Baseline | | | | |
| Test 2 | 32768 | 10 | | |

Discuss latency-versus-throughput. Larger batches are not automatically better for a checkout API that needs a fast ack.

---

### Step 3 – Compression

Test:

```text
compression.type=none
```

versus:

```text
compression.type=gzip
```

```java
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
```

Keep Test 2 batch settings so you change one idea at a time.

Discuss:

```text
Compression
    ↓
Less network/storage
    ↓
More CPU work
```

| Test | compression.type | Time | Throughput |
| --- | --- | ---: | ---: |
| none | none | | |
| gzip | gzip | | |

On a laptop with small messages, gzip may be slower. That is a valid result. Write what you observed.

If the environment supports them, the trainer may also mention `snappy`, `lz4`, and `zstd`.

---

### Step 4 – Reliability

Compare conceptually (and optionally with a timed run):

```text
acks=1
```

and:

```text
acks=all
```

```java
props.put(ProducerConfig.ACKS_CONFIG, "all");
```

Discuss reliability versus latency. `acks=all` waits for the required in-sync replicas. That is the right default for orders and payments, and it usually costs some throughput.

| acks | Time | Throughput | When you would use it |
| --- | ---: | ---: | --- |
| 1 | | | |
| all | | | |

---

### Step 5 – Consumer tuning discussion

Cover, with the trainer:

```text
fetch settings
poll behavior
processing time
partition count
number of consumers
```

Connect back to Day 2 consumer lag and Lab 03 Scenario 3.

Write one sentence:

```text
If Inventory processing is 5 seconds per record, tuning linger.ms will / will not
fix lag because: ________________________________________________
```

---

### Step 6 – Architecture-level OS and cross-DC notes

Hands-on OS and cross-data-center tuning is not done on every Windows laptop. Discuss:

- disk and `log.dirs` placement
- enough file handles / page cache
- `acks=all` plus `min.insync.replicas` across racks
- MirrorMaker / cluster linking for another DC (Day 2 instructor extension)

Do not change OS settings in this lab.

---

## Conclusion

You measured a 10,000-message baseline and changed batching, compression, and `acks` with numbers on the page.

Tuning is a trade-off among durability, latency, CPU, and throughput. Lab 05 moves from performance to **who is allowed** to produce or consume.

**You are ready for Lab 05 when:**

- The baseline table is filled
- Test 2 has batch/linger numbers
- You can explain `acks=1` vs `acks=all`

Next lab: [05-acl-security.md](05-acl-security.md)

---

## Knowledge Check

1. What do `linger.ms` and `batch.size` try to do?
2. Why might gzip reduce messages/sec on this lab machine?
3. Which `acks` value matches Day 1’s RF 3 payment topic?
4. Does producer tuning fix a 5-second `Thread.sleep` in the consumer?

**Expected answers**

1. Group more records per request.
2. CPU cost can exceed network savings for tiny local messages.
3. `acks=all` (with enough ISR).
4. No. That is a processing bottleneck.

---

## Common Issues

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| Topic missing | Day 2 topic never created | Create `order-events` RF 3 |
| `acks=all` fails | RF 1 topic | Recreate with RF 3 |
| Throughput looks random | Machine busy | Run each test twice and record both |

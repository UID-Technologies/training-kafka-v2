# Apache Kafka -- 32-Hour Training Guide

This repository is the learner-ready delivery pack for the supplied
**Apache Kafka -- 32 Hours** curriculum. It covers fundamentals,
architecture, cluster management, Java producer/consumer development,
topic and partition management, security, Kafka Connect, Kafka
Streams/ksqlDB, Schema Registry, and Confluent Control Center.

The running business story is **QuickCart**, an e-commerce event
platform. Day 1 builds the cluster. Day 2 builds Java services. Day 3
adds Avro, operations, security, and Connect. Day 4 adds stream
processing and schema governance.

> **Duration:** 32 Hours\
> **Format:** 4 Days × 8 Hours\
> **Approach:** Concepts + configuration + CLI + Java + hands-on labs\
> **Environment:** Windows 10/11, JDK 17+, Maven, Apache Kafka 3.8.x or
> 3.9.x (ZooKeeper mode)

**Start here for labs:** open the Day index, then complete each numbered
file in order. Do not skip **Step 0**.

| Day | Start file | Focus |
| ---: | --- | --- |
| 1 | [Labs/Day 1/00-initial.md](Labs/Day%201/00-initial.md) | Cluster, CLI, replication, consumer groups |
| 2 | [Labs/Day 2/00-initial.md](Labs/Day%202/00-initial.md) | Java producers, consumers, topics |
| 3 | [Labs/Day 3/00-initial.md](Labs/Day%203/00-initial.md) | Avro, admin, ACL, SSL, Connect |
| 4 | [Labs/Day 4/00-initial.md](Labs/Day%204/00-initial.md) | Streams, ksqlDB, Schema Registry, capstone |

Every lab file uses the same learner structure: title, lab number,
description, prerequisites, business use case, architecture diagram,
detailed steps (including Step 0), checkpoints, conclusion, knowledge
check, and common issues.

------------------------------------------------------------------------

## Table of Contents

1.  [How to use this repository](#how-to-use-this-repository)
2.  [Hands-on lab catalog](#hands-on-lab-catalog)
3.  [Day 1 -- Kafka Fundamentals, Architecture & Cluster
    Management](#day-1--kafka-fundamentals-architecture--cluster-management)
4.  [Day 2 -- Producer/Consumer APIs & Topic
    Management](#day-2--producerconsumer-apis--topic-management)
5.  [Day 3 -- Security & Kafka Connect](#day-3--security--kafka-connect)
6.  [Day 4 -- Kafka Streams, ksqlDB, Schema Registry & Control
    Center](#day-4--kafka-streams-ksqldb-schema-registry--control-center)
7.  [Hands-on exercise checklist](#hands-on-exercise-checklist)
8.  [Learning outcomes](#learning-outcomes)
9.  [Repository structure](#repository-structure)

------------------------------------------------------------------------

# How to use this repository

1.  Read the Day index (`00-initial.md`) once at the start of that day.
2.  Start the Day 1 three-broker cluster before Days 2–4.
3.  Complete lab files in numeric order. Later labs reuse earlier
    topics, Java code, and ports.
4.  Record your own observation tables. Sample CLI output will differ.
5.  Use each lab’s **Common Issues** section before asking for help.

Standard lab paths and ports (see each Day index for the full map):

| Item | Value |
| --- | --- |
| Kafka home | `C:\kafka-labs\kafka` |
| Java project | `C:\kafka-labs\quickcart-kafka` |
| ZooKeeper | `localhost:2181` |
| Brokers (PLAINTEXT) | `9092`, `9093`, `9094` |
| SASL listener (Day 3 Lab 05) | `9095` on Broker 1 |
| SSL listeners (Day 3 Lab 06) | `9192`, `9193`, `9194` |
| Schema Registry (Day 4) | `http://localhost:8081` (or trainer URL) |
| ksqlDB REST (Day 4) | `http://localhost:8088` (or trainer URL) |
| Control Center (Day 4) | `http://localhost:9021` (or trainer URL) |

------------------------------------------------------------------------

# Hands-on lab catalog

## Day 1 -- 11 labs

Index: [Labs/Day 1/00-initial.md](Labs/Day%201/00-initial.md)

| Lab | File | Exercise |
| ---: | --- | --- |
| 01 | [01-environment-setup.md](Labs/Day%201/01-environment-setup.md) | Workstation and Kafka installation |
| 02 | [02-single-broker.md](Labs/Day%201/02-single-broker.md) | Single node, single broker |
| 03 | [03-produce-consume.md](Labs/Day%201/03-produce-consume.md) | First topic, produce, consume |
| 04 | [04-kafka-cli.md](Labs/Day%201/04-kafka-cli.md) | Explore Kafka CLI commands |
| 05 | [05-partitions-offsets.md](Labs/Day%201/05-partitions-offsets.md) | Partitions, keys, and offsets |
| 06 | [06-zookeeper.md](Labs/Day%201/06-zookeeper.md) | Explore ZooKeeper |
| 07 | [07-multi-broker.md](Labs/Day%201/07-multi-broker.md) | Single node, multiple brokers |
| 08 | [08-replication-isr.md](Labs/Day%201/08-replication-isr.md) | Replication, leader, follower, ISR |
| 09 | [09-consumer-groups.md](Labs/Day%201/09-consumer-groups.md) | Consumer groups, lag, rebalancing |
| 10 | [10-multi-node-cluster.md](Labs/Day%201/10-multi-node-cluster.md) | Multi-node cluster design |
| 11 | [11-capstone-payment-processing.md](Labs/Day%201/11-capstone-payment-processing.md) | Day 1 capstone: payment events |

## Day 2 -- 8 labs

Index: [Labs/Day 2/00-initial.md](Labs/Day%202/00-initial.md)

Docker, disaster recovery, and MirrorMaker are instructor extensions on
the Day 2 index, not separate learner files.

| Lab | File | Exercise |
| ---: | --- | --- |
| 01 | [01-java-project-setup.md](Labs/Day%202/01-java-project-setup.md) | Java Kafka project setup |
| 02 | [02-java-producer.md](Labs/Day%202/02-java-producer.md) | Java producer (sync/async) |
| 03 | [03-java-consumer.md](Labs/Day%202/03-java-consumer.md) | Java consumer and offsets |
| 04 | [04-serializer-partitioner.md](Labs/Day%202/04-serializer-partitioner.md) | Custom serializer and partitioner |
| 05 | [05-consumer-groups-scaling.md](Labs/Day%202/05-consumer-groups-scaling.md) | Consumer groups, scaling, threads |
| 06 | [06-performance-reliability.md](Labs/Day%202/06-performance-reliability.md) | Batching, compression, backpressure |
| 07 | [07-topic-partition-management.md](Labs/Day%202/07-topic-partition-management.md) | Compaction, retention, log dirs, reassignment |
| 08 | [08-capstone-order-processing.md](Labs/Day%202/08-capstone-order-processing.md) | Day 2 developer capstone |

## Day 3 -- 10 labs

Index: [Labs/Day 3/00-initial.md](Labs/Day%203/00-initial.md)

Full Schema Registry is Day 4. Day 3 Lab 01 teaches Avro encoding only.

| Lab | File | Exercise |
| ---: | --- | --- |
| 01 | [01-avro-producer-consumer.md](Labs/Day%203/01-avro-producer-consumer.md) | Avro producer and consumer |
| 02 | [02-administration-toolkit.md](Labs/Day%203/02-administration-toolkit.md) | Administration toolkit |
| 03 | [03-troubleshooting.md](Labs/Day%203/03-troubleshooting.md) | Troubleshoot production problems |
| 04 | [04-performance-tuning.md](Labs/Day%203/04-performance-tuning.md) | Performance tuning workshop |
| 05 | [05-acl-security.md](Labs/Day%203/05-acl-security.md) | Authorization and ACL |
| 06 | [06-ssl-encryption.md](Labs/Day%203/06-ssl-encryption.md) | SSL/TLS encryption |
| 07 | [07-kafka-connect-file.md](Labs/Day%203/07-kafka-connect-file.md) | Kafka Connect file pipeline |
| 08 | [08-jdbc-pipeline.md](Labs/Day%203/08-jdbc-pipeline.md) | JDBC data pipeline |
| 09 | [09-cassandra-elasticsearch.md](Labs/Day%203/09-cassandra-elasticsearch.md) | Cassandra and Elasticsearch patterns |
| 10 | [10-capstone-secure-integration.md](Labs/Day%203/10-capstone-secure-integration.md) | Day 3 secure integration capstone |

## Day 4 -- 10 labs

Index: [Labs/Day 4/00-initial.md](Labs/Day%204/00-initial.md)

Labs 05–09 need the trainer Confluent/Docker stack (ksqlDB, Schema
Registry, Control Center).

| Lab | File | Exercise |
| ---: | --- | --- |
| 01 | [01-stream-processing-fundamentals.md](Labs/Day%204/01-stream-processing-fundamentals.md) | Stream processing fundamentals |
| 02 | [02-first-kafka-streams-app.md](Labs/Day%204/02-first-kafka-streams-app.md) | First Kafka Streams application |
| 03 | [03-filter-map-aggregate.md](Labs/Day%204/03-filter-map-aggregate.md) | Filter, map, aggregate |
| 04 | [04-kstream-ktable-join.md](Labs/Day%204/04-kstream-ktable-join.md) | KStream + KTable join |
| 05 | [05-ksqldb-streams-tables.md](Labs/Day%204/05-ksqldb-streams-tables.md) | ksqlDB streams and tables |
| 06 | [06-ksqldb-joins-rest.md](Labs/Day%204/06-ksqldb-joins-rest.md) | ksqlDB joins, REST, terminate |
| 07 | [07-schema-registry-avro.md](Labs/Day%204/07-schema-registry-avro.md) | Schema Registry and Avro |
| 08 | [08-schema-evolution.md](Labs/Day%204/08-schema-evolution.md) | Schema evolution and compatibility |
| 09 | [09-control-center.md](Labs/Day%204/09-control-center.md) | Confluent Control Center |
| 10 | [10-capstone-order-intelligence.md](Labs/Day%204/10-capstone-order-intelligence.md) | 32-hour course capstone |

------------------------------------------------------------------------

# Day 1 -- Kafka Fundamentals, Architecture & Cluster Management

**Labs:** [Day 1 index](Labs/Day%201/00-initial.md)

## Module 1: Apache Kafka Fundamentals -- 3 Hours

### 1. Messaging Systems

Understand why distributed applications use messaging systems to
decouple producers and consumers.

Topics: Introduction to messaging systems; messaging use cases;
Point-to-Point (P2P); Publish/Subscribe (Pub/Sub); limitations of
traditional approaches; JMS; RabbitMQ; ActiveMQ; Kafka's role in modern
messaging architectures.

### 2. Publish/Subscribe and Big Data

Understand how publish/subscribe systems allow multiple consumers to
independently consume the same stream of events.

Key areas: Publishers; subscribers; topics; event streams; decoupling
applications; Kafka in the big-data ecosystem.

### 3. Kafka for Big Data and Data Ingestion

Kafka is commonly used as an event backbone between applications,
databases, analytics platforms, and data-processing systems.

Topics: Data ingestion; event streaming; Kafka in ETL pipelines;
application integration; streaming data between systems.

### 4. Why Kafka?

Key concepts: High-throughput messaging; distributed architecture;
scalability; durability; fault tolerance; replayable events; horizontal
scaling.

### 5. Kafka Components

Cluster; broker; topic; partition; producer; consumer; consumer group;
offset; replication.

### 6. Kafka Features

Distributed architecture; persistent event storage; partition-based
scalability; replication; fault tolerance; high throughput; consumer
groups; event replay.

### 7. Kafka Use Cases

Application integration; log aggregation; data ingestion; event-driven
systems; ETL pipelines; analytics pipelines; change/event distribution.

### 8. Installing Kafka

Prepare a Kafka environment and understand the basic configuration
required to start Kafka.

**Labs:** [01 environment setup](Labs/Day%201/01-environment-setup.md),
[02 single broker](Labs/Day%201/02-single-broker.md),
[03 produce/consume](Labs/Day%201/03-produce-consume.md)

------------------------------------------------------------------------

## Module 2: Apache Kafka Architecture -- 3 Hours

### Kafka Architecture

Study how producers, brokers, topics, partitions, replicas, and
consumers work together.

``` text
Producer
   |
   v
+---------------- Kafka Cluster ----------------+
|                                                |
| Broker 1        Broker 2        Broker 3       |
| Topic-A P0      Topic-A P1      Topic-A P2     |
| Leader/Replica  Leader/Replica  Leader/Replica |
+---------------------+--------------------------+
                      |
                      v
               Consumer Group
```

### Cluster Components and Design

Kafka cluster design; broker responsibilities; topic distribution;
partition placement; replication; consumer scalability.

### Role of ZooKeeper

The supplied curriculum includes ZooKeeper-based Kafka administration
and cluster exercises: coordination; cluster metadata; broker
registration; controller-related coordination; ZooKeeper cluster.

> For this course, ZooKeeper is required because the training labs run
> Kafka in ZooKeeper mode.

### Broker, Topics, Partitions, Producer, Consumer

A broker stores partitions and serves client requests. A topic is a
logical stream of events. Partitions support parallelism, scalability,
and per-partition order.

### Leader, Follower, Replication, Offsets, ISR, Consumer Groups

One replica is leader; others are followers. Replication improves
availability. An offset is a record's position in a partition. ISR is
the in-sync replica set. Consumer groups share partitions.

**Labs:** [04 CLI](Labs/Day%201/04-kafka-cli.md),
[05 partitions and offsets](Labs/Day%201/05-partitions-offsets.md),
[06 ZooKeeper](Labs/Day%201/06-zookeeper.md),
[09 consumer groups](Labs/Day%201/09-consumer-groups.md)

------------------------------------------------------------------------

## Module 3: Managing Cluster -- 2 Hours

### Kafka Cluster and ZooKeeper Cluster

Understand how multiple brokers form a cluster and how this course
configures ZooKeeper.

### Single Node -- Multiple Broker Cluster

Configure three Kafka broker processes on one Windows machine (ports
9092, 9093, 9094).

### Multiple Nodes -- Multiple Brokers

Design a three-machine layout (ZooKeeper ensemble, `advertised.listeners`).
Hands-on on extra VMs is optional.

**Labs:** [07 multi-broker](Labs/Day%201/07-multi-broker.md),
[08 replication and ISR](Labs/Day%201/08-replication-isr.md),
[10 multi-node design](Labs/Day%201/10-multi-node-cluster.md),
[11 capstone](Labs/Day%201/11-capstone-payment-processing.md)

------------------------------------------------------------------------

# Day 2 -- Producer/Consumer APIs & Topic Management

**Labs:** [Day 2 index](Labs/Day%202/00-initial.md)

## Module 4: .NET/Java Producer and Consumer API -- 4 Hours

This delivery uses **Java 17 + Maven**. The syllabus also names .NET;
the same Kafka client concepts apply.

### Kafka Broker API, Producer and Consumer APIs

How clients communicate with brokers; configuration; keys and values;
sending records; subscription; poll; offsets; lifecycle.

### Synchronous and Asynchronous Producers

Compare `Future.get()` with completion callbacks.

### Custom Partitioner and Serializer

Route high-value orders; serialize `Order` objects to JSON.

### Single Consumer, Scaling, Multithreaded Consumers, Rebalancing

One consumer per process or thread. `KafkaConsumer` is not shared across
threads. Extra consumers beyond the partition count sit idle.

**Labs:** [01 project setup](Labs/Day%202/01-java-project-setup.md),
[02 producer](Labs/Day%202/02-java-producer.md),
[03 consumer](Labs/Day%202/03-java-consumer.md),
[04 serializer and partitioner](Labs/Day%202/04-serializer-partitioner.md),
[05 groups and scaling](Labs/Day%202/05-consumer-groups-scaling.md)

------------------------------------------------------------------------

## Module 5: Topic and Partition Management -- 4 Hours

### Compacted Topics, Compression, Retention

Latest value per key; `compression.type`; `retention.ms` vs compaction.

### Important Configurations

Producer: `acks`, `buffer.memory`, `compression.type`, `retries`,
`batch.size`, `linger.ms`, `client.id`,
`max.in.flight.requests.per.connection`, timeout-related settings.

Topic/broker: `num.partitions`, `log.retention.ms`,
`log.retention.bytes`, `log.segment.bytes`, `log.segment.ms`,
`message.max.bytes`.

### Partition Management, Record and Topic Deletion, Log Dirs, Reassignment

Increase partitions; delete records vs delete topic; inspect log
directories; reassign replicas.

### Disaster Recovery and MirrorMaker

Instructor discussion on the Day 2 index (not a standalone learner lab).

### Backpressure and Consumer Lag

Lag is log-end offset minus current offset. Day 2 Lab 06 makes this
visible with an artificial consumer delay.

**Labs:** [06 performance](Labs/Day%202/06-performance-reliability.md),
[07 topic management](Labs/Day%202/07-topic-partition-management.md),
[08 capstone](Labs/Day%202/08-capstone-order-processing.md)

------------------------------------------------------------------------

# Day 3 -- Security & Kafka Connect

**Labs:** [Day 3 index](Labs/Day%203/00-initial.md)

## Module 6: Security -- 4 Hours

### Avro Producer/Consumer

Introduce Avro mechanics (schema + binary encode/decode). Schema
Registry is Day 4.

### Kafka Administration Tools, Frequent Problems, Performance Tuning

CLI toolkit; deliberate break/fix scenarios; 10,000-event producer
workshop. Kafka Manager / CMAK is optional if the trainer provides a UI.

### Authorization, ACL, SSL

Authentication vs authorization. SASL/PLAIN on port 9095 for named
principals. ACLs on dedicated topics so PLAINTEXT 9092 stays usable for
Connect. TLS on ports 9192–9194 (not 9093).

**Labs:** [01 Avro](Labs/Day%203/01-avro-producer-consumer.md),
[02 admin](Labs/Day%203/02-administration-toolkit.md),
[03 troubleshooting](Labs/Day%203/03-troubleshooting.md),
[04 tuning](Labs/Day%203/04-performance-tuning.md),
[05 ACL](Labs/Day%203/05-acl-security.md),
[06 SSL](Labs/Day%203/06-ssl-encryption.md)

------------------------------------------------------------------------

## Module 7: Data Pipelines with Kafka Connect -- 4 Hours

### Kafka Connect Overview and Architecture

Workers, connectors, tasks, source vs sink, standalone vs distributed.

``` text
Source System
     |
     v
Source Connector
     |
     v
Kafka Topics
     |
     v
Sink Connector
     |
     v
Target System
```

### File, JDBC, Cassandra, Elasticsearch

FileStream source/sink on Windows standalone Connect. JDBC source
against a lightweight database (H2 unless the trainer specifies
another). Cassandra and Elasticsearch are trainer-hosted pattern labs;
students still write connector configs.

**Labs:** [07 file Connect](Labs/Day%203/07-kafka-connect-file.md),
[08 JDBC](Labs/Day%203/08-jdbc-pipeline.md),
[09 Cassandra/ES](Labs/Day%203/09-cassandra-elasticsearch.md),
[10 capstone](Labs/Day%203/10-capstone-secure-integration.md)

------------------------------------------------------------------------

# Day 4 -- Kafka Streams, ksqlDB, Schema Registry & Control Center

**Labs:** [Day 4 index](Labs/Day%204/00-initial.md)

## Module 8: Kafka Stream Processing and KSQL Server -- 4 Hours

### Stream Processing Concepts and Framework Selection

Batch vs stream. Kafka Streams is a **Java library**. ksqlDB is a
**server**. Choose by requirement (SQL analytics vs Java domain logic).

### KStream and KTable

KStream = “What happened?” KTable = “What is the current state?”

### Kafka Streams Applications

Filter high-value orders (`amount >= 50000`); map/selectKey; count per
customer; join orders to a compacted customer table.

### ksqlDB

Create streams and tables; live `SELECT ... EMIT CHANGES`; persistent
CSAS/CTAS; stream-table join; `SHOW QUERIES` / `EXPLAIN` / `TERMINATE`;
REST (`POST /ksql`).

**Labs:** [01 fundamentals](Labs/Day%204/01-stream-processing-fundamentals.md),
[02 first Streams app](Labs/Day%204/02-first-kafka-streams-app.md),
[03 filter/map/aggregate](Labs/Day%204/03-filter-map-aggregate.md),
[04 KStream-KTable join](Labs/Day%204/04-kstream-ktable-join.md),
[05 ksqlDB streams](Labs/Day%204/05-ksqldb-streams-tables.md),
[06 ksqlDB joins and REST](Labs/Day%204/06-ksqldb-joins-rest.md)

------------------------------------------------------------------------

## Module 9: Schema Registry and Confluent Control Center -- 4 Hours

### Schema Registry

Schemas as a first-class contract. Subjects, versions, schema ids.
Register, list subjects, retrieve versions. Avro producer/consumer using
registry-aware serializers.

### Compatibility and Evolution

Add `deliveryCity` as an optional field (typically BACKWARD). Observe
incompatible registration rejected. Add/delete a **disposable** subject
only.

### UI / Confluent Control Center

Guided visual lab: cluster, topics, brokers, groups, Connect, ksqlDB,
schemas. The UI does not replace CLI skills from Days 1–3.

**Labs:** [07 Schema Registry](Labs/Day%204/07-schema-registry-avro.md),
[08 evolution](Labs/Day%204/08-schema-evolution.md),
[09 Control Center](Labs/Day%204/09-control-center.md),
[10 course capstone](Labs/Day%204/10-capstone-order-intelligence.md)

------------------------------------------------------------------------

# Hands-on exercise checklist

Use this checklist during the 32-hour program. Each item maps to a lab
file.

### Day 1

-   [ ] Workstation and Kafka install -- Day 1 Lab 01
-   [ ] Single node, single broker -- Day 1 Lab 02
-   [ ] Produce and consume -- Day 1 Lab 03
-   [ ] Explore Kafka CLI -- Day 1 Lab 04
-   [ ] Partitions, keys, offsets -- Day 1 Lab 05
-   [ ] Explore ZooKeeper -- Day 1 Lab 06
-   [ ] Single node, multiple brokers -- Day 1 Lab 07
-   [ ] Replication, leader, ISR -- Day 1 Lab 08
-   [ ] Consumer groups and rebalancing -- Day 1 Lab 09
-   [ ] Multi-node cluster design -- Day 1 Lab 10
-   [ ] Day 1 capstone (payments) -- Day 1 Lab 11

### Day 2

-   [ ] Java project setup -- Day 2 Lab 01
-   [ ] Text/Java producer (sync/async) -- Day 2 Lab 02
-   [ ] Java consumer -- Day 2 Lab 03
-   [ ] Custom serializer -- Day 2 Lab 04
-   [ ] Custom partitioner -- Day 2 Lab 04
-   [ ] Consumer group, scaling, rebalance -- Day 2 Lab 05
-   [ ] Performance, compression, lag -- Day 2 Lab 06
-   [ ] Compacted topics, log dirs, reassignment -- Day 2 Lab 07
-   [ ] Docker demo (instructor) -- [Day 2 index](Labs/Day%202/00-initial.md)
-   [ ] DR / MirrorMaker (instructor) -- [Day 2 index](Labs/Day%202/00-initial.md)
-   [ ] Day 2 capstone -- Day 2 Lab 08

### Day 3

-   [ ] Avro producer/consumer -- Day 3 Lab 01
-   [ ] Administration toolkit -- Day 3 Lab 02
-   [ ] Troubleshooting -- Day 3 Lab 03
-   [ ] Performance tuning -- Day 3 Lab 04
-   [ ] ACL configuration -- Day 3 Lab 05
-   [ ] SSL configuration -- Day 3 Lab 06
-   [ ] Kafka Connect file pipeline -- Day 3 Lab 07
-   [ ] JDBC connector -- Day 3 Lab 08
-   [ ] Cassandra / Elasticsearch connectors -- Day 3 Lab 09
-   [ ] Day 3 capstone -- Day 3 Lab 10

### Day 4

-   [ ] Stream concepts and framework choice -- Day 4 Lab 01
-   [ ] Kafka Streams application -- Day 4 Lab 02
-   [ ] Filter, map, aggregate -- Day 4 Lab 03
-   [ ] KStream / KTable join -- Day 4 Lab 04
-   [ ] ksqlDB streams/tables/queries -- Day 4 Lab 05
-   [ ] ksqlDB joins, REST, terminate -- Day 4 Lab 06
-   [ ] Schema Registry operations and Avro clients -- Day 4 Lab 07
-   [ ] Schema evolution and compatibility -- Day 4 Lab 08
-   [ ] Confluent Control Center -- Day 4 Lab 09
-   [ ] Course capstone -- Day 4 Lab 10

------------------------------------------------------------------------

# Learning outcomes

After completing the program, participants should be able to:

1.  Explain messaging systems, P2P, Pub/Sub, and Kafka's role in
    data-ingestion and event-driven architectures.
2.  Explain Kafka brokers, topics, partitions, producers, consumers,
    offsets, replication, ISR, leaders/followers, and consumer groups.
3.  Configure the single-broker and three-broker Windows environments
    used by this training.
4.  Use Kafka command-line tools for topic, consumer, storage, and
    cluster operations.
5.  Develop Kafka producers and consumers using the Java client API
    covered by the course.
6.  Work with synchronous/asynchronous producers, serializers,
    partitioners, consumer groups, scaling, and rebalancing.
7.  Configure topic retention, compaction, compression, partitions, and
    important producer/consumer/broker properties.
8.  Understand consumer lag, backpressure, DR, MirrorMaker, log
    directories, and partition reassignment.
9.  Apply Kafka authorization through ACLs and understand SSL-based wire
    encryption.
10. Build data pipelines using Kafka Connect and the
    file/JDBC/Cassandra/Elasticsearch connectors covered by the
    curriculum.
11. Explain Kafka Streams, KStream, KTable, and stream-processing
    concepts.
12. Create and operate streams, tables, queries, and joins using the
    ksqlDB environment covered by the course.
13. Explain Schema Registry, Avro schemas, subjects, schema operations,
    and compatibility.
14. Use Confluent Control Center/UI concepts included in the course.

------------------------------------------------------------------------

# Repository structure

``` text
training-kafka-v2/
|
+-- README.md                 This course guide and lab catalog
|
+-- Labs/
    +-- Day 1/
    |   +-- 00-initial.md
    |   +-- 01-environment-setup.md
    |   +-- 02-single-broker.md
    |   +-- 03-produce-consume.md
    |   +-- 04-kafka-cli.md
    |   +-- 05-partitions-offsets.md
    |   +-- 06-zookeeper.md
    |   +-- 07-multi-broker.md
    |   +-- 08-replication-isr.md
    |   +-- 09-consumer-groups.md
    |   +-- 10-multi-node-cluster.md
    |   +-- 11-capstone-payment-processing.md
    |
    +-- Day 2/
    |   +-- 00-initial.md
    |   +-- 01-java-project-setup.md
    |   +-- ...
    |   +-- 08-capstone-order-processing.md
    |
    +-- Day 3/
    |   +-- 00-initial.md
    |   +-- 01-avro-producer-consumer.md
    |   +-- ...
    |   +-- 10-capstone-secure-integration.md
    |
    +-- Day 4/
        +-- 00-initial.md
        +-- 01-stream-processing-fundamentals.md
        +-- ...
        +-- 10-capstone-order-intelligence.md
```

Learners start each day at `Labs/Day N/00-initial.md`.

------------------------------------------------------------------------

## Notes

This README follows the supplied 32-hour training curriculum. It keeps
ZooKeeper, Kafka Manager (optional UI), KSQL Server/ksqlDB, Windows
exercises, and the named connectors because they are explicitly present
in the training plan.

Hands-on work lives in `Labs/`. Concept pages in this README map to
those files. Do not use leftover single-file lab dumps; each exercise is
its own learner-ready document.

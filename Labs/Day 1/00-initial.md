# Apache Kafka – Day 1 Hands-on Lab Guide

**Theme:** Kafka Fundamentals, Architecture, and Cluster Management  
**Duration:** 8 hours  
**Environment:** Windows 10 / Windows 11  
**Mode:** Individual (pairs optional for the capstone)  
**Business story:** QuickCart event-streaming platform

Start here. Complete the labs in order. Each exercise is a separate learner-ready file.

---

## How to Use This Guide

1. Read this page once at the start of Day 1.
2. Open the next lab file only when the previous lab is complete.
3. Keep terminals running unless a step tells you to stop a process.
4. Fill in every observation table. Those records are used later in the day.
5. Do not skip **Step 0**. It confirms the environment before you change anything.

If a command fails, use the **Common Issues** section at the end of that lab before asking for help.

---

## What You Will Be Able to Do

By the end of Day 1 you will be able to:

- Install and start Apache Kafka in ZooKeeper mode on Windows
- Run a single-broker cluster and a three-broker cluster on one machine
- Create topics, produce events, and consume events with the Kafka CLI
- Explain partitions, offsets, keys, leaders, followers, replicas, and ISR
- Inspect ZooKeeper metadata used by this training cluster
- Scale consumers with consumer groups and observe rebalancing and lag
- Recover from a broker failure using replication

---

## Lab Roadmap

| Lab | File | Exercise | Main topics | Time |
| ---: | --- | --- | --- | ---: |
| 01 | [01-environment-setup.md](01-environment-setup.md) | Workstation and Kafka installation | Java, folders, Kafka layout | 30 min |
| 02 | [02-single-broker.md](02-single-broker.md) | Single node, single broker | ZooKeeper, one broker, first cluster | 40 min |
| 03 | [03-produce-consume.md](03-produce-consume.md) | First topic, produce, consume | Topics, producers, consumers | 35 min |
| 04 | [04-kafka-cli.md](04-kafka-cli.md) | Explore Kafka CLI commands | Topics, configs, inspection | 35 min |
| 05 | [05-partitions-offsets.md](05-partitions-offsets.md) | Partitions, keys, and offsets | Parallelism, ordering, offsets | 40 min |
| 06 | [06-zookeeper.md](06-zookeeper.md) | Explore ZooKeeper | Coordination, broker/topic metadata | 30 min |
| 07 | [07-multi-broker.md](07-multi-broker.md) | Single node, multiple brokers | 3 brokers, ports, log dirs | 45 min |
| 08 | [08-replication-isr.md](08-replication-isr.md) | Replication, leader, follower, ISR | Fault tolerance, leader election | 40 min |
| 09 | [09-consumer-groups.md](09-consumer-groups.md) | Consumer groups, lag, rebalancing | Pub/Sub, parallelism, lag | 40 min |
| 10 | [10-multi-node-cluster.md](10-multi-node-cluster.md) | Multi-node cluster design | ZK ensemble, listeners, production layout | 30 min |
| 11 | [11-capstone-payment-processing.md](11-capstone-payment-processing.md) | Day 1 capstone | End-to-end payment platform | 60–75 min |

Total hands-on time is about 7.5 hours, plus short debriefs.

---

## Day 1 Architecture You Are Building

```text
                         QUICKCART
                    EVENT PLATFORM (Day 1)

     Order App          Payment App         Website
         |                   |                 |
         |                   |                 |
         v                   v                 v
   order-events        payment-events     click-events
         |                   |                 |
         +---------+---------+---------+-------+
                   |
                   v
        +--------------------------+
        |     Apache Kafka         |
        |                          |
        |  Broker 1   :9092        |
        |  Broker 2   :9093        |
        |  Broker 3   :9094        |
        +------------+-------------+
                     |
                     v
                 ZooKeeper
                   :2181

        Independent consumer groups
        ---------------------------
        inventory-service
        fraud-detection
        payment-analytics
```

Labs 01–03 run **one broker**. Labs 07–11 run **three brokers** on the same Windows machine.

---

## Standard Lab Environment

Use these paths and ports for every Day 1 lab unless a step says otherwise.

| Item | Value |
| --- | --- |
| Kafka home | `C:\kafka-labs\kafka` |
| Data root | `C:\kafka-labs\data` |
| ZooKeeper data | `C:\kafka-labs\data\zookeeper` |
| Single-broker logs | `C:\kafka-labs\data\kafka-0` |
| Broker 1 logs | `C:\kafka-labs\data\kafka-1` |
| Broker 2 logs | `C:\kafka-labs\data\kafka-2` |
| Broker 3 logs | `C:\kafka-labs\data\kafka-3` |
| ZooKeeper port | `2181` |
| Broker 1 | `localhost:9092` |
| Broker 2 | `localhost:9093` |
| Broker 3 | `localhost:9094` |
| Kafka version | Apache Kafka **3.8.x or 3.9.x** (ZooKeeper mode) |
| Java | **JDK 17** or later |
| Shell | Windows PowerShell |

Always change directory before running a Kafka script:

```powershell
cd C:\kafka-labs\kafka
```

---

## Terminal Map

Keep processes in dedicated terminals. Do not close a service terminal unless the lab says to stop that process.

| Terminal | Process | Typical command |
| ---: | --- | --- |
| 1 | ZooKeeper | `zookeeper-server-start.bat` |
| 2 | Broker 1 or single broker | `kafka-server-start.bat` |
| 3 | Broker 2 | `kafka-server-start.bat` |
| 4 | Broker 3 | `kafka-server-start.bat` |
| 5 | CLI / topics / groups | admin commands |
| 6 | Console producer | `kafka-console-producer.bat` |
| 7+ | Console consumers | `kafka-console-consumer.bat` |

---

## Lab File Template

Every lab file uses the same learner structure:

1. **Title and lab number**
2. **Description**
3. **Prerequisites**
4. **Business use case**
5. **Architecture diagram**
6. **Detailed steps**, starting with **Step 0** when setup is required
7. **Checkpoints** and observation tables
8. **Conclusion**
9. **Knowledge check**
10. **Common issues**

---

## Rules for Learners

- Complete labs in sequence. Later labs assume earlier configuration.
- Before Lab 07, stop the single-broker process from Lab 02. Do not run both layouts at the same time.
- When a broker or ZooKeeper is started, leave that terminal open.
- Record actual leader, replica, ISR, offset, and lag values. Do not copy sample tables as if they were your result.
- Kafka may assign leaders and partitions differently from the sample output. That is normal.
- Do not start Java producers or consumers today. Day 2 covers client APIs.

---

## Suggested Day Plan

| Block | Labs | Focus |
| --- | --- | --- |
| Morning 1 (90 min) | 01, 02, 03 | Install Kafka and move the first events |
| Morning 2 (90 min) | 04, 05, 06 | CLI, partitions, ZooKeeper |
| Afternoon 1 (90 min) | 07, 08 | Multi-broker cluster and replication |
| Afternoon 2 (90 min) | 09, 10 | Consumer groups and multi-node design |
| Close (75 min) | 11 | Capstone without trainer prompting |

---

## Start Lab 01

Open [01-environment-setup.md](01-environment-setup.md) and complete **Step 0**.

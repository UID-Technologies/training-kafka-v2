# Lab 08 – JDBC Connector Pipeline

**Lab Number:** 08  
**Day:** 3  
**Duration:** 45 minutes  
**Difficulty:** Intermediate  
**Environment:** Windows 10 / Windows 11  
**Mode:** Individual

---

## Description

JDBC connectors are an explicit Day 3 requirement.

You will create an `orders` table, insert two rows, write a JDBC source connector configuration, start the connector when the plugin is available, consume `quickcart-db-orders`, then insert a third row and watch it appear in Kafka.

Apache Kafka does not ship the JDBC connector. Use the jar the trainer provides, or complete the SQL and config even if the trainer runs the worker.

---

## Prerequisites

- Lab 07 standalone Connect works on `localhost:9092`
- A lightweight relational database is available, or the trainer prepared one
- This lab uses **H2** file mode unless the trainer specifies PostgreSQL / MySQL / SQL Server
- You understand source connectors from Lab 07

---

## Business Use Case

QuickCart has an existing relational database:

```text
QuickCart Database
       |
       | orders
       v
JDBC Source Connector
       |
       v
Kafka
       |
       v
quickcart-db-orders
```

No custom Java producer is required for database ingest.

---

## Architecture

```text
INSERT
  |
  v
Database table orders
  |
  v
JdbcSourceConnector
  mode=incrementing
  incrementing.column.name=id
  |
  v
topic.prefix + table name
  |
  v
quickcart-db-orders
```

```mermaid
flowchart LR
    SQL[INSERT orders] --> H2[(H2 database)]
    H2 --> JDBC[JDBC Source]
    JDBC --> T[quickcart-db-orders]
    T --> C[CLI consumer]
```

---

## Detailed Steps

### Step 0 – Initial Setup

```powershell
New-Item -ItemType Directory -Force -Path C:\kafka-labs\connect\plugins
New-Item -ItemType Directory -Force -Path C:\kafka-labs\data\h2
```

Confirm Connect still uses PLAINTEXT:

```powershell
cd C:\kafka-labs\kafka
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

Place these files if the trainer provided them:

| File | Destination |
| --- | --- |
| JDBC connector zip/jars | `C:\kafka-labs\connect\plugins\kafka-connect-jdbc\` |
| H2 jar (`h2-*.jar`) | same plugin folder **and** on the DB client classpath |

Add the plugin folder to the worker `plugin.path`:

```properties
plugin.path=C:/kafka-labs/kafka/libs,C:/kafka-labs/connect/plugins
```

If the JDBC plugin is missing, complete Steps 1–2 and 5 on paper, then watch the trainer start the connector.

---

### Step 1 – Create the table

The syllabus table is:

```sql
CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    amount DECIMAL(10,2),
    status VARCHAR(20)
);
```

`mode=incrementing` needs a numeric incrementing column. Use this training shape (same business columns, plus `id`):

```sql
CREATE TABLE orders (
    id IDENTITY PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    amount DECIMAL(10,2),
    status VARCHAR(20)
);
```

For PostgreSQL, use `id SERIAL PRIMARY KEY` instead of `IDENTITY`.

Insert:

```sql
INSERT INTO orders (order_id, customer_id, product_id, amount, status)
VALUES ('ORD8001','C801','P101',25000,'CREATED');

INSERT INTO orders (order_id, customer_id, product_id, amount, status)
VALUES ('ORD8002','C802','P102',15000,'CREATED');
```

**H2 example** (adjust the jar name):

```powershell
cd C:\kafka-labs\data\h2
java -cp C:\kafka-labs\connect\plugins\h2-2.2.224.jar org.h2.tools.Shell `
  -url jdbc:h2:C:/kafka-labs/data/h2/quickcart `
  -user sa `
  -password ""
```

Paste the `CREATE TABLE` and `INSERT` statements, then `exit`.

Record:

| order_id | customer_id | amount |
| --- | --- | ---: |
| ORD8001 | C801 | 25000 |
| ORD8002 | C802 | 15000 |

---

### Step 2 – Configure JDBC Source

Create `C:\kafka-labs\connect\jdbc-source.properties`. The exact class name depends on the connector build. For Confluent’s JDBC connector:

```properties
name=quickcart-jdbc-source
connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
tasks.max=1

connection.url=jdbc:h2:C:/kafka-labs/data/h2/quickcart
connection.user=sa
connection.password=

table.whitelist=orders
mode=incrementing
incrementing.column.name=id
topic.prefix=quickcart-db-

key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

If your plugin uses `JdbcSourceConnector` without the `io.confluent` package, change `connector.class` as the trainer directs.

---

### Step 3 – Start the connector

**Option A – standalone** (stop the Lab 07 worker first, or add the JDBC file to a new standalone process):

```powershell
cd C:\kafka-labs\kafka

.\bin\windows\connect-standalone.bat `
  C:\kafka-labs\connect\connect-standalone.properties `
  C:\kafka-labs\connect\jdbc-source.properties
```

**Option B – REST** if a distributed worker is already up on `http://localhost:8083`:

```powershell
# POST the connector JSON equivalent of jdbc-source.properties
# using the worker REST API the trainer enabled
```

Use the Kafka Connect REST API or worker configuration appropriate to the installed connector.

---

### Step 4 – Verify the topic

```powershell
.\bin\windows\kafka-topics.bat `
  --list `
  --bootstrap-server localhost:9092
```

You should see a topic such as:

```text
quickcart-db-orders
```

(`topic.prefix` + table name).

Consume:

```powershell
.\bin\windows\kafka-console-consumer.bat `
  --bootstrap-server localhost:9092 `
  --topic quickcart-db-orders `
  --from-beginning
```

You should see JSON (or structured) records for ORD8001 and ORD8002.

---

### Step 5 – Add a database record

```sql
INSERT INTO orders (order_id, customer_id, product_id, amount, status)
VALUES ('ORD8003','C803','P103',99000,'CREATED');
```

Verify it appears in Kafka without running a Java producer.

Students should now understand:

```text
INSERT
  |
  v
Database
  |
  v
JDBC Connector
  |
  v
Kafka
```

Complete:

| Check | Yes / No / Trainer demo |
| --- | --- |
| Table created | |
| Connector config written | |
| `quickcart-db-orders` listed | |
| ORD8003 appeared after INSERT | |

---

## Conclusion

Database rows can become Kafka events through a JDBC source connector. Incrementing mode uses a rising `id` so new inserts are captured.

If the plugin was trainer-hosted, you still own the table design and the connector contract.

**You are ready for Lab 09 when:**

- The `orders` table exists
- `jdbc-source.properties` is filled
- You can explain `mode=incrementing`

Next lab: [09-cassandra-elasticsearch.md](09-cassandra-elasticsearch.md)

---

## Knowledge Check

1. Why add `id` if `order_id` is already unique?
2. What does `topic.prefix=quickcart-db-` produce for table `orders`?
3. Why not write a Java producer for every table?
4. What happens if you reuse incrementing mode after deleting rows and resetting ids?

**Expected answers**

1. Incrementing mode needs a numeric, ever-increasing column.
2. Topic `quickcart-db-orders`.
3. Connect already implements polling, offsets, and tasks.
4. You can miss or duplicate events. Treat incrementing values as append-only.

---

## Common Issues

| Symptom | Likely cause | What to do |
| --- | --- | --- |
| Plugin not found | `plugin.path` missing JDBC jars | Restart worker after copying jars |
| `incrementing column` error | No numeric `id` | Use the training table DDL |
| H2 locked | Two processes opened the same file DB | Close the Shell before Connect starts |
| No new records | Connector already stored offset | INSERT a new row; do not reuse old ids |

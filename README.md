# Periscope

> A resilient PostgreSQL change-data-capture pipeline for Apache Kafka, written in modern Java.

Periscope turns committed PostgreSQL row changes into durable, ordered events that downstream services can consume. It reads the PostgreSQL Write-Ahead Log (WAL) through logical replication, converts database mutations into structured `ChangeEvent` records, publishes them to Kafka, and uses Raft to coordinate an active node during failover.

```text
PostgreSQL change → WAL → Periscope → Kafka topic → Your application
```

The project is a practical implementation of CDC, Kafka delivery, distributed coordination, and end-to-end verification.

## Why Periscope?

Polling a database for changes is slow, wasteful, and easy to get wrong. A polling process can miss updates, repeatedly process the same row, or struggle to determine what was committed and when.

Periscope listens to PostgreSQL's transaction log instead. Every committed row mutation becomes an event with its operation, primary-key information, WAL position, timestamp, and before/after values. Kafka provides durable storage and replayable delivery for downstream consumers.

## How it works

```text
┌──────────────┐     logical replication      ┌──────────────────┐
│ PostgreSQL   │ ───────────────────────────▶ │ Periscope node  │
│ tables + WAL │                              │                  │
└──────────────┘                              │ WAL parser       │
                                              │ Topic router     │
                                              │ Kafka publisher  │
                                              └────────┬─────────┘
                                                       │ acks=all
                                                       ▼
                                              ┌──────────────────┐
                                              │ Apache Kafka     │
                                              │ periscope.<schema>│
                                              │ .<table>         │
                                              └────────┬─────────┘
                                                       ▼
                                              Downstream services
```

### The event path

1. PostgreSQL commits an `INSERT`, `UPDATE`, `DELETE`, or `TRUNCATE`.
2. The logical replication slot exposes the WAL change to Periscope.
3. `WalMessageParser` converts the `test_decoding` message into a typed `ChangeEvent`.
4. `TopicRouter` maps the event to `periscope.<schema>.<table>` and uses the row primary key as the Kafka key.
5. `KafkaChangePublisher` sends the serialized JSON event with durable producer settings.
6. Only after Kafka acknowledges the record does Periscope send the processed LSN back to PostgreSQL.
7. Raft determines which node owns the active CDC pipeline; standby nodes can take over after leader failure.

## Example event

```json
{
  "schemaName": "public",
  "tableName": "customers",
  "operation": "UPDATE",
  "lsn": 123456,
  "timestamp": "2026-09-13T10:15:30Z",
  "before": null,
  "after": {
    "id": 100,
    "name": "Johnathan Doe"
  },
  "primaryKeyColumn": "id"
}
```

For a customer with primary key `100`:

```text
Topic: periscope.public.customers
Key:   100
```

Using the primary key as the Kafka key keeps all changes for the same entity on the same partition, preserving their order for that entity.

## Main capabilities

- **Non-polling CDC** through PostgreSQL logical replication.
- **Typed change events** with operation, LSN, timestamp, and row values.
- **Per-entity ordering** through primary-key Kafka partitioning.
- **Durable publishing** with `acks=all` and idempotent Kafka production.
- **LSN feedback** only after successful Kafka delivery.
- **Raft-based leader election** for active-passive failover.
- **Runnable sample consumer** for observing table events in real time.
- **End-to-end verification** across PostgreSQL, logical replication, Kafka, and consumers.

## Project structure

```text
src/main/java/com/periscope/
├── cdc/          PostgreSQL connections, slots, WAL parsing, stream consumption
├── config/       Type-safe database, Kafka, and application configuration
├── consensus/    Raft state, transport, elections, and failover lifecycle
├── demo/         Sample downstream Kafka consumer
├── kafka/        Event serialization, topic routing, and publishing
├── model/        ChangeEvent and operation types
└── pipeline/     CDC-to-Kafka coordination and LSN acknowledgement

src/test/java/com/periscope/
├── cdc/          Parser, connection, slot, and live stream tests
├── consensus/    Transport, election, and failover tests
├── e2e/          PostgreSQL-to-Kafka ordering verification
└── pipeline/     Unit tests for routing and acknowledgement behavior
```

## Quick start

### Requirements

- Java 21 or newer
- Maven 3.9+
- Docker Desktop with Docker Compose

### 1. Start PostgreSQL and Kafka

```bash
docker compose up -d
docker compose ps
```

The local environment exposes PostgreSQL at `localhost:5432` and Kafka at `localhost:9092`. PostgreSQL uses database `periscope_db`, user `postgres`, and password `periscope`. The initialization script creates the `customers` and `orders` tables and the `periscope_pub` publication.

### 2. Build the project

```bash
mvn clean package
mvn dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
```

### 3. Start Periscope

```bash
java -cp "target/classes:$(cat target/classpath.txt)" com.periscope.PeriscopeApp
```

The default process starts a single Periscope node, creates the configured publication and replication slot when necessary, waits for Raft leadership, and then starts the CDC-to-Kafka pipeline.

### 4. Start the sample consumer

In another terminal:

```bash
java -cp "target/classes:$(cat target/classpath.txt)" \
  com.periscope.demo.SampleConsumer \
  localhost:9092 periscope-demo periscope.public.customers
```

Then create a database change:

```bash
docker compose exec -T postgres psql -U postgres -d periscope_db \
  -c "INSERT INTO customers(name, email) VALUES ('Demo User', 'demo@example.com');"
```

The sample consumer logs the received operation, Kafka key, topic, partition, offset, and PostgreSQL LSN.

For the complete runbook, troubleshooting guide, and multi-node instructions, see [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md).

## Running tests

Run the complete suite:

```bash
mvn test
```

The suite covers configuration validation, WAL parsing, replication-slot lifecycle, Kafka publishing, pipeline acknowledgements, Raft transport, leader failover, and the full `INSERT → UPDATE → DELETE` path.

The infrastructure-backed tests expect PostgreSQL and Kafka to be available locally. Run the key integration checks individually with:

```bash
mvn -Dtest=DataIntegrityE2ETest test
mvn -Dtest=FailoverIntegrationTest test
```

The data-integrity test verifies that three mutations for one customer arrive at Kafka in strict order and use the same partition key. The failover test starts three real Raft transports, stops the elected leader, and verifies that a remaining node takes over.

## Configuration

Default application settings live in [`src/main/resources/application.properties`](src/main/resources/application.properties). Environment variables override database and Kafka values:

```text
PERISCOPE_DB_HOST
PERISCOPE_DB_PORT
PERISCOPE_DB_NAME
PERISCOPE_DB_USER
PERISCOPE_DB_PASSWORD
PERISCOPE_DB_SLOT
PERISCOPE_DB_PUBLICATION
PERISCOPE_KAFKA_BOOTSTRAP_SERVERS
PERISCOPE_KAFKA_ACKS
PERISCOPE_KAFKA_RETRIES
PERISCOPE_KAFKA_TOPIC_PREFIX
```

Raft node settings are supplied separately for multi-process demos:

```text
PERISCOPE_NODE_ID
PERISCOPE_RAFT_PORT
PERISCOPE_RAFT_PEERS
```

Example:

```bash
PERISCOPE_NODE_ID=node-1 \
PERISCOPE_RAFT_PORT=9001 \
PERISCOPE_RAFT_PEERS=localhost:9002,localhost:9003 \
java -cp "target/classes:$(cat target/classpath.txt)" com.periscope.PeriscopeApp
```

## Documentation

- [Getting Started](docs/GETTING_STARTED.md)
- [Architecture Specification](docs/ARCHITECTURE.md)
- [Problem Statement](docs/PROBLEM_STATEMENT.md)
- [Implementation Plan and Roadmap](docs/PLAN.md)

## Project status

The foundation, PostgreSQL CDC engine, Kafka pipeline, Raft failover simulation, sample consumer, end-to-end ordering test, and local developer guide are implemented. The project is intentionally compact and educational, making the data path and distributed-systems trade-offs easy to inspect.

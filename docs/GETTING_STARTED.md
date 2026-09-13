# Getting started

This guide runs one Periscope node locally, publishes PostgreSQL row changes to Kafka, and consumes them with the sample client.

## Prerequisites

- Java 21 or newer
- Maven 3.9+
- Docker Desktop with Docker Compose

Verify the tools:

```bash
java -version
mvn -version
docker compose version
```

## Start PostgreSQL and Kafka

From the repository root:

```bash
docker compose up -d
docker compose ps
```

PostgreSQL is available at `localhost:5432` with database `periscope_db`, user `postgres`, and password `periscope`. Logical replication is enabled and the `customers` and `orders` tables are created by [`init.sql`](../init.sql). Kafka is available at `localhost:9092`.

Wait until PostgreSQL is healthy before starting a node:

```bash
until docker compose exec -T postgres pg_isready -U postgres -d periscope_db; do sleep 1; done
```

## Build Periscope

```bash
mvn clean package
mvn dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
```

The project targets Java 21 bytecode and uses the local Java runtime to launch it.

## Start a Periscope node

The default configuration connects to the local PostgreSQL and Kafka services and starts a single-node Raft cluster:

```bash
java -cp "target/classes:$(cat target/classpath.txt)" com.periscope.PeriscopeApp
```

The node creates the `periscope_pub` publication and `periscope_slot` logical replication slot if they do not exist. Once elected, it starts the CDC-to-Kafka pipeline. Stop it with `Ctrl-C` so the shutdown hook can close the replication stream and Kafka producer.

Optional node settings are supplied through environment variables:

```bash
PERISCOPE_NODE_ID=node-1 \
PERISCOPE_RAFT_PORT=9001 \
PERISCOPE_RAFT_PEERS=localhost:9002,localhost:9003 \
java -cp "target/classes:$(cat target/classpath.txt)" com.periscope.PeriscopeApp
```

For a real three-node experiment, start three processes with distinct node IDs and ports. Each process's peer list should contain the other two nodes.

## Start the sample consumer

In another terminal:

```bash
java -cp "target/classes:$(cat target/classpath.txt)" \
  com.periscope.demo.SampleConsumer \
  localhost:9092 periscope-demo periscope.public.customers
```

The consumer uses the `periscope-demo` group, subscribes to the customers topic, and logs each event's Kafka key, operation, and PostgreSQL LSN.

Generate an event from a third terminal:

```bash
docker compose exec -T postgres psql -U postgres -d periscope_db \
  -c "INSERT INTO customers(name, email) VALUES ('Demo User', 'demo@example.com');"
```

The sample consumer should print an `INSERT` event. Updates and deletes are routed to the same topic and retain the row's primary-key key.

## Run verification

The end-to-end ordering test requires both services:

```bash
mvn -Dtest=DataIntegrityE2ETest test
```

It resets a dedicated replication slot, performs `INSERT -> UPDATE -> DELETE` for customer `100`, and verifies that Kafka receives exactly three events in that order with key `100`.

The Raft failover simulation does not require Docker:

```bash
mvn -Dtest=FailoverIntegrationTest test
```

## Test failover manually

Start three nodes with peer lists containing the other two nodes. Wait for one node to become leader, then terminate that process. The remaining nodes should elect a replacement leader within the configured election window. The automated version of this scenario is [`FailoverIntegrationTest`](../src/test/java/com/periscope/consensus/FailoverIntegrationTest.java).

## Troubleshooting

- `Connection refused` on port `5432` or `9092`: run `docker compose ps` and inspect `docker compose logs postgres kafka`.
- Kafka events are not visible: start the consumer before producing the database mutation and use a new group ID.
- The end-to-end test is skipped: the test intentionally skips when PostgreSQL or Kafka is unavailable.
- A stale replication slot exists: stop Periscope before removing it, then run `docker compose exec -T postgres psql -U postgres -d periscope_db -c "SELECT pg_drop_replication_slot('periscope_e2e_slot');"`.

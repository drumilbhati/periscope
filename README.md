# Periscope

> High-Availability PostgreSQL Change Data Capture (CDC) & Event Streaming Pipeline in Java

**Periscope** continuously captures committed row-level database changes from PostgreSQL's Write-Ahead Log (WAL), formats them into structured change events, routes them to Apache Kafka topics partitioned by primary key, and coordinates high-availability active-passive failover across Periscope nodes using Raft consensus.

---

## 📚 Documentation

* [Problem Statement](file:///Users/drumilbhati/Documents/Github/periscope/docs/PROBLEM_STATEMENT.md) — Motivation and core objectives.
* [Architecture Specification](file:///Users/drumilbhati/Documents/Github/periscope/docs/ARCHITECTURE.md) — Comprehensive technical architecture (Path A: CDC + Kafka + Raft HA).
* [Implementation Plan & Roadmap](file:///Users/drumilbhati/Documents/Github/periscope/docs/PLAN.md) — Phased milestones, atomic task checklist, and progress tracker.

---

## 🚀 Key Features

* **Non-Polling CDC:** Streams changes via PostgreSQL logical replication slots (`test_decoding` / `pgoutput`).
* **Strict Per-Row Ordering:** Enforces chronological ordering by partitioning Kafka topics using the row's Primary Key.
* **Guaranteed Durability:** Coordinates Kafka delivery confirmations (`acks=all`) directly with PostgreSQL LSN feedback before WAL segments are released.
* **Raft Consensus for High Availability:** Automated leader election and zero-downtime failover for PostgreSQL replication leases.
* **Standard Downstream Consumption:** Publishes directly to Apache Kafka, enabling standard Kafka consumer groups in any language.
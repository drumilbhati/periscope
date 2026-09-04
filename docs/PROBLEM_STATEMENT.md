# Periscope — Problem Statement

Modern applications often need to react when data changes in a database. For example, an order being created may need to trigger a notification, update an analytics system, or start another process.

A simple approach is to repeatedly query the database for changes. This is inefficient, can miss changes, and makes it difficult to reliably track what has already been processed.

**Periscope** aims to solve this problem by continuously observing committed changes in a PostgreSQL database and turning them into a reliable stream of events that other applications can consume.

The system should:

- Detect database changes without repeatedly polling tables.
- Preserve the order of changes within a database stream.
- Store changes durably so they are not lost when a process crashes.
- Allow consumers to read and process events independently.
- Recover from failures and continue from the correct position.
- Eventually support multiple Periscope nodes so the system can continue operating when a node fails.

The project will start as a simple single-node system and gradually evolve into a fault-tolerant distributed system.

**In short:** Periscope is a system for reliably observing, storing, and delivering PostgreSQL database changes to other applications.

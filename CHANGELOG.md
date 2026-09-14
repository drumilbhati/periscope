# Changelog

## 1.0.0 (2026-09-14)


### Features

* add DatabaseConfig and KafkaConfig records with validation and tests ([25c5207](https://github.com/drumilbhati/periscope/commit/25c52073d6bb3a9d43a761374b2558f70198b71b))
* configure SLF4J and Logback for structured logging with custom logger levels ([6164d61](https://github.com/drumilbhati/periscope/commit/6164d615a2592ee1f804c6132bbd39f15cbdbe66))
* implement CdcStreamConsumer for streaming PostgreSQL changes with LSN feedback ([ab3e610](https://github.com/drumilbhati/periscope/commit/ab3e61068e28bf93d84a73f808835ab9ab3ec11e))
* implement EventSerializer for JSON Schema Envelope serialization ([4b73a74](https://github.com/drumilbhati/periscope/commit/4b73a7408a91194233449e98a4a4b968892505bb))
* implement KafkaChangePublisher for idempotent message publishing ([df5ac66](https://github.com/drumilbhati/periscope/commit/df5ac666f16634e32ac46789588ae1518134db51))
* implement PeriscopeConfig loader with property parsing and add application.properties ([94cee3e](https://github.com/drumilbhati/periscope/commit/94cee3e6b9bc538763588c2aaaadfd66243eb1a6))
* implement PostgresConnectionFactory for creating JDBC and replication connections ([f85c65f](https://github.com/drumilbhati/periscope/commit/f85c65f4576b28e3bc2d69940d2d0d90c9c53f8d))
* implement ReplicationSlotManager for managing PostgreSQL replication slots and publications ([29c5ec3](https://github.com/drumilbhati/periscope/commit/29c5ec3249a27aaba32b68ce5b09581bc108e5d3))
* implement WalMessageParser for parsing PostgreSQL test_decoding messages into ChangeEvent records ([5ae327d](https://github.com/drumilbhati/periscope/commit/5ae327d743ca4ab5f2966b88d82750aede96a596))
* initialize Maven project with Java 21+ and core dependencies (closes [#6](https://github.com/drumilbhati/periscope/issues/6)) ([12fd76c](https://github.com/drumilbhati/periscope/commit/12fd76cf434552c9230666d7f7857845181e1993))
* model CDC ChangeEvent and OperationType using Java records ([264273b](https://github.com/drumilbhati/periscope/commit/264273be861061aa158d2daa572dc22a5d21617f))


### Documentation

* add detailed implementation plan and roadmap linked to GitHub issues ([402f5d0](https://github.com/drumilbhati/periscope/commit/402f5d0f71743963c83b429d2321e3c4e5a47680))
* add Path A architecture specification in docs/ARCHITECTURE.md ([ee697d7](https://github.com/drumilbhati/periscope/commit/ee697d78a5314da8a1c49677aa8c29c7c506dcf0))
* link milestones and add issue progress tracker table in PLAN.md ([50c3c12](https://github.com/drumilbhati/periscope/commit/50c3c1265d2c2ca488e6d5a05052ba7ff84a07df))
* mark Issue [#7](https://github.com/drumilbhati/periscope/issues/7) and Issue [#8](https://github.com/drumilbhati/periscope/issues/8) as completed in PLAN.md ([35d8405](https://github.com/drumilbhati/periscope/commit/35d84056d5cdec684858c749fafb661283476a9e))
* remove Path A references from documentation ([4522204](https://github.com/drumilbhati/periscope/commit/4522204a28b98646e9e756fe15baa9f6e5412c08))

# Changelog

All notable changes to FormDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

---

## [0.0.5] - 2026-01-12

Multi-Protocol API Server milestone: **M11 Complete**

This release completes Milestone M11, delivering a production-ready multi-protocol API server with full Form.Bridge FFI integration.

### Added

#### Multi-Protocol API Server
- **REST API** (`api/src/rest.zig`)
  - OpenAPI 3.1 specification compliance
  - Full CRUD endpoints for collections, documents, and queries
  - Health and metrics endpoints
  - Wired to Form.Bridge FFI for real database operations

- **gRPC API** (`api/src/grpc.zig`)
  - Protocol Buffer serialization/deserialization
  - Full protobuf encoder with varint, tag/wire type handling
  - Protobuf decoder for message parsing
  - All service methods wired to Form.Bridge FFI
  - Support for Query, ListCollections, GetCollection, CreateCollection
  - Support for GetJournal, DiscoverDependencies, AnalyzeNormalForm
  - Migration operations (StartMigration, GetMigrationStatus)

- **GraphQL API** (`api/src/graphql.zig`)
  - GraphQL SDL schema with full type system
  - Query, Mutation, and Subscription support
  - GraphiQL UI for exploration
  - Introspection support
  - WebSocket integration for subscriptions

- **WebSocket Support** (`api/src/websocket.zig`)
  - RFC 6455 compliant WebSocket implementation
  - WebSocket upgrade handling with SHA-1 accept key
  - Frame encoding/decoding (text, binary, ping, pong, close)
  - graphql-ws protocol for GraphQL subscriptions
  - Subscription management with connection state
  - Journal streaming subscription type

- **Form.Bridge Integration** (`api/src/bridge_client.zig`)
  - FFI bindings to core FormDB engine
  - CBOR encoding for all operations
  - Graceful degraded mode when bridge unavailable
  - Health check, query execution, collection management

#### Integration Tests (`api/src/integration_tests.zig`)
- REST API endpoint tests
- gRPC protobuf encoder/decoder tests
- gRPC frame encoding tests
- GraphQL request parsing tests
- WebSocket accept key computation tests (RFC 6455 compliance)
- WebSocket frame encoding tests
- WebSocket subscription message tests
- Bridge client integration tests

#### Ecosystem Coordination
- **UNIFIED-ROADMAP.scm** - Cross-repo roadmap for MVP 1.0.0
  - Dependency graph for FormDB, FQLdt, Studio, Debugger
  - Critical path phases P1-P3
  - Post-MVP roadmap (1.1.0, 1.2.0, 2.0.0)
  - Success metrics and quality gates

### Changed
- Updated STATE.scm to M11 100% completion
- All API handlers now use Form.Bridge FFI instead of mock responses
- Build configuration includes all new modules in test suite

---

## [0.0.4] - 2026-01-12

MVP Completion milestone: **Form.Runtime + Form.Normalizer Complete**

This release completes Milestones M8-M10, delivering a fully functional query engine and self-normalizing database capabilities.

### Added

#### Form.Runtime (M8) - Query Engine
- **FDQL Parser** (`core-factor/fdql/fdql.factor`)
  - Full PEG-based parser for FQL statements
  - Support for SELECT, INSERT, UPDATE, DELETE, CREATE, DROP
  - EXPLAIN, INTROSPECT statements
  - WHERE clause with comparison operators
  - LIMIT/OFFSET pagination
  - Graph traversal (TRAVERSE) syntax
  - WITH PROVENANCE clause

- **Query Planner**
  - Cost-based query planning
  - Step types: scan, project, limit, traverse, insert, update, delete
  - Plan optimization for filtered queries
  - Rationale generation for plan decisions

- **Query Executor**
  - In-memory and pluggable persistent storage
  - Full CRUD operations
  - Filter evaluation engine
  - Introspection commands (SCHEMA, CONSTRAINTS, COLLECTIONS, JOURNAL)

- **EXPLAIN Modes**
  - `EXPLAIN` - Show query plan
  - `EXPLAIN ANALYZE` - Execute and report timing
  - `EXPLAIN VERBOSE` - PostgreSQL-style readable plan output
  - `EXPLAIN ANALYZE VERBOSE` - Combined timing and verbose output

#### Form.Normalizer (M9) - Self-Normalizing Engine
- **FD Discovery** (`normalizer/factor/fd-discovery.factor`)
  - DFD (Depth-First Discovery) algorithm implementation
  - Configurable sampling and confidence thresholds
  - Three-tier confidence classification (high/medium/low)
  - Attribute partition refinement
  - Discovered FD validation

- **Normal Form Analysis**
  - 1NF through BCNF detection
  - Violation identification with explanations
  - Prime attribute detection
  - Key inference from functional dependencies

- **Denormalization Proposals**
  - Automatic denormalization suggestion generation
  - Join-based vs materialized view approaches
  - Migration path generation

- **Three-Phase Migration Framework** (`normalizer/factor/migration.factor`)
  - Announce phase: Signal intent, generate rewrite rules
  - Shadow phase: Dual-write to old and new schemas
  - Commit phase: Complete migration, remove compatibility views
  - Query rewriting during migration
  - Rollback support (abort migration)
  - Migration state tracking and narrative generation

#### Lean4 Integration
- **Bridge.lean** - FFI bindings with CBOR encoding
  - Status codes matching Zig ABI
  - FD proof encoding/decoding
  - Normalization proof encoding
  - Verification API

- **Proofs.lean** - Proof-carrying transformations
  - VerifiedFD, VerifiedNormalizationStep, VerifiedDenormalizationStep
  - VerifiedMigration with phase tracking
  - Journal packaging for proof blobs
  - Round-trip verification support

- **lakefile.toml** - Lean4 project configuration
  - Dependencies on Std, Mathlib4
  - Build configuration for FormNormalizer library

#### Production Hardening (M10)
- **Seam Tests** (`core-factor/fdql/seam-tests.factor`)
  - End-to-end pipeline validation: Parser → Planner → Executor → Normalizer
  - EXPLAIN correlation tests
  - Error propagation tests
  - Large dataset stress tests

- **Benchmarks** (`core-factor/fdql/benchmarks.factor`)
  - Parser performance benchmarks
  - Planner performance benchmarks
  - Executor benchmarks (SELECT, INSERT, UPDATE with varying data sizes)
  - FD discovery benchmarks
  - Normal form analysis benchmarks
  - Full pipeline benchmarks
  - Memory estimation utilities
  - Quick benchmark for CI regression detection

- **Storage Backend** (`core-factor/fdql/storage-backend.factor`)
  - Pluggable storage abstraction
  - Memory backend (default, for testing)
  - Bridge backend (persistent storage via Form.Bridge)
  - Runtime backend selection

#### Ecosystem Alignment
- Updated ECOSYSTEM.scm with alignment status for fdql-dt and formdb-debugger
- Cross-repo STATE.scm synchronization with integration points
- Documented FFI compatibility (CBOR proof blobs)
- Identified alignment gaps for future work

### Changed
- Executor now uses pluggable storage backend instead of direct hash table
- STATE.scm updated to Phase 8: MVP Complete (85% overall completion)
- All milestones M1-M10 now at 100%

### Migration Tests
- Comprehensive test suite for three-phase migration lifecycle
- Phase transition validation
- Rewrite rule generation tests
- Compatibility view tests
- Error handling and recovery tests

---

## [0.0.3] - 2026-01-12

Documentation milestone release: **Complete Documentation Suite**

This release completes Milestone M7, providing comprehensive documentation for production deployment and integration.

### Added
- Comprehensive QUICKSTART.adoc tutorial (15-minute guide with full examples)
- Complete VERSIONING.adoc stability policy document
- Complete documentation suite in `docs/`:
  - `docs/DEPLOYMENT.adoc` - Production deployment guide (Docker, Kubernetes, systemd)
  - `docs/SECURITY-AUTH.adoc` - Authentication, authorization, and security hardening
  - `docs/API-REFERENCE.adoc` - Complete programmatic interface reference (Form.Bridge FFI, FQL API)
  - `docs/MIGRATION-FROM-RDBMS.adoc` - PostgreSQL/MySQL/SQLite migration guide with type mappings
  - `docs/OBSERVABILITY.adoc` - Logging, Prometheus metrics, OpenTelemetry tracing, Grafana dashboards
  - `docs/INTEGRATION-PATTERNS.adoc` - Message queues, search engines, analytics, AI/ML pipelines

### Changed
- License updated from MPL-2.0 to Palimpsest-MPL 1.0 (PMPL-1.0)
- README.adoc reorganized with comprehensive Documentation section

### Fixed
- License badge now correctly shows PMPL-1.0
- Fixed typo in Palimpsest link (licence → license)

---

## [0.0.2] - 2026-01-11

Major milestone release: **Core Specifications Complete + PoC Implementation**

This release completes Milestones M1-M6, establishing FormDB as a functional proof-of-concept.

### Added

#### Core Specifications (M1)
- **FQL Language Specification** (`spec/fql.adoc`)
  - Complete EBNF grammar for PoC subset
  - 10 example queries covering all operations
  - Document, edge, schema, and introspection operations
  - Provenance syntax (`WITH PROVENANCE {...}`)

- **FQL Dependent Types Specification** (`spec/fql-dependent-types.md`)
  - Full FQLdt specification with Lean 4 integration
  - Compile-time query verification
  - Proof-carrying schema evolution
  - Type-level encoding of database constraints

- **Self-Normalizing Database Specification** (`spec/self-normalizing.adoc`)
  - Automatic functional dependency discovery (DFD/TANE/FDHits)
  - Normal form predicates (1NF through BCNF)
  - Proof-carrying normalization decisions
  - Narrative explanations for all schema changes

- **Block Format Specification** (`spec/blocks.adoc`)
  - 4 KiB fixed-size blocks with 64-byte headers
  - Block types: SUPERBLOCK, DOCUMENT, EDGE, JOURNAL, SCHEMA, etc.
  - CRC32C checksums for integrity
  - Compression and encryption flags

- **Journal Format Specification** (`spec/journal.adoc`)
  - Append-only journal with sequence numbers
  - Full operation history with inverses
  - Crash recovery and replay semantics

- **Cloud Storage Specification** (`spec/cloud-storage.adoc`)
  - Object storage integration patterns
  - Tiered storage for hot/warm/cold data

- **FQL Design Philosophy** (`spec/fql-philosophy.adoc`)
  - Narrative-first query design
  - Comparison with SQL philosophy
  - Constraints as ethics

#### Forth Implementation (M2-M5)
- **Form.Blocks** (`core-forth/src/formdb-blocks.fs`)
  - Fixed-size block storage layer
  - Block header structure with magic, version, type, checksums
  - Memory buffer management
  - CRC32C implementation (Castagnoli polynomial)

- **Form.Journal** (`core-forth/src/formdb-journal.fs`)
  - Append-only journal implementation
  - Sequence numbering
  - Operation logging with inverses
  - Crash recovery primitives

- **Form.Model** (`core-forth/src/formdb-model.fs`)
  - Document collection support
  - Edge collection support
  - Schema metadata storage
  - Constraint storage

- **Test Suite** (`core-forth/test/`)
  - Block operations tests
  - Journal operations tests
  - Model layer tests

#### Documentation
- Architecture guide (`ARCHITECTURE.adoc`)
- Roadmap (`ROADMAP.adoc`)
- Philosophy document (`PHILOSOPHY.adoc`)
- Contributing guidelines (`CONTRIBUTING.adoc`)
- Maintainers list (`MAINTAINERS.adoc`)

#### Ecosystem Integration
- Related projects documentation:
  - FormDB Studio (zero-friction GUI)
  - FormDB Debugger (proof-carrying debugger)
  - FormBase (Airtable alternative)
  - Zotero-FormDB (reference manager)
  - FQLdt (dependently-typed FQL)

#### Machine-Readable Artefacts
- 6SCM files for AI agent integration:
  - `STATE.scm` - Project state tracking
  - `META.scm` - Architecture decisions
  - `ECOSYSTEM.scm` - Ecosystem position
  - `PLAYBOOK.scm` - Operational runbook
  - `AGENTIC.scm` - AI interaction patterns
  - `NEUROSYM.scm` - Neurosymbolic config

### Changed
- Eliminated C dependency in favor of Zig-only ABI (Form.Bridge)
- Consolidated FQLdt specification into single comprehensive document
- Clarified that FQL = FormDB Query Language (not "forms" query language)

### Security
- Fixed workflow security issues (ERR-WF-008, ERR-WF-009)
- Updated actions/cache SHA from v2 to v4

---

## [0.0.1] - 2026-01-03

Initial release: **Repository Initialization**

### Added

#### Repository Structure
- Initial repository setup following RSR (Rhodium Standard Repositories) pattern
- Standard hyperpolymath/mustfile structure
- RSR enforcement workflows

#### Documentation Framework
- README.adoc with project overview
- Core thesis: "Schemas, constraints, migrations, blocks, and journals are narrative artefacts"
- Primary values table (Auditability > Performance, Meaning > Features, etc.)
- Target domains: investigative journalism, governance, agentic ecosystems, archives
- Layer architecture diagram

#### Licensing
- MPL-2.0 base license
- Palimpsest philosophy notice (ethical open source)

#### CI/CD
- GitHub Actions workflows for quality enforcement
- Casket-SSG GitHub Pages workflow
- Security scanning workflows

---

## Version History Summary

| Version | Date | Milestone | Key Features |
|---------|------|-----------|--------------|
| 0.0.5 | 2026-01-12 | M11 Complete | Multi-protocol API server, WebSocket subscriptions |
| 0.0.4 | 2026-01-12 | M8-M10 Complete | Query engine, normalizer, production hardening |
| 0.0.3 | 2026-01-12 | M7 Complete | Complete documentation suite, PMPL-1.0 license |
| 0.0.2 | 2026-01-11 | M1-M6 Complete | Full specs, Forth PoC, documentation |
| 0.0.1 | 2026-01-03 | Repository Init | Structure, licensing, CI/CD |

## Upgrade Notes

### Upgrading to 0.0.3

No breaking changes from 0.0.2. This release adds comprehensive documentation and changes the license to PMPL-1.0.

### Upgrading to 0.0.2

No breaking changes from 0.0.1. This release adds specifications and implementation.

### Pre-1.0 Warning

FormDB is in pre-1.0 development. APIs, formats, and interfaces may change without deprecation warnings. See [VERSIONING.adoc](VERSIONING.adoc) for stability guarantees.

---

## Links

[Unreleased]: https://github.com/hyperpolymath/formdb/compare/v0.0.5...HEAD
[0.0.5]: https://github.com/hyperpolymath/formdb/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/hyperpolymath/formdb/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/hyperpolymath/formdb/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/hyperpolymath/formdb/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/hyperpolymath/formdb/releases/tag/v0.0.1

## Related Documentation

- [VERSIONING.adoc](VERSIONING.adoc) - Stability policy and version guarantees
- [ROADMAP.adoc](ROADMAP.adoc) - Planned features and milestones
- [QUICKSTART.adoc](QUICKSTART.adoc) - Getting started guide
- [ARCHITECTURE.adoc](ARCHITECTURE.adoc) - Technical architecture

## Changelog Conventions

This changelog follows these conventions:

- **Added** - New features
- **Changed** - Changes to existing functionality
- **Deprecated** - Features that will be removed in future versions
- **Removed** - Features that have been removed
- **Fixed** - Bug fixes
- **Security** - Security-related changes

Each release includes:
- Summary of the milestone achieved
- Detailed list of changes by category
- Breaking changes highlighted (when applicable)
- Upgrade notes (when applicable)

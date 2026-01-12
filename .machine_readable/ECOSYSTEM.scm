;; SPDX-License-Identifier: PMPL-1.0
;; ECOSYSTEM.scm - Ecosystem position for FormDB
;; Media-Type: application/vnd.ecosystem+scm

(ecosystem
  (version "1.0")
  (name "FormDB")
  (type "database-engine")
  (purpose "Narrative-first, reversible, audit-grade database for domains
            where provenance, auditability, and human understanding matter
            more than raw performance.")

  (position-in-ecosystem
    (category "Databases")
    (subcategory "Multi-Model / Document-Graph Hybrid")
    (unique-value
      "Provenance-native: Every mutation requires actor + rationale"
      "Reversible: Append-only journal with inverses for any operation"
      "Narrative: Schemas, constraints, migrations are human-readable artefacts"
      "Self-normalizing: Discovers FDs and proposes schema improvements with proofs"
      "Audit-grade: Complete history from genesis, time-travel queries"))

  (related-projects
    ;; Sibling projects in the FormDB ecosystem
    (formdb-studio
      (relationship "sibling-tool")
      (repo "github.com/hyperpolymath/formdb-studio")
      (status "planned")
      (description "Zero-friction GUI for FormDB. Visual schema design,
                    query builder, provenance explorer, journal viewer."))

    (formdb-debugger
      (relationship "sibling-tool")
      (repo "github.com/hyperpolymath/formdb-debugger")
      (status "planned")
      (description "Proof-carrying debugger. Step through queries, inspect
                    constraint violations, visualize normalization proofs."))

    (formbase
      (relationship "sibling-application")
      (repo "github.com/hyperpolymath/formbase")
      (status "planned")
      (description "Airtable alternative built on FormDB. Spreadsheet UI
                    with full provenance, versioning, and audit trail."))

    (zotero-formdb
      (relationship "sibling-integration")
      (repo "github.com/hyperpolymath/zotero-formdb")
      (status "planned")
      (description "Zotero plugin for FormDB. Reference management with
                    provenance tracking for academic research."))

    (fqldt
      (relationship "sibling-language")
      (repo "github.com/hyperpolymath/fqldt")
      (status "specification")
      (description "FQL with Dependent Types. Lean 4 integration for
                    compile-time query verification and proof-carrying migrations."))

    ;; External inspirations and comparisons
    (datomic
      (relationship "inspiration")
      (description "Immutable database with time-travel. FormDB shares the
                    immutability philosophy but adds provenance and narrative."))

    (arangodb
      (relationship "comparison")
      (description "Multi-model database (document + graph). FormDB is similar
                    but prioritizes auditability over performance."))

    (sqlite
      (relationship "comparison")
      (description "Embedded database. FormDB aims for similar simplicity
                    but with built-in versioning and provenance."))

    (event-sourcing
      (relationship "pattern-inspiration")
      (description "FormDB's journal is conceptually similar to event sourcing
                    but with first-class inverses and provenance."))

    (git
      (relationship "philosophy-inspiration")
      (description "Content-addressable, append-only, complete history.
                    FormDB applies git's philosophy to structured data.")))

  (target-domains
    (investigative-journalism
      "Track sources, verify claims, maintain evidence chains.
       Every fact has provenance. Retractions are reversible corrections.")

    (governance-and-policy
      "Audit trails for decisions. Who approved what, when, why.
       Regulation compliance with explainable constraints.")

    (agentic-ecosystems
      "AI agents need to explain their reasoning. FormDB provides
       the audit infrastructure for accountable AI systems.")

    (archives-and-preservation
      "Long-term data preservation with complete provenance.
       Future researchers can trace every change.")

    (scientific-research
      "Reproducibility requires knowing exactly what data existed when.
       Time-travel queries reconstruct historical states."))

  (what-this-is
    "A database engine that treats data history as sacred"
    "A query language (FQL) designed for provenance and narrative"
    "A storage format (blocks + journal) optimized for auditability"
    "A philosophy: databases should explain themselves"
    "An ecosystem of tools for narrative-first data management"
    "Open source under PMPL-1.0 (ethical open source)")

  (what-this-is-not
    "Not a drop-in SQL replacement (FQL is intentionally different)"
    "Not optimized for OLAP workloads (narrative overhead)"
    "Not a distributed database (single-node PoC, clustering planned)"
    "Not a real-time streaming platform (use CDC integration)"
    "Not a full-text search engine (integrate with Elasticsearch/Meilisearch)"
    "Not a time-series database (different access patterns)"
    "Not trying to be the fastest database (auditability > performance)"))

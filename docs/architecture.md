# Architecture

This document describes the medallion architecture of the Insurance DLT Pipeline.

## Diagram

```mermaid
flowchart LR
    A[Source<br/>insurance.csv<br/>Unity Catalog Volume] --> B[Bronze<br/>@dlt.table<br/>raw ingestion]
    B --> C[Silver<br/>@dlt.expect_or_drop<br/>cleansed + validated]
    C --> D[Gold<br/>dlt.apply_changes<br/>SCD / CDC]

    style A fill:#e8e8e8,stroke:#555,color:#000
    style B fill:#cd7f32,stroke:#7a4a1f,color:#fff
    style C fill:#c0c0c0,stroke:#666,color:#000
    style D fill:#ffd700,stroke:#a88a00,color:#000
```

## Layer Responsibilities

### Source
The raw `insurance.csv` is landed on a **Unity Catalog Volume**. Volumes provide governed,
catalog-level access to non-tabular files and remove the need for cluster-mounted storage.

### Bronze — Raw Ingestion
Defined with `@dlt.table`. The Bronze table reads the CSV with schema inference (or an explicit
schema once stable) and persists it as Delta. No business rules are applied here — Bronze is a
faithful, append-only copy of the source plus ingestion metadata (`_ingested_at`, `_source_file`).

### Silver — Cleansed & Validated
Built on top of Bronze. Data quality is enforced declaratively using DLT expectations:

| Tier              | Decorator                | Behaviour on violation                |
|-------------------|--------------------------|---------------------------------------|
| Warn              | `@dlt.expect`            | Record metric, keep the row           |
| Drop              | `@dlt.expect_or_drop`    | Drop the offending row                |
| Fail              | `@dlt.expect_or_fail`    | Abort the pipeline run                |

Silver applies type casting, normalisation (e.g. trimming, lowercasing categorical fields),
and any deduplication required before downstream modelling.

### Gold — CDC / SCD
Modelled tables for analytics. CDC is handled via `dlt.apply_changes`, which produces
SCD Type 1 or Type 2 outputs without hand-rolling `MERGE` statements. This layer is where
business-friendly column names, surrogate keys, and slowly-changing history live.

## Why declarative?

Unlike imperative PySpark — where the developer writes the driver script that orchestrates
each `read → transform → write` step — DLT computes the table dependency graph from the
references between `@dlt.table` definitions and orchestrates execution itself. This gives
free retries, incremental refresh, lineage, and observability, and it removes a large class
of operational concerns (job scheduling, dependency management, recovery from partial
failures) from the developer's responsibility.

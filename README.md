# Insurance DLT Pipeline

> 🚧 **Status: Actively in development — Bronze layer with DLT in progress (May 2026)**

End-to-end declarative Lakehouse pipeline using Delta Live Tables on Azure Databricks — insurance domain.

---

## Author

**Kumari Shishubala**
*Data Engineer | Databricks Certified Professional | London, UK*

🔗 [LinkedIn](https://linkedin.com/in/kumari-shishubala-b01b8b253)

---

## Architecture

```mermaid
flowchart LR
    A["Source<br>insurance.csv<br>Unity Catalog Volume"] --> B["Bronze<br>@dlt.table<br>raw ingestion"]
    B --> C["Silver<br>@dlt.expect_or_drop<br>cleansed + validated"]
    C --> D["Gold<br>dlt.apply_changes<br>SCD / CDC"]

    style A fill:#e8e8e8,stroke:#555,color:#000
    style B fill:#cd7f32,stroke:#7a4a1f,color:#fff
    style C fill:#c0c0c0,stroke:#666,color:#000
    style D fill:#ffd700,stroke:#a88a00,color:#000
```

This pipeline differs from an **imperative** PySpark pipeline because Delta Live Tables manages
orchestration, the dependency graph between tables, automatic retries on failure, and
incremental refresh — all derived from the declarative `@dlt.table` definitions. Instead of
writing a driver notebook that calls `read → transform → write` step-by-step, you describe
*what* each table should be, and DLT figures out *how and when* to materialise it.

---

## Why DLT?

- **Declarative authoring** — describe the target table; DLT plans the execution.
- **Native data quality expectations** with three enforcement tiers: `expect` (warn),
  `expect_or_drop` (drop bad rows), `expect_or_fail` (fail the pipeline).
- **Built-in lineage and observability** — table-level lineage, event log, and run metrics
  available out of the box, surfaced in the DLT UI and Unity Catalog.
- **Native CDC** via `dlt.apply_changes` — Type 1 / Type 2 SCD without hand-rolling MERGE logic.

---

## Tech Stack

- **Azure Databricks** — managed runtime
- **Delta Live Tables (DLT / Lakeflow)** — declarative pipeline framework
- **Delta Lake** — ACID storage layer
- **PySpark** — transformation engine
- **Unity Catalog** — governance, lineage, Volumes for source files
- **Python 3.11**

---

## Project Status

- [x] Repo skeleton and project scaffolding
- [ ] **Bronze layer (in progress)** — raw CSV ingestion via `@dlt.table` from a UC Volume
- [ ] Silver layer — cleansing with `@dlt.expect_or_drop` quality rules
- [ ] Gold layer — CDC via `dlt.apply_changes` (SCD Type 2)
- [ ] Unit tests (`pytest`) for transformation helpers
- [ ] CI workflow (lint + tests)

---

## What this demonstrates

- Declarative **DLT pipelines** instead of imperative Spark jobs
- Three-tier **data quality expectations** (`expect` / `expect_or_drop` / `expect_or_fail`)
- **Change Data Capture** using `dlt.apply_changes` for SCD handling
- **Unity Catalog** integration (Volumes for source data, three-level namespacing for tables)
- **Dependency graph auto-resolution** — DLT infers table order from references

---

## Related Project

See also: **NYC Taxi Medallion Pipeline** (imperative PySpark) — [github.com/Shishubala-01/nyc-taxi-medallion-pipeline](https://github.com/Shishubala-01/nyc-taxi-medallion-pipeline)

---

## Contact

📧 [kshishubala051@gmail.com](mailto:kshishubala051@gmail.com)
🔗 [linkedin.com/in/kumari-shishubala-b01b8b253](https://linkedin.com/in/kumari-shishubala-b01b8b253)

# Insurance DLT Pipeline

> ✅ **Status:** Complete — full DLT pipeline (Bronze, Silver, Gold with apply_changes for SCD Type 1 and Type 2) delivered May 2026.

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

## CDC Demonstration

The pipeline includes a synthetic CDC dataset (19 customer change events) to demonstrate:
- **SCD Type 1** (`gold_customer_current`): current state only, latest values per customer (10 rows — one per unique customer_id).
- **SCD Type 2** (`gold_customer_history`): full history with `__START_AT`/`__END_AT` validity windows (19 rows — one per change event, with closed and active versions).
- **Out-of-order arrival handling**: customer 10's events appear in the source file with the September UPDATE before the March INSERT. DLT's `sequence_by = "_change_timestamp"` correctly places March first in the SCD2 history regardless of file order — a critical resilience pattern for distributed CDC systems.

In production, change events would come from CDC tools like Debezium, AWS DMS, or Oracle GoldenGate; the synthetic data here demonstrates the same patterns.

---

## Why DLT?

- **Declarative authoring** — describe the target table; DLT plans the execution.
- **Native data quality expectations** with three enforcement tiers: `expect` (warn),
  `expect_or_drop` (drop bad rows), `expect_or_fail` (fail the pipeline).
- **Built-in lineage and observability** — table-level lineage, event log, and run metrics
  available out of the box, surfaced in the DLT UI and Unity Catalog.
- **Native CDC** via `dlt.apply_changes` — Type 1 / Type 2 SCD without hand-rolling MERGE logic.

---

## Business Insights Surfaced

- Risk-stratified pricing visible at the Silver level: high-risk band (60+ AND smoker, n=27) averages £40,630; medium (60+ OR smoker, n=334) averages £26,975.99; low (n=977) averages £7,828.88 — a 5.2× spread driven by the derived risk_band column.
- Source data quality: 0 negative charges, 0 invalid ages, 0 invalid children counts in the Kaggle dataset — confirming Kaggle's data curation. Five rows triggered the plausible_bmi warning (BMI outside the 10-60 range), retained for analysis rather than dropped.
- Region distribution intact: all 1,338 rows have one of the four valid regions, confirming the @dlt.expect_or_fail invariant on region_present.

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

| Layer  | Status         | Description                                                                                                                                                                                                       | Date     |
|--------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| Bronze | ✅ Complete     | DLT `@dlt.table` with Auto Loader (`cloudFiles`) ingestion. Three audit columns: `_ingestion_timestamp`, `_source_file` (UC-compliant via `_metadata.file_path`), `_source_system`. Runs as Lakeflow declarative pipeline. | May 2026 |
| Silver | ✅ Complete | DLT @dlt.expect three-tier expectations (warn/drop/fail). Five rules: plausible_bmi (warn), non_negative_charges + valid_age + valid_children_count (drop), region_present (fail). Light enrichment: risk_band derived from age + smoker. _silver_processed_timestamp audit column. | May 2026 |
| Gold   | ✅ Complete     | apply_changes for CDC. Two Gold tables: gold_customer_current (SCD Type 1, current state only) and gold_customer_history (SCD Type 2 with __START_AT/__END_AT validity windows). Synthetic CDC dataset with 19 events across 10 customers, including a deliberate out-of-order arrival test for sequence_by validation. | May 2026 |

Supporting work:
- [x] Repo skeleton and project scaffolding
- [ ] Unit tests (`pytest`) for transformation helpers
- [ ] CI workflow (lint + tests)

---

## Recent updates

- **May 2026:** Completed Gold layer with apply_changes for CDC. Built two parallel SCD outputs from the same source — gold_customer_current (Type 1) and gold_customer_history (Type 2) — proving that stored_as_scd_type alone determines retention behaviour. Tested out-of-order arrival scenario where September event arrives in the file before March event; sequence_by correctly placed March first in the SCD2 timeline.
- **May 2026:** Completed Silver DLT layer with three-tier expectations. Of 1,338 Bronze rows, 1,338 reached Silver (clean data — 0 drops, 5 plausible_bmi warnings, 0 fails). Surfaced first business insight: high-risk insureds (60+ smokers) average £40,630 in charges versus £7,828 for low-risk — a 5.2× pricing signal stratified by the derived risk_band column.
- **May 2026:** Completed Bronze DLT layer. Auto Loader ingestion of Kaggle insurance dataset (1,338 rows) into `workspace.default.bronze_insurance_claims` via declarative `@dlt.table`. Pipeline runs green on Databricks Free Edition with serverless compute in 39 seconds end-to-end.

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

## Lessons Learned

- **Unity Catalog migration:** `input_file_name()` is deprecated in UC-governed workspaces. The replacement is `_metadata.file_path`, which is a hidden struct column providing structured access to source-file attributes (path, size, modification time). A small but real migration step when moving from Hive metastore to Unity Catalog.
- **DLT expect tiers — observability vs action:** `@dlt.expect` (warn) tracks counts but doesn't filter or modify rows; bad rows still flow through to the output. To surface WHICH rows failed a warn rule, you need a parallel `@dlt.view` that filters to the failing rows or a quality flag column. `@dlt.expect_or_drop` filters; `@dlt.expect_or_fail` aborts the whole pipeline if any row fails — useful for invariants like null primary keys, but a single bad row kills the run, so use sparingly.
- **DLT apply_changes is a 2-step pattern:** declare an empty target with `dlt.create_streaming_table()`, then populate it with `dlt.apply_changes()`. The `@dlt.table` decorator alone doesn't work for CDC because apply_changes manages the table directly rather than returning a DataFrame.
- **`stored_as_scd_type` alone determines retention behaviour:** with the same source, keys, and sequence_by, changing this single parameter from 1 to 2 transforms the output from current-state to full history. This is the elegance of declarative CDC — one line decides whether you keep history or only the latest state.
- **DLT uses sequence_by for ordering, not the operation column:** during development I noticed that even when INSERT/UPDATE labels were inconsistent with timestamp ordering, the SCD2 output was still correct because DLT only uses sequence_by to sort events. The operation column is informational unless explicitly wired to apply_as_deletes. This is useful framework semantics — in real CDC pipelines, source operation labels serve downstream consumers (or explicit delete handling), but the timestamp drives the SCD logic itself.

---

## Future Enhancements

- **Refactor Auto Loader boilerplate** — extract a shared helper function for the common Bronze ingestion pattern (Auto Loader + audit columns) used across both flows. Currently duplicated between `bronze_insurance_claims` and `bronze_customer_changes`. Standard DRY refactor for a production pipeline.
- **Add apply_as_deletes for hard delete propagation** — extend the CDC flow to handle delete events from source. Currently the synthetic data only contains INSERT and UPDATE; adding `apply_as_deletes = "operation = 'DELETE'"` would propagate deletes to both Type 1 (row removed) and Type 2 (active row closed with __END_AT).
- **Threshold-based quality gating** — DLT's `expect_or_fail` aborts on any single failing row. For percentage-based fail thresholds (e.g., "fail only if more than 5% of rows fail this rule"), a separate quality-gate workflow querying the pipeline event log would be required. Genuinely a gap in DLT's native capabilities.
- **Databricks Asset Bundle deployment** — refactor this pipeline into a deployable bundle with multi-environment configuration (dev/staging/prod) and GitHub Actions CI/CD. Tracked in separate repo: databricks-asset-bundles-cicd. Target: 26 May 2026.

---

## Contact

📧 [kshishubala051@gmail.com](mailto:kshishubala051@gmail.com)
🔗 [linkedin.com/in/kumari-shishubala-b01b8b253](https://linkedin.com/in/kumari-shishubala-b01b8b253)

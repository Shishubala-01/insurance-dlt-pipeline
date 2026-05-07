"""
Bronze layer for the Insurance Claims pipeline (Lakeflow Declarative Pipelines / DLT).

This file is the SOURCE for a Lakeflow pipeline. It does not execute when you 
edit cells — it executes only when you click "Run pipeline" at the top right.

Each @dlt.table decorator declares a managed Delta table that the pipeline 
will materialise. Lakeflow auto-resolves dependencies, runs tables in order,
applies expectations, and tracks lineage and quality metrics in the pipeline UI.

Author: Kumari Shishubala
Project: Insurance DLT Pipeline
Layer: Bronze
"""

import dlt
from pyspark.sql.functions import current_timestamp, lit, col,when,expr
from pyspark.sql.types import (
    IntegerType, DoubleType, StringType, StructType, StructField
)

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

SOURCE_PATH    = "/Volumes/workspace/default/insurance_landing/"
SOURCE_SYSTEM  = "kaggle_insurance_v1"

# Define the source schema explicitly so DLT enforces it on read.
# Same principle as Project 1's Bronze: schema-on-read with explicit declaration.
INSURANCE_SCHEMA = StructType([
    StructField("age",      IntegerType(), nullable=True),
    StructField("sex",      StringType(),  nullable=True),
    StructField("bmi",      DoubleType(),  nullable=True),
    StructField("children", IntegerType(), nullable=True),
    StructField("smoker",   StringType(),  nullable=True),
    StructField("region",   StringType(),  nullable=True),
    StructField("charges",  DoubleType(),  nullable=True),
])


# ─────────────────────────────────────────────────────────────
# Bronze table — raw ingestion via Auto Loader
# ─────────────────────────────────────────────────────────────
# Auto Loader (format("cloudFiles")) handles incremental file discovery.
# If more CSVs land in the volume later, only new ones are processed on
# subsequent runs. The schema location stores Auto Loader's tracking state.
#
# Three audit metadata columns are added at ingestion (same pattern as
# Project 1 Bronze): when did the row land, what file did it come from,
# and what source system produced it.
#
# Note: We use _metadata.file_path (UC-compliant) rather than input_file_name()
# which is deprecated in Unity Catalog workspaces.
# ─────────────────────────────────────────────────────────────

@dlt.table(
    name="bronze_insurance_claims",
    comment=(
        "Bronze layer: raw insurance claims ingested from CSV via Auto Loader. "
        "Append-only, immutable. Source of truth for downstream Silver/Gold layers."
    ),
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def bronze_insurance_claims():
    """
    Reads insurance.csv from the Unity Catalog Volume using Auto Loader
    and adds three audit metadata columns. No business transformations
    happen here — Bronze is intentionally raw.
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/workspace/default/insurance_landing/_checkpoints/bronze")
            .option("header", "true")
            .schema(INSURANCE_SCHEMA)
            .load(SOURCE_PATH)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file",         col("_metadata.file_path"))
            .withColumn("_source_system",       lit(SOURCE_SYSTEM))
    )

# ─────────────────────────────────────────────────────────────
# Silver table — cleansed and enriched insurance claims
# ─────────────────────────────────────────────────────────────
# This Silver table demonstrates DLT's three-tier expectations
# framework — the declarative replacement for the manual
# quarantine pattern from Project 1.
#
# Five expectations spanning all three severity tiers:
#   - 1 warn   (@dlt.expect)         — track but don't filter
#   - 3 drop   (@dlt.expect_or_drop) — filter out clearly bad data
#   - 1 fail   (@dlt.expect_or_fail) — abort if critical violation
#
# Light enrichment: derived risk_band column from age + smoker.
# Audit metadata: _silver_processed_timestamp.
#
# Note: Plain @dlt.expect tracks counts only — failed rows pass
# through to the output. To see WHICH rows failed a warn rule,
# you'd add a parallel @dlt.view (production pattern, omitted
# here for clarity).
# ─────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_insurance_claims",
    comment=(
        "Silver layer: cleansed and enriched insurance claims. "
        "Applies five DLT expectations across warn, drop, and fail tiers. "
        "Adds risk_band enrichment derived from age + smoker."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
# Tier 1: warn — track but don't filter (BMI > 50 is unusual but possible)
@dlt.expect("plausible_bmi", "bmi BETWEEN 10 AND 60")

# Tier 2: drop — clearly invalid, filter out
@dlt.expect_or_drop("non_negative_charges",  "charges >= 0")
@dlt.expect_or_drop("valid_age",             "age >= 0 AND age <= 120")
@dlt.expect_or_drop("valid_children_count",  "children >= 0 AND children <= 10")

# Tier 3: fail — critical invariant; if violated, source is broken
@dlt.expect_or_fail("region_present", "region IS NOT NULL AND region <> ''")
def silver_insurance_claims():
    """
    Reads Bronze, applies expectations, adds risk_band enrichment,
    adds Silver audit metadata, returns a clean Silver DataFrame.
    """
    return (
        dlt.read_stream("bronze_insurance_claims")
        # Light enrichment — derive a risk_band column for downstream analytics
        .withColumn(
            "risk_band",
            when((col("age") >= 60) & (col("smoker") == "yes"), lit("high"))
            .when((col("age") >= 60) | (col("smoker") == "yes"), lit("medium"))
            .otherwise(lit("low"))
        )
        # Silver audit metadata — when this row was processed by Silver
        .withColumn("_silver_processed_timestamp", current_timestamp())
    )

# ─────────────────────────────────────────────────────────────
# CDC PIPELINE — Bronze → Silver → Gold (Type 1 + Type 2)
# ─────────────────────────────────────────────────────────────
# This second flow demonstrates DLT's apply_changes for CDC.
# It runs in the same pipeline as the main insurance flow above
# but ingests a separate synthetic CDC dataset and produces
# two Gold tables side-by-side: SCD Type 1 (current state only)
# and SCD Type 2 (full history with __START_AT/__END_AT).
#
# Note: The CDC dataset is synthetic for demonstration purposes.
# In production, change events would come from a CDC tool like
# Debezium, AWS DMS, or Oracle GoldenGate.
# ─────────────────────────────────────────────────────────────

CDC_SOURCE_PATH = "/Volumes/workspace/default/insurance_landing/cdc_events/"

# Schema for the CDC events
CDC_SCHEMA = StructType([
    StructField("customer_id",        IntegerType(),   nullable=False),
    StructField("age",                IntegerType(),   nullable=True),
    StructField("sex",                StringType(),    nullable=True),
    StructField("bmi",                DoubleType(),    nullable=True),
    StructField("children",           IntegerType(),   nullable=True),
    StructField("smoker",             StringType(),    nullable=True),
    StructField("region",             StringType(),    nullable=True),
    StructField("charges",            DoubleType(),    nullable=True),
    StructField("_change_timestamp",  StringType(),    nullable=False),  # CSV → string, cast in Silver
    StructField("operation",          StringType(),    nullable=False),
])


# ─────────────────────────────────────────────────────────────
# Bronze: ingest CDC events via Auto Loader
# ─────────────────────────────────────────────────────────────

@dlt.table(
    name="bronze_customer_changes",
    comment=(
        "Bronze layer for CDC: raw customer change events from synthetic CDC source. "
        "Each row represents an INSERT or UPDATE operation at a specific point in time."
    ),
    table_properties={"quality": "bronze"},
)
def bronze_customer_changes():
    """Reads synthetic CDC events from the volume using Auto Loader."""
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation",
                    "/Volumes/workspace/default/insurance_landing/_checkpoints/bronze_cdc")
            .option("header", "true")
            .schema(CDC_SCHEMA)
            .load(CDC_SOURCE_PATH)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file",         col("_metadata.file_path"))
            .withColumn("_source_system",       lit("synthetic_cdc_v1"))
    )


# ─────────────────────────────────────────────────────────────
# Silver: cast timestamp + light quality
# ─────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_customer_changes",
    comment=(
        "Silver layer for CDC: typed and validated change events. "
        "Cast _change_timestamp from string to timestamp for use in apply_changes."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("non_null_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_operation",      "operation IN ('INSERT', 'UPDATE', 'DELETE')")
def silver_customer_changes():
    """Casts _change_timestamp to a real timestamp type so apply_changes can sequence by it."""
    return (
        dlt.read_stream("bronze_customer_changes")
        .withColumn("_change_timestamp", col("_change_timestamp").cast("timestamp"))
    )


# ─────────────────────────────────────────────────────────────
# Gold: SCD Type 1 — current state only
# ─────────────────────────────────────────────────────────────
# apply_changes is a 2-step pattern in DLT:
#   1. Declare an empty target table with @dlt.table
#   2. Call dlt.apply_changes() to populate it via CDC logic
#
# DLT figures out inserts vs updates from the source events and
# applies them in sequence_by order, so out-of-order arrivals
# don't corrupt the final state.
# ─────────────────────────────────────────────────────────────

dlt.create_streaming_table(
    name="gold_customer_current",
    comment=(
        "Gold SCD Type 1: latest state per customer. "
        "Current values for each customer_id, with old values overwritten on each change. "
        "Use case: real-time dashboards, current premium pricing."
    ),
    table_properties={"quality": "gold"},
)

dlt.apply_changes(
    target            = "gold_customer_current",
    source            = "silver_customer_changes",
    keys              = ["customer_id"],
    sequence_by       = col("_change_timestamp"),
    stored_as_scd_type = 1,
    except_column_list = ["operation", "_ingestion_timestamp", "_source_file", "_source_system"],
)


# ─────────────────────────────────────────────────────────────
# Gold: SCD Type 2 — full history with __START_AT / __END_AT
# ─────────────────────────────────────────────────────────────

dlt.create_streaming_table(
    name="gold_customer_history",
    comment=(
        "Gold SCD Type 2: full change history per customer with validity windows. "
        "__START_AT and __END_AT columns auto-generated by DLT. "
        "Use case: audit, regulatory compliance, point-in-time queries, claim trend analysis."
    ),
    table_properties={"quality": "gold"},
)

dlt.apply_changes(
    target            = "gold_customer_history",
    source            = "silver_customer_changes",
    keys              = ["customer_id"],
    sequence_by       = col("_change_timestamp"),
    stored_as_scd_type = 2,
    except_column_list = ["operation", "_ingestion_timestamp", "_source_file", "_source_system"],
)
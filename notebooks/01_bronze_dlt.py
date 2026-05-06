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
from pyspark.sql.functions import current_timestamp, lit, col
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
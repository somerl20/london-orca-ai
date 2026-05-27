#!/usr/bin/env python3
"""
Query the measurements Delta Lake table.

Run from inside the worker container:
    docker exec -it london-orca-ai-worker-1 python3 scripts/query_measurements.py

Or mount and run:
    docker exec -it london-orca-ai-worker-1 \
        python3 /path/to/scripts/query_measurements.py
"""

import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

WAREHOUSE = os.environ.get("WAREHOUSE_PATH", "/data/warehouse")
TABLE     = f"{WAREHOUSE}/measurements"

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("query-measurements")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.enabled", "false")
).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load(TABLE)
df.createOrReplaceTempView("measurements")

print("\n=== schema ===")
df.printSchema()

print("=== row count ===")
print(f"  {df.count():,} rows\n")

print("=== sample (10 rows) ===")
spark.sql("""
    SELECT *
    FROM measurements
    ORDER BY event_ts DESC
    LIMIT 10
""").show(truncate=False)

print("=== rows per ship ===")
spark.sql("""
    SELECT source_id, COUNT(*) AS rows, MAX(event_ts) AS latest
    FROM measurements
    GROUP BY source_id
    ORDER BY source_id
""").show(truncate=False)

print("=== pipeline lag (event_ts → recorded_at) ===")
spark.sql("""
    SELECT
        source_id,
        ROUND(AVG(UNIX_TIMESTAMP(recorded_at) - UNIX_TIMESTAMP(event_ts)), 2) AS avg_lag_sec,
        ROUND(MAX(UNIX_TIMESTAMP(recorded_at) - UNIX_TIMESTAMP(event_ts)), 2) AS max_lag_sec
    FROM measurements
    GROUP BY source_id
    ORDER BY source_id
""").show(truncate=False)

spark.stop()

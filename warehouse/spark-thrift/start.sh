#!/bin/bash
set -e

WAREHOUSE=${WAREHOUSE_PATH:-/data/warehouse}

exec /opt/bitnami/spark/bin/spark-submit \
    --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.sql.warehouse.dir=${WAREHOUSE}" \
    --conf "spark.driver.memory=1g" \
    --hiveconf "hive.server2.thrift.port=10000" \
    --hiveconf "hive.server2.thrift.bind.host=0.0.0.0" \
    spark-internal

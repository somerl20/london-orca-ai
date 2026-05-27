#!/bin/bash
set -e

WAREHOUSE=${WAREHOUSE_PATH:-/data/warehouse}
BEELINE=/opt/spark/bin/beeline
JDBC="jdbc:hive2://localhost:10000"

/opt/spark/bin/spark-submit \
    --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.sql.warehouse.dir=${WAREHOUSE}" \
    --conf "spark.driver.memory=1g" \
    --hiveconf "hive.server2.thrift.port=10000" \
    --hiveconf "hive.server2.thrift.bind.host=0.0.0.0" \
    spark-internal &

SERVER_PID=$!

# Wait for the Thrift Server to accept connections (up to 120s)
echo "Waiting for Thrift Server on port 10000..."
for i in $(seq 1 60); do
    if $BEELINE -u "$JDBC" --silent=true -e "SELECT 1" >/dev/null 2>&1; then
        echo "Thrift Server is ready."
        break
    fi
    if [ $i -eq 60 ]; then
        echo "Thrift Server did not start in time." >&2
        exit 1
    fi
    sleep 2
done

# Register Delta tables so Metabase can query them by plain name.
$BEELINE -u "$JDBC" --silent=true <<'SQL'
CREATE TABLE IF NOT EXISTS measurements
USING DELTA
LOCATION '/data/warehouse/measurements';

CREATE TABLE IF NOT EXISTS processed_files
USING DELTA
LOCATION '/data/warehouse/_processed_files';
SQL

echo "Tables registered: measurements, processed_files"

# Keep container alive — exit only when the Spark server exits
wait $SERVER_PID

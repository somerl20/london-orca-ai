FROM metabase/metabase:latest
USER root
RUN apk add --no-cache libstdc++ gcompat
RUN mkdir -p /plugins && \
    curl -L -o /plugins/duckdb.metabase-driver.jar \
    "https://github.com/AlexR2D2/metabase_duckdb_driver/releases/latest/download/duckdb.metabase-driver.jar"

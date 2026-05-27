FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Download Delta Lake JARs at build time so they are available without internet at runtime.
RUN curl -fsSL -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -fsSL -o /opt/spark/jars/delta-storage-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

COPY start.sh /opt/start.sh
RUN chmod +x /opt/start.sh

ENTRYPOINT []
CMD ["/opt/start.sh"]

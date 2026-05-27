FROM bitnami/spark:3.5

USER root

# Download Delta Lake JARs into the Spark jars directory at build time
# so they are available without an internet connection at runtime.
RUN curl -fsSL -o /opt/bitnami/spark/jars/delta-spark_2.12-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -fsSL -o /opt/bitnami/spark/jars/delta-storage-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

COPY start.sh /opt/start.sh
RUN chmod +x /opt/start.sh

USER 1001

ENTRYPOINT []
CMD ["/opt/start.sh"]

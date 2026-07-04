FROM apache/kafka:4.0.2 AS kafka-base

FROM ibm-semeru-runtimes:open-17-jre

# Re-create the same non-root user that apache/kafka uses (uid/gid 1000, username esuser)
RUN groupadd --gid 1000 esgroup && \
    useradd  --uid 1000 --gid esgroup --no-create-home --shell /sbin/nologin esuser

# Copy the full Kafka Connect runtime from the build stage
COPY --from=kafka-base --chown=esuser:esgroup /opt/kafka /opt/kafka

# Copy the connector uber-jar into the plugins directory
COPY --chown=esuser:esgroup target/kafka-connect-*-jar-with-dependencies.jar /opt/kafka/plugins/

# Ensure the logs directory exists (plugins dir already exists from the COPY above)
RUN mkdir -p /opt/kafka/logs && chown esuser:esgroup /opt/kafka/logs

USER esuser

WORKDIR /opt/kafka

EXPOSE 8083

ENTRYPOINT ["./bin/connect-distributed.sh", "config/connect-distributed.properties"]

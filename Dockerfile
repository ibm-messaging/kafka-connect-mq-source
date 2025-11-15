FROM apache/kafka:4.0.0

COPY --chown=appuser:appuser target/kafka-connect-*-jar-with-dependencies.jar /opt/kafka/plugins/

COPY --chown=appuser:appuser target/kafka-connect-*-jar-with-dependencies.jar /opt/kafka/plugins/

RUN mkdir -p /opt/kafka/logs && chown -R appuser:appuser /opt/kafka/logs

WORKDIR /opt/kafka

EXPOSE 8083

ENTRYPOINT ["./bin/connect-distributed.sh", "config/connect-distributed.properties"]
FROM strimzi/kafka:latest-kafka-2.5.0 as builder

FROM ibmjava:8-jre

RUN addgroup --gid 5000 --system esgroup && \
    adduser --uid 5000 --ingroup esgroup --system esuser

COPY --chown=esuser:esgroup --from=builder /opt/kafka/bin/ /opt/kafka/bin/
COPY --chown=esuser:esgroup --from=builder /opt/kafka/libs/ /opt/kafka/libs/
COPY --chown=esuser:esgroup --from=builder /opt/kafka/config/connect-distributed.properties /opt/kafka/config/
COPY --chown=esuser:esgroup --from=builder /opt/kafka/config/connect-log4j.properties /opt/kafka/config/
RUN mkdir /opt/kafka/logs && chown esuser:esgroup /opt/kafka/logs
COPY --chown=esuser:esgroup target/kafka-connect-mq-source-1.3.0-jar-with-dependencies.jar /opt/kafka/libs/

WORKDIR /opt/kafka

EXPOSE 8083

USER esuser

ENTRYPOINT ["./bin/connect-distributed.sh", "config/connect-distributed.properties"]
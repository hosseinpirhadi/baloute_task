FROM maven:3.9.9-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests


FROM flink:2.1.1-scala_2.12

USER root

RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints && \
    chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints

USER flink

COPY --from=builder \
    /build/target/flink-kafka-top10-1.0-SNAPSHOT.jar \
    /opt/flink/usrlib/flink-kafka-top10.jar
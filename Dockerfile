# ---------- Build stage ----------
FROM maven:3.9.9-eclipse-temurin-17 AS builder

WORKDIR /build

COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests


# ---------- Flink runtime ----------
FROM flink:2.1.1-scala_2.12

# 1. سوییچ به کاربر root برای ایجاد پوشه‌ها و تغییر دسترسی
USER root

# 2. ساخت مسیرها و انتقال مالکیت به کاربر flink
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints && \
    chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints

# 3. بازگشت به کاربر استاندارد فلیم برای رعایت امنیت
USER flink

# 4. کپی کردن فایل jar
COPY --from=builder \
    /build/target/flink-kafka-top10-1.0-SNAPSHOT.jar \
    /opt/flink/usrlib/flink-kafka-top10.jar
# quay.io/debezium/connect ships only Debezium's own source connectors plus
# the Debezium JDBC sink -- confirmed by querying its /connector-plugins
# REST API against a running container, not assumed. plugin.path is scoped
# to /kafka/connect, so FileStreamSinkConnector (present on the general
# classpath at /kafka/libs/connect-file-*.jar, but Connect's isolated
# classloader mode ignores anything outside plugin.path) and any S3 sink
# need to be added explicitly.
FROM quay.io/debezium/connect:2.7

USER root

# FileStreamSinkConnector -- already inside the base image, just not on the
# scanned plugin path. Zero external download needed.
RUN mkdir -p /kafka/connect/connect-file \
    && cp /kafka/libs/connect-file-*.jar /kafka/connect/connect-file/

# Aiven's S3 sink connector -- not part of the base image at all. Verified
# (via `curl .../releases/latest`, then inspecting the jar's bytecode
# directly) that its S3 client is built with withPathStyleAccessEnabled and
# an explicit aws.s3.endpoint override, so it works against MinIO natively.
ARG S3_CONNECTOR_VERSION=2.15.0
RUN curl -sSL -o /tmp/s3-connector.tar \
      "https://github.com/Aiven-Open/s3-connector-for-apache-kafka/releases/download/v${S3_CONNECTOR_VERSION}/s3-connector-for-apache-kafka-${S3_CONNECTOR_VERSION}.tar" \
    && mkdir -p /kafka/connect/s3-connector-for-apache-kafka \
    && tar -xf /tmp/s3-connector.tar --strip-components=1 -C /kafka/connect/s3-connector-for-apache-kafka \
    && rm /tmp/s3-connector.tar

USER kafka

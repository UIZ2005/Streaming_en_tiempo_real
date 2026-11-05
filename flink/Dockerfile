
FROM flink:1.18.1-scala_2.12-java11

# 1) los TaskManager ejecutan nuestro código Python en procesos "worker"
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.18.1


ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar /opt/flink/lib/


RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    && mkdir -p /tmp/flink-checkpoints \
    && chown flink:flink /tmp/flink-checkpoints

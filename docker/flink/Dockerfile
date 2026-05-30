FROM flink:1.20.4-scala_2.12-java17

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 \
        python-is-python3 \
        python3-grpcio \
        python3-ruamel.yaml \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/flink/opt/python_lib \
    && python3 -m zipfile -e /opt/flink/opt/python/pyflink.zip /opt/flink/opt/python_lib \
    && python3 -c "import urllib.request; jars = {'hadoop-client-api-3.3.6.jar': 'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar', 'hadoop-client-runtime-3.3.6.jar': 'https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar'}; [urllib.request.urlretrieve(url, '/opt/flink/lib/' + name) for name, url in jars.items()]" \
    && chmod +x /opt/flink/opt/python_lib/pyflink/bin/*.sh \
    && chown -R flink:flink /opt/flink/opt/python_lib /opt/flink/lib/hadoop-client-*.jar

ENV PYFLINK_CLIENT_EXECUTABLE=python3 \
    PYFLINK_PYTHON=python3 \
    PYTHONPATH=/opt/flink/opt/python_lib:/opt/flink/opt/python/py4j-0.10.9.7-src.zip:/opt/flink/opt/python/cloudpickle-2.2.0-src.zip

USER flink

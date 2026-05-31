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
    && python3 -c "import urllib.request, hashlib, sys; jars = {'hadoop-client-api-3.3.6.jar': ('https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar', 'f3d2347a6e1c6885d5bcfd4f60c3ac3810ec11068fc161e04329baabf412d963'), 'hadoop-client-runtime-3.3.6.jar': ('https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar', '15f01bc804294df06d2effc87de363a83cf589f50558bdbf48f72541ad8de854')}; [urllib.request.urlretrieve(url, '/opt/flink/lib/' + name) for name, (url, sha) in jars.items()]; [sys.exit('Checksum mismatch for ' + name) for name, (url, sha) in jars.items() if hashlib.sha256(open('/opt/flink/lib/' + name, 'rb').read()).hexdigest() != sha]" \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/lib/ \
    && chmod +x /opt/flink/opt/python_lib/pyflink/bin/*.sh \
    && chown -R flink:flink /opt/flink/opt/python_lib /opt/flink/lib/hadoop-client-*.jar /opt/flink/lib/flink-s3-fs-hadoop-*.jar

ENV PYFLINK_CLIENT_EXECUTABLE=python3 \
    PYFLINK_PYTHON=python3 \
    PYTHONPATH=/opt/flink/opt/python_lib:/opt/flink/opt/python/py4j-0.10.9.7-src.zip:/opt/flink/opt/python/cloudpickle-2.2.0-src.zip

USER flink

FROM apache/iceberg-rest-fixture:1.10.1

ARG POSTGRES_JDBC_VERSION=42.7.4

USER root

ADD https://repo.maven.apache.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar /usr/lib/iceberg-rest/postgresql.jar

RUN chown iceberg:iceberg /usr/lib/iceberg-rest/postgresql.jar \
    && chmod 0644 /usr/lib/iceberg-rest/postgresql.jar

USER iceberg:iceberg

CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]

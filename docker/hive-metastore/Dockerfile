# Custom Hive Metastore image with hadoop-aws S3 support.
#
# The official apache/hive:4.2.0 image does NOT bundle hadoop-aws or
# aws-java-sdk-bundle, so HMS cannot validate or work with `s3a://` table
# locations out of the box. This Dockerfile adds the matching jars from
# Maven Central plus a metastore-site.xml configured for RustFS.
#
# Used by docker-compose.yml's `hive-metastore` service.
#
# Build: docker compose build hive-metastore
# Run:   docker compose up -d

FROM apache/hive:4.2.0

# Hive 4.2.0 ships with Hadoop 3.4.1. hadoop-aws 3.4.x uses AWS SDK v2
# (software.amazon.awssdk:bundle), NOT v1 (com.amazonaws:aws-java-sdk-bundle).
# Hadoop 3.4.x was tested against AWS SDK v2 2.24–2.29.x.
ARG HADOOP_VERSION=3.4.1
ARG AWS_SDK_V2_VERSION=2.29.52

USER root

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar
ADD https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar \
    /opt/hive/lib/aws-sdk-v2-bundle-${AWS_SDK_V2_VERSION}.jar

RUN chmod 644 /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar \
              /opt/hive/lib/aws-sdk-v2-bundle-${AWS_SDK_V2_VERSION}.jar

COPY metastore-site.xml /opt/hive/conf/metastore-site.xml

USER hive

# Create the embedded Derby database at build time, owned by `hive`. A fresh
# named volume mounted there (docker-compose.yml `hms-db`) is populated from
# the image, so it starts as a valid database the entrypoint's
# -initOrUpgradeSchema accepts. An empty pre-created directory is not enough:
# Derby refuses to create a database in a directory that already exists
# (ERROR XBM0J), and a volume on a path absent from the image is root-owned.
RUN cd /opt/hive && /opt/hive/bin/schematool -dbType derby -initSchema \
    && test -f /opt/hive/metastore_db/service.properties

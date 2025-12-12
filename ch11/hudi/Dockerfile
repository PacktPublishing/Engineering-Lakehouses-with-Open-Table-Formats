FROM jupyter/pyspark-notebook:x86_64-spark-3.5.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER jovyan

# Defaults (overridden by docker-compose env vars)
ENV KAFKA_BOOTSTRAP=kafka:29092
ENV KAFKA_TOPIC=globalmart.retail.products
ENV HUDI_BASE_PATH=/data/hudi/products_hudi
ENV CHECKPOINT_PATH=/data/checkpoints/products

# Pre-fetch Spark packages at build time via spark-submit (avoids needing pyspark import during build)
RUN /usr/local/spark/bin/spark-submit \
    --master local[1] \
    --conf spark.ui.enabled=false \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
    --class org.apache.spark.examples.SparkPi \
    /usr/local/spark/examples/jars/spark-examples_2.12-3.5.0.jar 1

# Promote the downloaded jars into a shared, version-pinned location and wire into classpath
USER root
RUN mkdir -p /opt/spark/jars-extra && cp /home/jovyan/.ivy2/jars/*.jar /opt/spark/jars-extra/
USER jovyan

# Use the local jars (no runtime downloads required)
ENV PYSPARK_SUBMIT_ARGS="--conf spark.driver.extraClassPath=/opt/spark/jars-extra/* --conf spark.executor.extraClassPath=/opt/spark/jars-extra/* --conf spark.serializer=org.apache.spark.serializer.KryoSerializer pyspark-shell"

FROM jupyter/pyspark-notebook:spark-3.4.1

USER root
RUN apt-get update && apt-get install -y openjdk-8-jdk
USER $NB_UID

RUN pip install delta-spark==2.4.0

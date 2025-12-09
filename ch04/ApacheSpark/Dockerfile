# Jupyter + Spark + Hadoop pre-built image
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.0

# Optional: install extra Python libs
USER root
RUN pip install --no-cache-dir pyspark findspark && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

USER ${NB_UID}

# Default working dir where notebooks live
WORKDIR /home/jovyan/work

# Optionally copy your notebook(s) into the image
# (you can also just mount them as a volume in docker-compose)
COPY ./notebooks ./notebooks

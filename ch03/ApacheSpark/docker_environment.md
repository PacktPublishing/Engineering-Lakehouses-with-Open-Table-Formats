## Create a `docker-compose.yaml` file in your local environment with the following:

```###########################################
# Iceberg - Spark Setup
###########################################

version: "3"

services:
  # Spark Notebook Server
  spark-iceberg:
    image: alexmerced/spark33-notebook
    container_name: spark-iceberg
    networks:
      iceberg-nessie-flink-net:
    depends_on:
      - storage-service
    volumes:
      - ./warehouse:/home/docker/warehouse
      - ./notebooks:/home/docker/notebooks
      - ./datasets:/home/docker/datasets
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - AWS_DEFAULT_REGION=us-east-1
    ports:
      - 8888:8888
      - 8080:8080
      - 10000:10000
      - 10001:10001
  # Minio Storage Server
  storage-service:
    image: minio/minio:RELEASE.2023-07-21T21-12-44Z
    container_name: storage-service
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=storage
      - MINIO_REGION_NAME=us-east-1
      - MINIO_REGION=us-east-1
    networks:
      iceberg-nessie-flink-net:
    ports:
      - 9003:9001
      - 9002:9000
    command: ["server", "/data", "--console-address", ":9001"]
  # Minio Client Container
  minio-client:
    depends_on:
      - storage-service
    image: minio/mc:RELEASE.2023-07-21T20-44-27Z
    container_name: minio-client
    networks:
      iceberg-nessie-flink-net:
        aliases:
          - minio.storage
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - AWS_DEFAULT_REGION=us-east-1
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc config host add minio http://storage-service:9000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/warehouse;
      /usr/bin/mc mb minio/warehouse;
      /usr/bin/mc mb minio/iceberg;
      /usr/bin/mc policy set public minio/warehouse;
      /usr/bin/mc policy set public minio/iceberg;
      tail -f /dev/null
      "
networks:
  iceberg-nessie-flink-net:
```
## This will generate an environment with:

- Minio on localhost:9003
- Jupyter Notebook w/ Spark on localhost:8888

## To Run the container:
- Navigate terminal to the same directory as the docker-compose.yaml
- run `docker-compose up`

After every service is up & running, upload the .ipynb file `Spark_Iceberg.ipynb` under `notebooks` folder in your environment and start running the code blocks!

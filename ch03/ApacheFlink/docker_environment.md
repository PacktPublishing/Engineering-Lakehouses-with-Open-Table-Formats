## Create a `docker-compose.yaml` file in your local environment with the following:

```###########################################
# Flink - Iceberg - DynamoDB Setup
###########################################

version: "3"

services:
  sql-client:
    build: ./sql-client
    image: sql-client
    networks:
      iceberg-dynamodb-flink-net:
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - AWS_DEFAULT_REGION=us-east-1
    stdin_open: true
    tty: true
  # Flink Job Manager
  flink-jobmanager:
    image: alexmerced/flink-iceberg:latest
    ports:
      - "8081:8081"
    command: jobmanager
    networks:
      iceberg-dynamodb-flink-net:
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - AWS_DEFAULT_REGION=us-east-1
      - S3_ENDPOINT=http://minio.storage:9000
      - S3_PATH_STYLE_ACCESS=true
    platform: linux/amd64
  # Flink Task Manager
  flink-taskmanager:
    image: alexmerced/flink-iceberg:latest
    depends_on:
      - flink-jobmanager
    command: taskmanager
    networks:
      iceberg-dynamodb-flink-net:
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 2
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - AWS_DEFAULT_REGION=us-east-1
      - S3_ENDPOINT=http://minio.storage:9000
      - S3_PATH_STYLE_ACCESS=true
    platform: linux/amd64
  
  #All of the following services are optional
  # Catalog
  dynamodb-local:
    command: "-jar DynamoDBLocal.jar -sharedDb -dbPath ./data"
    image: "amazon/dynamodb-local:latest"
    container_name: dynamodb-local
    networks:
      iceberg-dynamodb-flink-net:
    ports:
      - "8000:8000"
    volumes:
      - "./docker/dynamodb:/home/dynamodblocal/data"
    working_dir: /home/dynamodblocal
  # Minio Storage Server
  storage:
    image: minio/minio
    container_name: storage
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=storage
      - MINIO_REGION_NAME=us-east-1
      - MINIO_REGION=us-east-1
    networks:
      iceberg-dynamodb-flink-net:
    ports:
      - 9001:9001
      - 9000:9000
    command: ["server", "/data", "--console-address", ":9001"]
  # Minio Client Container
  mc:
    depends_on:
      - storage
    image: minio/mc
    container_name: mc
    networks:
      iceberg-dynamodb-flink-net:
        aliases:
          - minio.storage
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - AWS_DEFAULT_REGION=us-east-1
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc config host add minio http://storage:9000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/warehouse;
      /usr/bin/mc mb minio/warehouse;
      /usr/bin/mc mb minio/iceberg;
      /usr/bin/mc policy set public minio/warehouse;
      /usr/bin/mc policy set public minio/iceberg;
      tail -f /dev/null
      " 

networks:
  iceberg-dynamodb-flink-net:
```
## This will generate an environment with:

- Minio on `localhost:9001`
- DynamoDB catalog on `localhost:8000`
- Flink on `localhost:8081`

## To Run the container:
- Navigate terminal to the same directory as the `docker-compose.yaml`
- run `docker-compose up`

After every service is up & running, you can start the Flink SQL client using `docker compose exec sql-client bash -c "./bin/sql-client.sh"`

# Apache Flink with Delta Lake

This directory contains a Docker-based setup for running Apache Flink with Delta Lake connector support. This enables you to work with Delta tables using Flink SQL.

## Overview

This setup provides:
- **Apache Flink 1.18.0** cluster (JobManager + TaskManager)
- **Delta Lake connector** for Flink (delta-flink-3.2.1)
- **Local file system** storage for Delta tables
- **Flink SQL Client** for interactive queries

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB of available RAM
- Ports 8081 (Flink Web UI) available

## Directory Structure

```
ApacheFlink/
├── docker-compose.yaml    # Docker Compose configuration
├── jars/                  # Delta Lake and Flink connector JARs
├── scripts/               # SQL scripts for demonstrations
│   └── flink_demo.sql     # Example Delta Lake operations
└── warehouse/             # Local storage for Delta tables
```

## Quick Start

1. **Start the Flink cluster:**
   ```bash
   docker-compose up -d
   ```

2. **Access the Flink Web UI:**
   - Open http://localhost:8081 in your browser

3. **Start the Flink SQL Client:**
   ```bash
   docker-compose exec sql-client bash -c "./bin/sql-client.sh"
   ```

4. **Run the demo script:**
   ```bash
   docker-compose exec sql-client bash -c "./bin/sql-client.sh -f /scripts/flink_demo.sql"
   ```

## Services

The `docker-compose.yaml` defines three services:

### 1. `flink-jobmanager`
- Manages Flink cluster and coordinates jobs
- Web UI available at http://localhost:8081
- Exposes port 8081

### 2. `flink-taskmanager`
- Executes Flink tasks
- Configured with 2 task slots
- Automatically connects to JobManager

### 3. `sql-client`
- Interactive Flink SQL client container
- Pre-configured with Delta Lake connector JARs
- Mounts `scripts/` and `warehouse/` directories

## Delta Lake Connector Setup

The Delta Lake connector JARs are automatically mounted to `/opt/flink/lib/delta` in all containers:

- `delta-flink-3.2.1.jar` - Main Flink connector
- `delta-standalone_2.12-3.2.1.jar` - Delta Lake standalone library
- `delta-storage-3.2.1.jar` - Storage layer
- Additional dependencies (Hadoop, Jackson, etc.)

## Example Usage

### Creating a Delta Catalog

```sql
CREATE CATALOG delta_catalog WITH (
  'type' = 'delta-catalog',
  'catalog-type' = 'in-memory'
);

USE CATALOG delta_catalog;
```

### Creating a Delta Table

```sql
CREATE DATABASE IF NOT EXISTS customer_data;
USE customer_data;

CREATE TABLE customers (
  customer_id BIGINT,
  first_name  STRING,
  last_name   STRING,
  email       STRING,
  charges     FLOAT,
  state       STRING
)
PARTITIONED BY (state)
WITH (
  'connector' = 'delta',
  'table-path' = 'file:///warehouse/customers'
);
```

### Inserting Data

```sql
-- Insert mode (append)
INSERT INTO customers VALUES
  (1, 'John', 'Doe', 'john@example.com', 100.0, 'CA');

-- Overwrite mode
SET execution.runtime-mode = 'batch';
INSERT OVERWRITE customers VALUES
  (2, 'Jane', 'Smith', 'jane@example.com', 200.0, 'NY');
```

### Querying Data

```sql
SELECT * FROM customers;
```

## Demo Script

The `scripts/flink_demo.sql` file demonstrates:
1. Creating a Delta catalog
2. Creating a database and Delta table
3. Inserting data in append mode
4. Overwriting data in batch mode
5. Querying the Delta table

Run it with:
```bash
docker-compose exec sql-client bash -c "./bin/sql-client.sh -f /scripts/flink_demo.sql"
```

## Storage

Delta tables are stored in the `warehouse/` directory, which is mounted to `/warehouse` in all containers. This directory persists data between container restarts.

Example table structure:
```
warehouse/
└── customers/
    ├── _delta_log/          # Transaction log
    │   └── 00000000000000000000.json
    └── state=CA/            # Partitioned data
        └── part-*.parquet
```

## Runtime Modes

Flink supports both batch and streaming modes:

### Batch Mode
```sql
SET execution.runtime-mode = 'batch';
```
- Processes data in bounded datasets
- Supports `INSERT OVERWRITE` operations
- Better for ETL workloads

### Streaming Mode (default)
```sql
SET execution.runtime-mode = 'streaming';
```
- Processes unbounded data streams
- Supports continuous inserts
- Better for real-time workloads

## Troubleshooting

### Container won't start
- Check if port 8081 is already in use
- Ensure Docker has enough resources allocated
- Check logs: `docker-compose logs`

### SQL Client connection issues
- Verify JobManager is running: `docker-compose ps`
- Check JobManager logs: `docker-compose logs flink-jobmanager`
- Ensure network connectivity: `docker network ls`

### Delta connector not found
- Verify JARs are mounted: `docker-compose exec sql-client ls -la /opt/flink/lib/delta`
- Check JAR file permissions
- Restart containers: `docker-compose restart`

### Table path issues
- Ensure warehouse directory exists and is writable
- Use absolute paths: `file:///warehouse/table_name`
- Check volume mounts: `docker-compose exec sql-client ls -la /warehouse`

## Stopping the Cluster

```bash
# Stop containers (data persists)
docker-compose stop

# Stop and remove containers (data persists)
docker-compose down

# Stop and remove containers + volumes (deletes data)
docker-compose down -v
```

## Additional Resources

- [Apache Flink Documentation](https://flink.apache.org/docs/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Delta Flink Connector](https://github.com/delta-io/connectors/tree/master/flink)

## Notes

- The setup uses Flink 1.18.0 with Scala 2.12 and Java 11
- Delta Lake connector version: 3.2.1
- Tables are stored on the local file system (not S3/HDFS)
- For production use, consider configuring S3 or HDFS storage


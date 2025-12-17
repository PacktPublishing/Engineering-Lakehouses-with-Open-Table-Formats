# Chapter 11: Real-World Lakehouse Use Cases

## Hudi CDC demo (GlobalMart)

Folder: `ch11/hudi`

What's inside:
- `docker-compose.yml` / `Dockerfile`: spin up Zookeeper, Kafka, MySQL (seeded), Debezium Connect, and a Jupyter+Spark image with Kafka+Hudi jars cached. Includes a `kafka-init` helper to create the CDC topic.
- `notebooks/ch11_globalmart_hudi.ipynb`: two demo paths  
  - Quick demo (no Kafka): seed/batch write → snapshot & changelog validators → CDC validation summary.  
  - Full CDC: streaming read from Kafka → optional CDC pump → snapshot/changelog/validation.
- `scripts/pump_mysql_changes.sh`: emits CDC sequences (product 201: insert→update→delete→reinsert; product 202: insert→update).
- `connect/connectors/*` + `scripts/register_connector.sh`: Debezium MySQL connector and auto-registration script.
- `.gitignore`: ignores local `data/`, `tmp/`, and notebook checkpoints.

How to run:
1) `cd ch11/hudi && docker compose up --build -d`
2) Open Jupyter: http://localhost:8888
3) Quick demo: run seed (if empty) → optional batch demo → snapshot/changelog/validation cells.
4) Full CDC: run the streaming cell (~60s), in another terminal run `docker compose exec mysql-primary bash /scripts/pump_mysql_changes.sh`, then snapshot/changelog/validation.

## Iceberg demo (Acme Manufacturing)

Folder: `ch11/iceberg`

What's inside:
- `dremio-iceberg-nessie.sql`: SQL to configure Dremio with Nessie + MinIO for Iceberg.
- `README.md`: includes a docker-compose snippet for Nessie, MinIO, and Dremio to back Iceberg tables.

How to run (Nessie/MinIO/Dremio):
1) Use the docker-compose snippet in `ch11/iceberg/README.md` to start Nessie (catalog), MinIO (storage), and Dremio.
2) Apply the SQL in `dremio-iceberg-nessie.sql` inside Dremio to register the catalog/bucket.
3) Explore Iceberg tables via Dremio UI; adapt the notebook for ML workflow context if desired.

## Delta Lake demo (Visionary Telecom)

Folder: `ch11/deltalake`

What's inside:
- `Visionary_Telecom_Delta_MLFlow.ipynb`: notebook showing a lakehouse ML workflow (Delta/MLflow) alongside Iceberg scenario notes.

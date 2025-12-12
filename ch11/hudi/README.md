# GlobalMart (Apache Hudi CDC) demo environment

This stack spins up MySQL (with seed data), Kafka, Debezium Connect, and a Jupyter+Spark image preloaded with the Kafka + Hudi Spark packages.

## Quick start (fastest path)
1) Start the stack (detached so you keep your terminal free):
   ```bash
   cd ch11/hudi
   docker compose up --build -d
   ```
   - Watch logs if you want: `docker compose logs -f --tail=200`
2) Open JupyterLab: http://localhost:8888
3) In the notebook `notebooks/ch11_globalmart_hudi.ipynb`, run the “Quick seed if table is empty” cell (it writes a few demo rows to Hudi). Then run the snapshot/changelog cells to see data immediately.
4) Optional: to simulate CDC via Debezium/Kafka, run this in another terminal after the stack is up:
   ```bash
   docker compose exec mysql-primary bash /scripts/pump_mysql_changes.sh
   ```
   Then run the streaming cell for ~30 seconds, interrupt it, and run the snapshot/changelog cells to see the CDC events reflected.
5) The notebook now has two clear paths:
   - **Quick demo (no Kafka):** run seed (if empty) → optional batch demo → snapshot/changelog validators.
   - **Full CDC:** configure/run the streaming cell (auto-stops after ~60s), optionally run the pump script above to emit CDC, then snapshot/changelog validators.

Services:
- JupyterLab: http://localhost:8888 (no token/password; notebook folder mounted at `./notebooks`)
- Kafka: localhost:9092 (inside network: kafka:29092)
- Kafka Connect REST: http://localhost:8083
- MySQL: localhost:3306 (`root/rootpw`, `debezium/dbz_password`, db `retail`)

## What the stack does
- Seeds MySQL with `retail.products` (`./mysql/init/01_init.sql`).
- Starts Kafka Connect and auto-registers the Debezium MySQL connector via `scripts/register_connector.sh` using `connect-init`.
- Writes Debezium change events to topic `globalmart.retail.products` (schemaless JSON).
- Mounts `./data` into the notebook container for Hudi table + checkpoints.
- Ensures the Kafka topic `globalmart.retail.products` exists at startup (`kafka-init` service).
 - Configures Spark to use Kryo serialization (required by Hudi) in the notebook container.

## Notebook to run
Open `notebooks/ch11_globalmart_hudi.ipynb` inside JupyterLab and run the cells:
- The Spark image already has `spark-sql-kafka` and `hudi-spark3.5` cached at build time, so the SparkSession should start offline.
- The notebook reads from the Kafka topic above and writes to `/data/hudi/products_hudi` with checkpoints at `/data/checkpoints/products`.

## Tips / troubleshooting
- If you restart frequently, `docker compose down -v` will reset Kafka/MySQL state.
- The MySQL container may log tzdata warnings; they are benign for this demo.
- If the connector already exists, the registration script will log a 409 but continue.
- If you see Jupyter kernel 404s after rebuilding/restarting the container, refresh the browser and start a new kernel for the notebook (old kernel IDs from previous runs are invalid). Clearing cookies for `127.0.0.1`/`localhost` can help if you see “invalid/expired login cookie” warnings.
- If the streaming write fails with `UnknownTopicOrPartitionException`, wait a few seconds for the Debezium connector to be registered and the topic to appear, then rerun the cell. The `kafka-init` service also creates the topic on startup to avoid this.
  - Ensure you are using a fresh kernel after (re)build so it picks up the latest docker-compose changes (including `kafka-init`).
  - If it persists, ensure the stack was started with the updated compose (topic creator waits up to ~60s), then restart the kernel and rerun the streaming cell.
  - You can also manually create/verify the topic from the host:  
    `docker compose exec kafka kafka-topics --create --if-not-exists --topic globalmart.retail.products --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1`  
    `docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list`  
    `docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic globalmart.retail.products`
  - If `kafka-init` shows `sleep: missing operand`, pull latest compose and restart; the command is now passed as a single string to avoid parsing issues on Windows hosts.
- To generate demo CDC activity quickly (insert -> update -> delete), run:  
  `docker compose exec mysql-primary bash /scripts/pump_mysql_changes.sh`  
  Then rerun the streaming cell in the notebook to see Hudi ingest those events. By default the script inserts + updates product_id=201 and leaves it present (so snapshot shows a row). To test deletes, uncomment the DELETE line in the script and rerun.
- Full CDC path (Debezium → Kafka → Spark Structured Streaming → Hudi):
  1) In Jupyter, start the streaming cell (it will keep running).
  2) In a separate terminal, run: `docker compose exec mysql-primary bash /scripts/pump_mysql_changes.sh` to emit insert+update (and optionally delete) events.
  3) Let the stream run for ~30s so Spark ingests the topic.
  4) Interrupt the streaming cell, then run the snapshot and changelog cells. You should see product_id=201 reflected (and a delete if you uncommented it).
  5) If you don’t see data, ensure the topic exists (`docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list`) and rerun the pump/stream steps.

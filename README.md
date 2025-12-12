# Engineering Lakehouses with Open Table Formats Code Samples

# Contents
## Chapter 1: Open Data Lakehouse – a New Architectural Paradigm
## Chapter 2: Transactional Capabilities in Lakehouse
## Chapter 3: Apache Iceberg Deep Dive
## Chapter 4: Apache Hudi Deep Dive
## Chapter 5: Delta Lake Deep Dive
## Chapter 6: Catalogs and Metadata Management
## Chapter 7: Interoperability and Data Federation
## Chapter 8: Performance Optimization and Tuning
## Chapter 9: Data Governance and Security in Lakehouse
## Chapter 10: Decisions on Open Table Formats
## Chapter 11: Real-World Lakehouse Use Cases

---

## Chapter 11 (Hudi CDC) quick map

Folder: `ch11/hudi`

What’s inside:
- `docker-compose.yml` / `Dockerfile`: spin up Zookeeper, Kafka, MySQL (seeded), Debezium Connect, and a Jupyter+Spark image with Kafka+Hudi jars cached. Includes a `kafka-init` helper to create the CDC topic.
- `notebooks/ch11_globalmart_hudi.ipynb`: two demo paths  
  - Quick demo (no Kafka): seed/batch write → snapshot & changelog validators → CDC validation summary.  
  - Full CDC: streaming read from Kafka → optional CDC pump → snapshot/changelog/validation.
- `scripts/pump_mysql_changes.sh`: emits CDC sequences (product 201: insert→update→delete→reinsert; product 202: insert→update).
- `connect/connectors/*` + `scripts/register_connector.sh`: Debezium MySQL connector and auto-registration script.

How to run:
1) `cd ch11/hudi && docker compose up --build -d`
2) Open Jupyter: http://localhost:8888
3) Quick demo: run seed (if empty) → optional batch demo → snapshot/changelog/validation cells.
4) Full CDC: run the streaming cell (~60s), in another terminal run `docker compose exec mysql-primary bash /scripts/pump_mysql_changes.sh`, then snapshot/changelog/validation.

# Authors/Contributors
* Dipankar Mazumdar
* Vinoth Govindarajan

# Publisher
[Packt Publishing](https://www.packtpub.com/en-us/help/about)

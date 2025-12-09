# Chapter 4: Apache Hudi Deep Dive

## Quickstart: Jupyter/Spark/Hudi environment

Start the full environment and explore Hudi with this chapter's runnable demo notebook.

----

## How to Run

```bash
cd ch04/ApacheSpark
# Build and start the environment
docker compose up -d
# Access Jupyter at http://localhost:8888 (token is in docker logs hudi-spark-notebook)
```
Your notebook is in `notebooks/Hudi_Deep_Dive.ipynb` under `ApacheSpark/`.

If you want to extend the image, see `Dockerfile` and `docker-compose.yml` in `ApacheSpark/` for more options (extra pip dependencies, etc).

----

## Organization & Code Structure

- Main demo notebook (Spark+SQL): `ApacheSpark/notebooks/Hudi_Deep_Dive.ipynb`
- **Spark notebooks/env/scripts:** in `ApacheSpark/` (plus Docker/Jupyter, config, markdown)
- **Flink SQL/scripts/configs:** in `ApacheFlink/` (plus Flink SQL examples, docker env, markdown)
- Top-level Docker/Jupyter launch: see Dockerfile and docker-compose.yml now inside `ApacheSpark/`

This structure matches the pattern from Chapters 3 and 5 for easy comparison across lakehouse engines!

----

## About This Chapter
- Jupyter+Spark+Hudi demo notebook: `ApacheSpark/notebooks/Hudi_Deep_Dive.ipynb`
- Dockerfile: Extends jupyter/pyspark-notebook with minimal config, see `ApacheSpark/Dockerfile`
- docker-compose.yml: Launches an interactive notebook server with Hudi configs for Spark 3.5, see `ApacheSpark/docker-compose.yml`
- Flink + Hudi SQL snippets: see Section 4 in the notebook or `ApacheFlink/flink_hudi_examples.sql` for shell-only code
- Extra scripts, config, or documentation are organized by engine in their respective subfolders for future extension.
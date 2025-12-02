# Chapter 5: Delta Lake Deep Dive

This chapter demonstrates **Delta Lake** with Apache Spark in a completely hands-on, reproducible, beginner-focused way.

---
## 📂 Notebook & Data Folder Structure

- All main exercises are in:
  `ch05/ApacheSpark/notebooks/DeltaLake_Spark.ipynb`
- **Do not track notebook outputs or table data in git!** All Spark/Delta tables are managed in:
  - `ch05/ApacheSpark/notebooks/spark-warehouse/`
- Jupyter workspace for Docker Compose lives in `notebooks/`. Use this as your starting launcher wherever possible.
- Data/checkpoint/temporary folders like `minio-data/`, `spark-warehouse/`, { }/tmp/, etc. are git-ignored and automatically created as needed by Spark/Delta/Minio.

---

## 📚 Compare Notebooks & Quick Navigation

**Not sure which hands-on Delta Lake notebook to use?**
- [DeltaLake_Spark.ipynb](./ApacheSpark/notebooks/DeltaLake_Spark.ipynb): "Classic" Spark Delta walkthrough. Best for a quick, linear tour, or if you prefer minimal assertion checks and direct scripting. [See also: Deep Dive version](./ApacheSpark/notebooks/Delta_Lake_Deep_Dive.ipynb)
- [Delta_Lake_Deep_Dive.ipynb](./ApacheSpark/notebooks/Delta_Lake_Deep_Dive.ipynb): "Step-by-step"/assert-based lab. Great for validating your understanding, learning Delta operations with feedback after every step, or for troubleshooting. [See also: Classic version](./ApacheSpark/notebooks/DeltaLake_Spark.ipynb)

**Tip:** You can open both side by side in Jupyter to compare narrative, see alternative demo patterns, or jump between code styles as needed.

## 🚀 Quickstart

1. **Recommended:** Use Docker Compose as described in [`ApacheSpark/docker_environment.md`](./ApacheSpark/docker_environment.md) for a fully automated environment (Spark + Delta + Jupyter + Minio):
    ```bash
    docker-compose up  # from ch05/ApacheSpark/
    ```
    - Jupyter runs at [http://localhost:8888/](http://localhost:8888/) (no password)
    - Minio S3 UI at [http://localhost:9001/](http://localhost:9001/) (admin/password)
    - Notebooks live in `/notebooks/`, all managed tables go in the git-ignored `spark-warehouse/`
2. Open and run one or both:
    - [`DeltaLake_Spark.ipynb`](./ApacheSpark/notebooks/DeltaLake_Spark.ipynb)
    - [`Delta_Lake_Deep_Dive.ipynb`](./ApacheSpark/notebooks/Delta_Lake_Deep_Dive.ipynb)
    - **See also:** Each notebook cross-links to the other for feature and style comparison.
3. For local: install Python 3.8+, Java 8+, `pip install pyspark==3.4.1 delta-spark==2.4.0 findspark`, and run either notebook in your UI.

## 📔 Available Notebooks

- [`DeltaLake_Spark.ipynb`](./ApacheSpark/notebooks/DeltaLake_Spark.ipynb) (Classic / Linear):
    - Best for quick runs, straightforward code reference, and seeing standard Delta Lake usage patterns.
    - [Jump to: Deep Dive notebook for checked/validated workflow.](./ApacheSpark/notebooks/Delta_Lake_Deep_Dive.ipynb)
- [`Delta_Lake_Deep_Dive.ipynb`](./ApacheSpark/notebooks/Delta_Lake_Deep_Dive.ipynb) (Step-by-step/assert-based):
    - Every section checks its outputs with assertions for robust learning and fast failure discovery.
    - [Jump to: Classic Spark notebook for minimal setup, fastest navigation.](./ApacheSpark/notebooks/DeltaLake_Spark.ipynb)

---
## 📝 What You’ll Learn

*All topics below are covered in both labs; Deep Dive offers step-by-step checks/validations. Use Classic Spark for a fast, linear demo.*
- Delta Lake table creation (SQL + DataFrame API)
- Batch and streaming data flows (inserts/appends/queries)
- Update, Delete, Upsert (MERGE INTO demo)
- Schema evolution: add, drop, rename columns
- Safe time travel using table version/history
- Delta Lake table management (HISTORY, VACUUM, OPTIMIZE, etc.)
- Change Data Feed (CDC) usage where supported

---
## 🔀 See Also: Notebook Quick Comparison

- [`DeltaLake_Spark.ipynb`](./ApacheSpark/notebooks/DeltaLake_Spark.ipynb): "Linear" experience. Best for instructors, demo speed, and easy code scanning.
- [`Delta_Lake_Deep_Dive.ipynb`](./ApacheSpark/notebooks/Delta_Lake_Deep_Dive.ipynb): "Step by step"/check-after-each-op mode. Best for self-study, troubleshooting, and notebook-based evaluation.

Jump between them any time; both cover all core Delta features, but Deep Dive will assert and catch errors after every major step. [Compare source on GitHub](./ApacheSpark/notebooks/) for full details.

---
## ⚠️ Common Pitfalls & Advice

- **Do not manually add, remove, or alter files in `spark-warehouse/` or `/tmp/`**
- Run notebook cells sequentially; out-of-order execution may lead to "column not found" or analysis exceptions (e.g. after renames or schema changes)
- Always check your table version history with `dt.history().show()` before using time travel/CDC features
- **If you want to reset for a clean run:** Stop Spark, delete the entire `spark-warehouse/` and `/tmp/delta_*` folders, then re-start Jupyter and rerun the notebook from the top.
- Make sure all of your team is using the same Delta/Spark versions for collaborative labs

---

## 🔄 Resetting the Lab (for beginners)

If you hit a problem with mismatched schema, missing columns, or want to start over:
1. Shut down your Jupyter server (in Docker, use Ctrl+C or `docker-compose down`)
2. Delete the managed table data: `rm -rf notebooks/spark-warehouse/ /tmp/delta_*`
3. Restart Jupyter and rerun the notebook from the very top, running every cell in order.

*This ensures you always have a clean, reproducible hands-on lab experience.*

---
See in-notebook comments and markdown for more Delta/Spark tips and common error fixups!
# Delta Lake on Spark + Minio Docker Environment

## Quickstart

- All Jupyter work is done in `/notebooks/`. **You have two hands-on Delta Lake notebooks:**
    - [`DeltaLake_Spark.ipynb`](./notebooks/DeltaLake_Spark.ipynb): Classic linear walkthrough. Fastest for demos, lectures, or anyone wanting to skim/scan code.
    - [`Delta_Lake_Deep_Dive.ipynb`](./notebooks/Delta_Lake_Deep_Dive.ipynb): Lab-style, assert-based notebook for testable, self-paced learning. Every major step checks its output, making it easy to spot mistakes and learn correct patterns.
- Both are runnable in Docker Compose; you can open them side by side for comparison if desired. Each notebook links to the other for easy reference.
- All managed tables are created automatically under `notebooks/spark-warehouse/` and are NOT tracked in git.

### To launch the environment:
1. From this directory, run:
   ```bash
   docker-compose up # from ch05/ApacheSpark/
   ```
2. Open your browser to [http://localhost:8888/](http://localhost:8888/)
   - The Jupyter workspace will be `/notebooks/` (home to the main notebook and your work).
   - Managed tables go to `notebooks/spark-warehouse/` (no action needed).
3. Open either (or both!) [DeltaLake_Spark.ipynb](./notebooks/DeltaLake_Spark.ipynb) or [Delta_Lake_Deep_Dive.ipynb](./notebooks/Delta_Lake_Deep_Dive.ipynb) from `/notebooks/` and run each cell as instructed. Both work as local, file-based exercises in this Docker Compose setup.

#### Notes:
- Do **not** manually upload, move, or check in warehouse/ or table data files.
- To restart or reset your lab: shut down Docker, delete all contents of notebooks/spark-warehouse/ and /tmp/Deltas created during play, then start up and rerun the notebook from the top. See the main README for more.
- All Spark, Delta, and streaming/CDC features demonstrated will automatically save data to the correct locations.

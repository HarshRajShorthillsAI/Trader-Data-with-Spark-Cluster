# 🚀 Spark-based ETL Pipeline using Docker Compose

This project implements a scalable, modular ETL (Extract, Transform, Load) pipeline using **Apache Spark** and **PySpark**, orchestrated through **Docker Compose**. It is designed to process large `.tsv.gz` files efficiently using distributed computing principles, while maintaining development flexibility via host-driven execution.

---

## 📌 Project Goals

- Build a **containerized Spark cluster** (Master + Workers) using Docker
- Develop a **PySpark-based ETL pipeline** with modular code components
- Enable the host machine to act as the **driver**, while containers serve as **executors**
- Utilize **bind mounts** to ensure data accessibility inside containers
- Address and document issues related to **path consistency** and **symbolic linking**

---

## 🧱 Architecture Overview

### 🔧 Components

| Component         | Description |
|------------------|-------------|
| `spark-master`    | Runs Spark master process, listens at `spark://localhost:7077` |
| `spark-worker-1`  | Spark executor container connected to master |
| `spark-worker-2`  | Another worker container for parallelism |
| `spark-submit`    | Optional utility container for in-cluster job submission |
| `main.py` (host)  | Entry-point Python script acting as the PySpark driver |
| `data/`           | Host directory with `.tsv.gz` files (bind-mounted to `/data`) |

### 📡 Communication Flow

1. `main.py` runs **on host**, initializing a `SparkSession` targeting `spark://localhost:7077`
2. Spark **master** container accepts the job and distributes tasks to workers
3. All containers access `.tsv.gz` files via the mounted `/data` directory
4. **Transformations and actions** are applied inside containerized workers
5. Results (e.g., filtered data, counts) are returned to the host driver

---

## 🗂️ Project Structure

```
.
├── docker-compose.yml
├── data/                             # Host directory for input .tsv.gz files
├── spark/
│   ├── app/                          # Additional scripts (if any)
│   └── etl/                          # ETL pipeline implementation
│       ├── main.py                   # Entry point script
│       ├── etl_pipeline.py           # Coordinates all ETL tasks
│       ├── read_data.py              # Handles data ingestion
│       └── transform_data.py         # Applies domain-specific transformations
```

---

## ⚙️ How to Run

### 1. Start the Spark Cluster

```bash
docker-compose up -d
```

This launches:
- Spark master on `localhost:7077`
- Two Spark workers connected via `spark-network`
- All containers have access to `/data` via bind mount

### 2. Prepare Data

Place your `.tsv.gz` files inside the `data/` directory on your host machine:

```bash
mkdir -p data/
mv *.tsv.gz data/
```

### 3. (Optional) Create Symlink (if needed)

If your PySpark script requires `/data` to exist as an absolute path:

```bash
sudo ln -s /home/your_user/Training/Spark-cluster/data /data
```

> This step depends on how paths are interpreted in your code and container runtime.

### 4. Execute the ETL Pipeline from Host

```bash
python3 spark/etl/main.py
```

The script:
- Initializes a Spark session connected to the containerized cluster
- Loads `.tsv.gz` files from `/data`
- Applies various ETL transformations
- Prints sample outputs, row counts, and schema information

---

## 🔍 ETL Pipeline Breakdown

The pipeline performs the following tasks:

| Task | Description |
|------|-------------|
| `task1()` | Loads all `.tsv.gz` files into a single DataFrame |
| `task2()` | Filters rows by `post_event_id`, constructs product list |
| `task3()` | Counts ad impressions by dealer/product |
| `task4()` | Aggregates impression data by relevant grouping keys |
| `task5()` | Extracts product models using event variable `eVar117` |

### Example Code Snippet

```python
from etl_pipeline import ETLPipeline

pipeline = ETLPipeline('/data')
pipeline.task5()
```

---

## 💡 Design Rationale

### Why Run Driver on Host?

- Offers **debugging ease** and rapid iteration without rebuilding containers
- Connects seamlessly to the Spark master via exposed ports
- Leverages bind-mounted data for minimal I/O overhead

### Why Bind Mount?

- Allows sharing actual files (not copies) between host and containers
- Ensures consistency in file access across all services
- Critical for large datasets (e.g., compressed TSVs)

### Why Symlink?

- Some PySpark logic expects a consistent absolute path (`/data`)
- If the project directory differs between host and container, symbolic links resolve the discrepancy

---

## 🧠 Technologies Used

- **Apache Spark** (via `bitnami/spark`)
- **PySpark** for distributed ETL
- **Docker & Docker Compose** for orchestration
- **GZip + TSV** for data compression and storage
- **UNIX filesystem tools** (`ln`, volume mounts)

---

## 📌 Requirements

- Python 3.8+
- Docker & Docker Compose
- `pyspark` installed on host (if running driver locally)
- `.tsv.gz` files placed in `data/` directory

---

## 📈 Sample Output

The pipeline prints:
- Data preview with `.take(1)`
- Row counts and schema after each stage
- Informative logs for tracking transformations

---

## 📄 License

This project is open-source under the [MIT License](LICENSE), unless otherwise specified.

---

Let me know if you'd like to add badges (e.g., Docker, Spark, Python), CI/CD hooks, or academic citation format.
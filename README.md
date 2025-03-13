# ETL Pipeline in Databricks

## 📌 Overview
This project implements a simple ETL (Extract, Transform, Load) pipeline in **Databricks** using **PySpark**. The pipeline follows the **Medallion Architecture**:
- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Data cleaning and transformation
- **Gold Layer**: Aggregated and ready-for-use data

## 📂 Project Structure
```
│── etl/
│   ├── ingestion/
│   │   ├── bronze_ingestion.py
│   ├── transformation/
│   │   ├── silver_transformation.py
│   ├── aggregation/
│   │   ├── gold_aggregation.py
│── scripts/
│   ├── run_etl.py   <-- Runs the entire ETL pipeline
│── config/
│   ├── settings.py  <-- Configuration settings
│── tests/
│   ├── test_silver.py
│── README.md
```

## 🚀 How to Run
### 1️⃣ Run Manually Using `run_etl.py`
If running locally or in Databricks Notebooks:
```bash
python3 scripts/run_etl.py
```
This will:
1. Load data into the **Bronze** layer
2. Clean and transform it into the **Silver** layer
3. Aggregate and store results in the **Gold** layer

### 2️⃣ Run Using Databricks Workflows (Recommended)
1. Create **3 Workflows** in Databricks:
   - **Job 1** → Runs `bronze_ingestion.py`
   - **Job 2** → Runs `silver_transformation.py`
   - **Job 3** → Runs `gold_aggregation.py`
2. **Set Dependencies** so that:
   - Job 2 runs **after** Job 1
   - Job 3 runs **after** Job 2

## 🔍 Unit Tests
Unit tests are included using `pytest`. Run them via:
```bash
pytest tests/
```

## 🛠️ Configuration
Edit `config/settings.py` to update paths, storage locations, or other settings.

## 📌 Notes
- If using **Databricks Repos**, ensure the workspace is properly synced.
- If using **GitHub Actions**, ensure the CI/CD pipeline deploys correctly.
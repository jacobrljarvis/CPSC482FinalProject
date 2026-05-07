# NBA Player Performance & Salary Analytics Pipeline

**CPSC 482 — Data-Intensive Systems | Jacob Jarvis | Spring 2026**

An end-to-end data analytics pipeline on Google Cloud Platform that processes 70+ years of NBA statistics and 30+ years of salary data to analyze player performance, team spending efficiency, and predict player salaries using BigQuery ML.

## Architecture

```
Raw CSVs → Cloud Storage (GCS) → Dataproc (PySpark) → BigQuery → BigQuery ML → Looker Studio
```

| Stage | Service | Purpose |
|-------|---------|---------|
| Ingestion | Raw CSVs (Kaggle) | 4 datasets, 164 MB total |
| Storage | Cloud Storage | Landing zone: raw/ and processed/ prefixes |
| Processing | Dataproc (PySpark) | Clean, deduplicate, compute features, output Parquet |
| Warehouse | BigQuery | 4 tables, 6 analytics views |
| ML | BigQuery ML | Linear Regression + XGBoost salary prediction |
| Visualization | Looker Studio | 5-page interactive dashboard |

## Datasets

| File | Rows | Size | Coverage |
|------|------|------|----------|
| NBA Player Box Score Stats (1950–2022) | 1,183,956 | 158 MB | Per-game box scores |
| NBA Player Stats (1950–2022) | 25,732 | 4.5 MB | Season averages |
| NBA Salaries (1990–2023) | 15,203 | 740 KB | Player salaries |
| NBA Payroll (1990–2023) | 966 | 43 KB | Team payroll |

## Project Structure

```
nba-analytics-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── scripts/
│   ├── transform_local.py         # Local pandas transformation (dev/testing)
│   ├── transform_pyspark.py       # PySpark job for Dataproc
│   ├── upload_to_gcs.sh           # Upload raw CSVs to Cloud Storage
│   └── setup_and_run.sh           # Full pipeline automation
├── sql/
│   ├── create_tables.sql          # BigQuery dataset + table creation
│   └── bigquery_ml.sql            # Analytics views + ML models
├── data/
│   └── raw/                       # Source CSV files (see Datasets above)
└── docs/
    ├── project_proposal.docx
    ├── project_checkin.docx
    ├── final_report.docx
    └── nba_presentation.pptx
```

## Quick Start

### Prerequisites
- GCP account with billing enabled
- `gcloud` CLI installed and authenticated
- Python 3.10+

### Run Locally (Test Transformations)
```bash
pip install pandas pyarrow
python scripts/transform_local.py
```

### Run Full GCP Pipeline
```bash
export GCP_PROJECT_ID=your-project-id
bash scripts/setup_and_run.sh
```

Then run `sql/bigquery_ml.sql` in the BigQuery console and connect Looker Studio.

## BigQuery ML Models

Two models predict inflation-adjusted salary from 11 performance features:

| Model | R² Score | MAE | Explained Variance |
|-------|----------|-----|--------------------|
| Linear Regression | 0.381 | $4.07M | 0.383 |
| XGBoost | 0.484 | $3.53M | 0.487 |

Features: age, games_played, mpg, ppg, rpg, apg, spg, bpg, fg_pct, fg3_pct, efg_pct

## Cost
Total GCP cost: **< $3**

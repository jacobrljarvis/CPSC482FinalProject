#!/bin/bash
# Full GCP pipeline: upload → Dataproc → BigQuery
# Usage: export GCP_PROJECT_ID=your-project-id && bash scripts/setup_and_run.sh

set -e
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
REGION="${GCP_REGION:-us-central1}"
BUCKET="${PROJECT_ID}-nba-analytics"
DATASET="nba_analytics"
CLUSTER="nba-cluster"

gcloud config set project ${PROJECT_ID}
gcloud services enable storage.googleapis.com dataproc.googleapis.com bigquery.googleapis.com

# Upload raw data
gsutil mb -p ${PROJECT_ID} -l ${REGION} gs://${BUCKET}/ 2>/dev/null || echo "Bucket exists."
gsutil -m cp data/raw/*.csv gs://${BUCKET}/raw/

# Dataproc
gcloud dataproc clusters create ${CLUSTER} --region=${REGION} --single-node \
  --master-machine-type=n1-standard-4 --master-boot-disk-size=100GB \
  --image-version=2.1-debian11 --max-idle=30m 2>/dev/null || echo "Cluster exists."

gsutil cp scripts/transform_pyspark.py gs://${BUCKET}/scripts/
gcloud dataproc jobs submit pyspark gs://${BUCKET}/scripts/transform_pyspark.py \
  --cluster=${CLUSTER} --region=${REGION}

# BigQuery
bq mk --dataset --location=US ${PROJECT_ID}:${DATASET} 2>/dev/null || echo "Dataset exists."
for TABLE in player_box_scores player_season_stats player_salaries team_payroll; do
  bq load --source_format=PARQUET --autodetect --replace \
    ${DATASET}.${TABLE} "gs://${BUCKET}/processed/${TABLE}/*.parquet"
done

# Cleanup
gcloud dataproc clusters delete ${CLUSTER} --region=${REGION} --quiet

echo "Pipeline complete! Run sql/bigquery_ml.sql in BigQuery console next."

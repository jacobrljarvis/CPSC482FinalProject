#!/bin/bash
# Upload raw CSVs to Google Cloud Storage
# Usage: bash scripts/upload_to_gcs.sh

set -e
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
BUCKET="${PROJECT_ID}-nba-analytics"

gsutil mb -p ${PROJECT_ID} -l us-central1 gs://${BUCKET}/ 2>/dev/null || echo "Bucket exists."
gsutil -m cp data/raw/*.csv gs://${BUCKET}/raw/
echo "Uploaded to gs://${BUCKET}/raw/"

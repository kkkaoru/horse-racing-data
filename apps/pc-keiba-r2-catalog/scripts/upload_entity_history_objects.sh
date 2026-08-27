#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:-tmp/entity-history-objects}"
bucket="${ENTITY_HISTORY_OBJECT_BUCKET:-pc-keiba-r2-catalog}"
prefix="entity-serving-v1"

: "${R2_ACCESS_KEY_ID:?R2_ACCESS_KEY_ID is required}"
: "${R2_SECRET_ACCESS_KEY:?R2_SECRET_ACCESS_KEY is required}"
: "${R2_ENDPOINT_URL:?R2_ENDPOINT_URL is required}"

test -d "${output_dir}/data"
test -f "${output_dir}/generations.json"

export AWS_ACCESS_KEY_ID="${R2_ACCESS_KEY_ID}"
export AWS_SECRET_ACCESS_KEY="${R2_SECRET_ACCESS_KEY}"
export AWS_DEFAULT_REGION="auto"

# Immutable generation data is uploaded first. The single manifest write is the
# atomic publication point, so readers never observe a partially uploaded year.
aws s3 sync \
  "${output_dir}/data" \
  "s3://${bucket}/${prefix}/data" \
  --endpoint-url "${R2_ENDPOINT_URL}" \
  --only-show-errors
aws s3 cp \
  "${output_dir}/generations.json" \
  "s3://${bucket}/${prefix}/generations.json" \
  --endpoint-url "${R2_ENDPOINT_URL}" \
  --content-type "application/json" \
  --only-show-errors

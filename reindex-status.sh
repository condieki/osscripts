#!/bin/bash
# reindex_status.sh
# Checks status of reindex tasks saved in reindex_tasks.json

OS_HOST="host:9200"
OS_USER=""
OS_PASS=""

TASK_FILE="./reindex_tasks.json"

if [[ ! -f "$TASK_FILE" ]]; then
  echo "[ERROR] Task file $TASK_FILE not found!"
  exit 1
fi

echo "[INFO] Checking status of reindex tasks..."

jq -r 'to_entries[] | "\(.key) \(.value)"' "$TASK_FILE" | while read IDX TASK_ID; do
  RESPONSE=$(curl -s -u "$OS_USER:$OS_PASS" "$OS_HOST/_tasks/$TASK_ID?pretty")
  COMPLETED=$(echo "$RESPONSE" | jq -r '.completed')
  if [[ "$COMPLETED" == "true" ]]; then
    SUCCESS=$(echo "$RESPONSE" | jq -r '.response.total')
    FAILURES=$(echo "$RESPONSE" | jq -r '.response.failures | length')
    echo "[DONE] $IDX reindex completed. Success: $SUCCESS, Failures: $FAILURES"
  else
    echo "[RUNNING] $IDX reindex still in progress..."
  fi
done

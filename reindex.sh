#!/bin/bash
# reindex.sh
# Kicks off remote reindex for all business indices from ES -> OpenSearch
# Saves task IDs to a JSON file

# Config
ES_HOST="host:9200"
ES_USER=""
ES_PASS=""

OS_HOST="host:9200"
OS_USER=""
OS_PASS=""

TASK_FILE="./reindex_tasks.json"

# Optional: date filter parameter
DATE_FILTER="$1" # e.g., "2025-01-01"

declare -A TASKS

echo "[INFO] Fetching business indices from Elasticsearch..."
INDICES=$(curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_cat/indices?h=index" | grep -Ev '^\.(internal|kibana|alerts)')  # filter system indices

echo "[INFO] Found indices:"
echo "$INDICES"

for IDX in $INDICES; do
  echo "[INFO] Starting async reindex for $IDX..."

  # Optional query by date
  QUERY=""
  if [[ -n "$DATE_FILTER" ]]; then
    QUERY=",\"query\": {\"range\": {\"created_at\": {\"gte\": \"$DATE_FILTER\"}}}"
  fi

  RESPONSE=$(curl -s -X POST -u "$OS_USER:$OS_PASS" "$OS_HOST/_reindex?wait_for_completion=false" \
    -H 'Content-Type: application/json' \
    -d "{
      \"source\": {
        \"remote\": {
          \"host\": \"$ES_HOST\",
          \"username\": \"$ES_USER\",
          \"password\": \"$ES_PASS\"
        },
        \"index\": \"$IDX\"
        $QUERY
      },
      \"dest\": {\"index\": \"$IDX\"},
      \"conflicts\": \"proceed\"
    }")

  TASK_ID=$(echo "$RESPONSE" | jq -r '.task')
  if [[ "$TASK_ID" != "null" && -n "$TASK_ID" ]]; then
    TASKS["$IDX"]="$TASK_ID"
    echo "[INFO] $IDX reindex started. Task ID: $TASK_ID"
  else
    echo "[ERROR] Failed to start reindex for $IDX: $RESPONSE"
  fi
done

# Save tasks to JSON
echo "[INFO] Saving task IDs to $TASK_FILE..."
echo "{" > "$TASK_FILE"
first=true
for IDX in "${!TASKS[@]}"; do
  [[ "$first" = true ]] && first=false || echo "," >> "$TASK_FILE"
  echo "  \"$IDX\": \"${TASKS[$IDX]}\"" >> "$TASK_FILE"
done
echo "}" >> "$TASK_FILE"

echo "[INFO] All tasks recorded in $TASK_FILE"

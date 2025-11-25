#!/usr/bin/env bash

ES_HOST="host:9200"
ES_USER=""
ES_PASS=""

OS_HOST="host:9200"
OS_USER=""
OS_PASS=""

TMP_DIR="./tmp_migration"
mkdir -p "$TMP_DIR"

CREATED_LOG="$TMP_DIR/created_indices.log"
FAILED_LOG="$TMP_DIR/failed_indices.log"
> "$CREATED_LOG"
> "$FAILED_LOG"

# Fetch only business indices (skip internal/system indices starting with '.')
fetch_business_indices() {
    curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_cat/indices?h=index&format=json" \
        | jq -r '.[].index' \
        | grep -v '^\.'   # ignore internal/system indices
}

for IDX in $(fetch_business_indices); do
    # Skip empty lines just in case
    [[ -z "$IDX" ]] && continue

    echo "==== Processing $IDX ===="
    
    SETTINGS_FILE="$TMP_DIR/${IDX}_settings.json"
    MAPPINGS_FILE="$TMP_DIR/${IDX}_mapping.json"

    # Fetch settings
    if ! curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/$IDX/_settings" -o "$SETTINGS_FILE"; then
        echo "[ERROR] Failed to fetch settings for $IDX" | tee -a "$FAILED_LOG"
        continue
    fi

    # Fetch mappings
    if ! curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/$IDX/_mapping" -o "$MAPPINGS_FILE"; then
        echo "[ERROR] Failed to fetch mapping for $IDX" | tee -a "$FAILED_LOG"
        continue
    fi

    # Create index in OpenSearch if it doesn’t exist
    if curl -s -u "$OS_USER:$OS_PASS" -I "$OS_HOST/$IDX" | grep -q "200 OK"; then
        echo "[INFO] Index $IDX already exists. Skipping creation." | tee -a "$CREATED_LOG"
        continue
    fi

    # Create index (payload minimal if merge fails)
    PAYLOAD="$TMP_DIR/${IDX}_payload.json"
    if ! jq -n --slurpfile s "$SETTINGS_FILE" --slurpfile m "$MAPPINGS_FILE" \
        '{settings: $s[0][$IDX].settings, mappings: $m[0][$IDX].mappings}' > "$PAYLOAD" 2>/dev/null; then
        echo "[WARN] Failed to merge settings/mappings for $IDX. Using minimal settings." | tee -a "$FAILED_LOG"
        echo '{}' > "$PAYLOAD"
    fi

    # PUT index
    RESP=$(curl -s -u "$OS_USER:$OS_PASS" -X PUT "$OS_HOST/$IDX" -H 'Content-Type: application/json' -d @"$PAYLOAD")
    STATUS=$(echo "$RESP" | jq -r '.status // empty')

    if [[ "$STATUS" == "400" ]]; then
        REASON=$(echo "$RESP" | jq -c '.error')
        echo "[ERROR] Index $IDX creation failed with HTTP 400." | tee -a "$FAILED_LOG"
        echo "[ERROR] Reason: $REASON" | tee -a "$FAILED_LOG"
    else
        echo "[INFO] Index $IDX created successfully." | tee -a "$CREATED_LOG"
    fi
done

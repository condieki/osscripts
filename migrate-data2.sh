#!/usr/bin/env bash
#
# migrate-data.sh - Migrate data for business indices from ES to OpenSearch
#
# Prerequisites:
#   - Run migrate-mapping.sh first (creates indices in OpenSearch)
#   - Requires elasticdump installed globally (npm install -g elasticdump)
#
# Usage:
#   Standard single-range:
#       ./migrate-data.sh [PARALLEL_JOBS] [DATE_FIELD] [DATE_FROM] [DATE_TO]
#
#   Yearly mode (sequential years, parallel indices):
#       ./migrate-data.sh 10 created_at "" "" yearly 2020 2025
#

#############################################
#      ARGUMENT PARSING
#############################################

ES_HOST="http://host:9200"
ES_USER=""
ES_PASS=""

OS_HOST="http://host:9200"
OS_USER=""
OS_PASS=""

PARALLEL_JOBS="${1:-5}"
DATE_FIELD="${2:-}"
DATE_FROM="${3:-}"
DATE_TO="${4:-}"

MODE="${5:-}"
START_YEAR="${6:-}"
END_YEAR="${7:-}"

#############################################
#      YEARLY MODE (SEQUENTIAL YEARS)
#############################################

run_yearly() {
    local field="$1"
    local start="$2"
    local end="$3"
    local jobs="$4"

    if [ -z "$field" ] || [ -z "$start" ] || [ -z "$end" ]; then
        echo "❌ Yearly mode requires: DATE_FIELD START_YEAR END_YEAR"
        echo "Example:"
        echo "   ./migrate-data.sh 10 created_at \"\" \"\" yearly 2020 2025"
        exit 1
    fi

    for YEAR in $(seq "$start" "$end"); do
        local NEXT=$((YEAR + 1))

        echo ""
        echo "======================================"
        echo " YEARLY MODE → Migrating year: $YEAR"
        echo " Range: ${YEAR}-01-01 → ${NEXT}-01-01"
        echo " Parallel jobs: $jobs"
        echo "======================================"
        echo ""

        # Call ourselves with proper date range
        bash "$0" "$jobs" "$field" "${YEAR}-01-01" "${NEXT}-01-01"
    done

    exit 0
}

# If yearly mode requested, run it and exit early
if [ "$MODE" = "yearly" ]; then
    run_yearly "$DATE_FIELD" "$START_YEAR" "$END_YEAR" "$PARALLEL_JOBS"
fi

#############################################
#      LOG SETUP
#############################################

LOG_DIR="./migration_logs_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

COMPLETED_LOG="$LOG_DIR/completed.txt"
FAILED_LOG="$LOG_DIR/failed.txt"
PROGRESS_LOG="$LOG_DIR/progress.log"

> "$COMPLETED_LOG"
> "$FAILED_LOG"
> "$PROGRESS_LOG"

echo "========================================="
echo "ES → OpenSearch Data Migration"
echo "========================================="
echo "Source: $ES_HOST"
echo "Target: $OS_HOST"
echo "Parallel Jobs: $PARALLEL_JOBS"

if [ -n "$DATE_FIELD" ]; then
    echo "Date Field: $DATE_FIELD"
    [ -n "$DATE_FROM" ] && echo "  From: $DATE_FROM"
    [ -n "$DATE_TO" ] && echo "  To: $DATE_TO"
fi

echo "Logs: $LOG_DIR/"
echo ""

#############################################
#      DEPENDENCIES
#############################################

if ! command -v elasticdump >/dev/null 2>&1; then
    echo "❌ elasticdump not installed!"
    echo "Install with: npm install -g elasticdump"
    exit 1
fi

echo "✓ elasticdump version: $(elasticdump --version)"
echo ""

#############################################
#      FETCH INDEX LIST
#############################################

fetch_business_indices() {
    curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_cat/indices?h=index&format=json" \
        | jq -r '.[].index' \
        | grep -v '^\.'   # ignore system indices
}

#############################################
#      BUILD SEARCH QUERY
#############################################

SEARCH_QUERY=""
if [ -n "$DATE_FIELD" ]; then
    RANGE_PARTS=()
    [ -n "$DATE_FROM" ] && RANGE_PARTS+=("\"gte\":\"${DATE_FROM}\"")
    [ -n "$DATE_TO" ] && RANGE_PARTS+=("\"lt\":\"${DATE_TO}\"")

    if [ ${#RANGE_PARTS[@]} -gt 0 ]; then
        RANGE_JSON=$(IFS=,; echo "${RANGE_PARTS[*]}")
        SEARCH_QUERY="{\"query\":{\"range\":{\"${DATE_FIELD}\":{${RANGE_JSON}}}}}"
    fi
fi

#############################################
#      GET BUSINESS INDICES
#############################################

echo "Fetching business indices..."
fetch_business_indices > "$LOG_DIR/indices_to_migrate.txt"

TOTAL_INDICES=$(wc -l < "$LOG_DIR/indices_to_migrate.txt" | tr -d ' ')
echo "✓ Found $TOTAL_INDICES indices"
echo ""

if [ "$TOTAL_INDICES" -eq 0 ]; then
    echo "❌ No business indices found"
    exit 1
fi

#############################################
#      MIGRATION FUNCTION
#############################################

migrate_index() {
    local idx="$1"
    local log_file="$LOG_DIR/${idx}.log"
    local start_time=$(date +%s)

    echo "[$(date '+%F %T')] Starting: $idx" | tee -a "$PROGRESS_LOG"

    local source_auth=$(echo "$ES_HOST" | sed "s|://|://${ES_USER}:${ES_PASS}@|")
    local target_auth=$(echo "$OS_HOST" | sed "s|://|://${OS_USER}:${OS_PASS}@|")

    local cmd="elasticdump \
        --input=\"${source_auth}/${idx}\" \
        --output=\"${target_auth}/${idx}\" \
        --scrollTime=10m \
        --type=data \
        --limit=10000 \
        --noRefresh \
        --retryAttempts=5 \
        --retryDelay=5000 \
        --maxSockets=10"

    [ -n "$SEARCH_QUERY" ] && cmd="$cmd --searchBody='$SEARCH_QUERY'"

    if eval $cmd >> "$log_file" 2>&1; then
        local duration=$(( $(date +%s) - start_time ))
        echo "[$(date '+%F %T')] ✓ Completed: $idx (${duration}s)" | tee -a "$PROGRESS_LOG"
        echo "$idx" >> "$COMPLETED_LOG"
    else
        local duration=$(( $(date +%s) - start_time ))
        echo "[$(date '+%F %T')] ❌ Failed: $idx (${duration}s)" | tee -a "$PROGRESS_LOG"
        echo "$idx" >> "$FAILED_LOG"
    fi
}

export -f migrate_index
export ES_HOST ES_USER ES_PASS OS_HOST OS_USER OS_PASS LOG_DIR SEARCH_QUERY PROGRESS_LOG COMPLETED_LOG FAILED_LOG

#############################################
#      EXECUTE MIGRATION (PARALLEL)
#############################################

echo "========================================="
echo " Beginning Migration"
echo "========================================="
echo ""

START=$(date +%s)

if command -v parallel >/dev/null 2>&1; then
    echo "Using GNU parallel..."
    cat "$LOG_DIR/indices_to_migrate.txt" | parallel -j "$PARALLEL_JOBS" --progress migrate_index {}
else
    echo "Using xargs (GNU parallel recommended)..."
    cat "$LOG_DIR/indices_to_migrate.txt" | xargs -P "$PARALLEL_JOBS" -I {} bash -c 'migrate_index "$@"' _ {}
fi

END=$(date +%s)
DURATION=$((END - START))

echo ""
echo "========================================="
echo " Migration Complete"
echo "========================================="
printf "Total time: %02dh %02dm %02ds\n" \
    $((DURATION/3600)) $(((DURATION%3600)/60)) $((DURATION%60))

echo ""
echo "✓ Completed: $(wc -l < "$COMPLETED_LOG")"
echo "❌ Failed:   $(wc -l < "$FAILED_LOG")"

if [ -s "$FAILED_LOG" ]; then
    echo ""
    echo "Failed indices:"
    cat "$FAILED_LOG"
fi

echo ""
echo "Logs stored at: $LOG_DIR/"
echo ""

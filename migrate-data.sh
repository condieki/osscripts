#!/usr/bin/env bash
#
# migrate-data.sh - Migrate data for business indices from ES to OpenSearch
#
# Prerequisites: Run migrate-mapping.sh first to create indices
# This script migrates ONLY the data (documents), not mappings/settings
#
# Usage: ./migrate-data.sh [PARALLEL_JOBS] [DATE_FIELD] [DATE_FROM] [DATE_TO]
#

ES_HOST="host:9200"
ES_USER=""
ES_PASS=""

OS_HOST="host:9200"
OS_USER=""
OS_PASS=""

PARALLEL_JOBS="${1:-5}"
DATE_FIELD="${2:-}"
DATE_FROM="${3:-}"
DATE_TO="${4:-}"

LOG_DIR="./migration_logs_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

COMPLETED_LOG="$LOG_DIR/completed.txt"
FAILED_LOG="$LOG_DIR/failed.txt"
PROGRESS_LOG="$LOG_DIR/progress.log"
> "$COMPLETED_LOG"
> "$FAILED_LOG"
> "$PROGRESS_LOG"

echo "========================================="
echo "Data Migration: ES → OpenSearch"
echo "========================================="
echo "Source: $ES_HOST"
echo "Target: $OS_HOST"
echo "Parallel Jobs: $PARALLEL_JOBS"
if [ ! -z "$DATE_FIELD" ]; then
    echo "Date Filter: $DATE_FIELD"
    [ ! -z "$DATE_FROM" ] && echo "  From: $DATE_FROM"
    [ ! -z "$DATE_TO" ] && echo "  To: $DATE_TO"
fi
echo "Logs: $LOG_DIR/"
echo ""

# Check if elasticdump is installed
if ! command -v elasticdump &> /dev/null; then
    echo "❌ elasticdump not found!"
    echo ""
    echo "Install with: npm install -g elasticdump"
    exit 1
fi

echo "✓ elasticdump version: $(elasticdump --version)"
echo ""

# Fetch only business indices (skip internal/system indices starting with '.')
fetch_business_indices() {
    curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_cat/indices?h=index&format=json" \
        | jq -r '.[].index' \
        | grep -v '^\.'   # ignore internal/system indices
}

# Build search query for date filtering
SEARCH_QUERY=""
if [ ! -z "$DATE_FIELD" ]; then
    QUERY_PARTS=()
    
    if [ ! -z "$DATE_FROM" ]; then
        QUERY_PARTS+=("\"gte\":\"${DATE_FROM}\"")
    fi
    
    if [ ! -z "$DATE_TO" ]; then
        QUERY_PARTS+=("\"lt\":\"${DATE_TO}\"")
    fi
    
    if [ ${#QUERY_PARTS[@]} -gt 0 ]; then
        RANGE_QUERY=$(IFS=,; echo "${QUERY_PARTS[*]}")
        SEARCH_QUERY="{\"query\":{\"range\":{\"${DATE_FIELD}\":{${RANGE_QUERY}}}}}"
    fi
fi

# Get list of indices
echo "Fetching business indices..."
fetch_business_indices > "$LOG_DIR/indices_to_migrate.txt"
TOTAL_INDICES=$(cat "$LOG_DIR/indices_to_migrate.txt" | wc -l | tr -d ' ')
echo "✓ Found $TOTAL_INDICES business indices to migrate"
echo ""

if [ $TOTAL_INDICES -eq 0 ]; then
    echo "❌ No business indices found!"
    exit 1
fi

# Migration function
migrate_index() {
    local idx=$1
    local log_file="$LOG_DIR/${idx}.log"
    local start_time=$(date +%s)
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting: $idx" | tee -a "$PROGRESS_LOG"
    
    # Build elasticdump command with auth in URL
    # Convert http://host:port to http://user:pass@host:port
    local source_with_auth=$(echo "$ES_HOST" | sed "s|://|://${ES_USER}:${ES_PASS}@|")
    local target_with_auth=$(echo "$OS_HOST" | sed "s|://|://${OS_USER}:${OS_PASS}@|")

    local cmd="elasticdump \
        --input=\"${source_with_auth}/${idx}\" \
        --output=\"${target_with_auth}/${idx}\" \
        --type=data \
        --limit=10000 \
        --noRefresh \
        --retryAttempts=5 \
        --retryDelay=5000 \
        --maxSockets=10"
    
    # Add search query if provided
    if [ ! -z "$SEARCH_QUERY" ]; then
        cmd="$cmd --searchBody='$SEARCH_QUERY'"
    fi
    
    # Execute migration
    if eval $cmd >> "$log_file" 2>&1; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✓ Completed: $idx (${duration}s)" | tee -a "$PROGRESS_LOG"
        echo "$idx" >> "$COMPLETED_LOG"
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ Failed: $idx (${duration}s)" | tee -a "$PROGRESS_LOG"
        echo "$idx" >> "$FAILED_LOG"
        return 1
    fi
}

export -f migrate_index
export ES_HOST ES_USER ES_PASS OS_HOST OS_USER OS_PASS LOG_DIR SEARCH_QUERY PROGRESS_LOG COMPLETED_LOG FAILED_LOG

echo "========================================="
echo "Starting parallel migration..."
echo "========================================="
echo ""

START_TIME=$(date +%s)

# Use GNU parallel if available, otherwise use xargs
if command -v parallel &> /dev/null; then
    echo "Using GNU parallel..."
    cat "$LOG_DIR/indices_to_migrate.txt" | \
        parallel -j $PARALLEL_JOBS --progress \
        migrate_index {}
else
    echo "Using xargs (install GNU parallel for better progress tracking)..."
    cat "$LOG_DIR/indices_to_migrate.txt" | \
        xargs -P $PARALLEL_JOBS -I {} bash -c 'migrate_index "{}"'
fi

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))
HOURS=$((TOTAL_DURATION / 3600))
MINUTES=$(((TOTAL_DURATION % 3600) / 60))
SECONDS=$((TOTAL_DURATION % 60))

echo ""
echo "========================================="
echo "Migration Complete!"
echo "========================================="
echo "Total time: ${HOURS}h ${MINUTES}m ${SECONDS}s"
echo ""

COMPLETED_COUNT=$(cat "$COMPLETED_LOG" 2>/dev/null | wc -l | tr -d ' ')
FAILED_COUNT=$(cat "$FAILED_LOG" 2>/dev/null | wc -l | tr -d ' ')

echo "Results:"
echo "  ✓ Completed: $COMPLETED_COUNT"
echo "  ❌ Failed: $FAILED_COUNT"
echo ""

if [ $FAILED_COUNT -gt 0 ]; then
    echo "Failed indices:"
    cat "$FAILED_LOG"
    echo ""
    echo "To retry failed indices:"
    echo "  cat $FAILED_LOG | while read idx; do"
    echo "    elasticdump --input=$ES_HOST/\$idx --output=$OS_HOST/\$idx --type=data --sourceAuth=$ES_USER:$ES_PASS --targetAuth=$OS_USER:$OS_PASS"
    echo "  done"
    echo ""
fi

echo "Logs saved to: $LOG_DIR/"
echo ""
echo "Next step: Run ./check-status.sh to validate migration"

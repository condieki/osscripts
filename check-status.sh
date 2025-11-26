#!/usr/bin/env bash
#
# check-status.sh - Check migration status and validate data
#
# This script compares document counts between source ES and target OpenSearch
# Run this during or after migration to check progress
#
# Usage: ./check-status.sh [--watch]
#

ES_HOST="host:9200"
ES_USER=""
ES_PASS=""

OS_HOST="host:9200"
OS_USER=""
OS_PASS=""

WATCH_MODE=false
if [[ "$1" == "--watch" ]]; then
    WATCH_MODE=true
fi

# Fetch only business indices (skip internal/system indices starting with '.')
fetch_business_indices() {
    curl -s -u "$ES_USER:$ES_PASS" "$ES_HOST/_cat/indices?h=index&format=json" \
        | jq -r '.[].index' \
        | grep -v '^\.'   # ignore internal/system indices
}

# Get document count for an index
get_doc_count() {
    local host=$1
    local auth=$2
    local index=$3
    
    curl -s -u "$auth" "$host/$index/_count" | jq -r '.count // 0'
}

# Check status once
check_status() {
    clear
    
    echo "========================================="
    echo "Migration Status Check"
    echo "========================================="
    echo "Source: $ES_HOST"
    echo "Target: $OS_HOST"
    echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    
    printf "%-40s %15s %15s %15s %10s\n" "INDEX" "SOURCE_DOCS" "TARGET_DOCS" "DIFFERENCE" "STATUS"
    echo "--------------------------------------------------------------------------------------------------------"
    
    local total_source=0
    local total_target=0
    local total_match=0
    local total_mismatch=0
    local total_missing=0
    
    for idx in $(fetch_business_indices); do
        # Get counts
        local source_count=$(get_doc_count "$ES_HOST" "$ES_USER:$ES_PASS" "$idx")
        local target_count=$(get_doc_count "$OS_HOST" "$OS_USER:$OS_PASS" "$idx")
        
        # Check if index exists on target
        if ! curl -s -u "$OS_USER:$OS_PASS" -I "$OS_HOST/$idx" 2>/dev/null | grep -q "200 OK"; then
            printf "%-40s %15s %15s %15s %10s\n" "$idx" "$source_count" "MISSING" "-" "❌ MISSING"
            total_missing=$((total_missing + 1))
            continue
        fi
        
        total_source=$((total_source + source_count))
        total_target=$((total_target + target_count))
        
        # Compare counts
        if [ "$source_count" -eq "$target_count" ]; then
            printf "%-40s %15s %15s %15s %10s\n" "$idx" "$source_count" "$target_count" "0" "✓ MATCH"
            total_match=$((total_match + 1))
        else
            local diff=$((source_count - target_count))
            local pct=0
            if [ $source_count -gt 0 ]; then
                pct=$((target_count * 100 / source_count))
            fi
            printf "%-40s %15s %15s %15s %10s\n" "$idx" "$source_count" "$target_count" "$diff" "⚠️  ${pct}%"
            total_mismatch=$((total_mismatch + 1))
        fi
    done
    
    echo "--------------------------------------------------------------------------------------------------------"
    printf "%-40s %15s %15s %15s\n" "TOTAL" "$total_source" "$total_target" "$((total_source - total_target))"
    echo ""
    
    echo "========================================="
    echo "Summary"
    echo "========================================="
    echo "✓ Matching: $total_match indices"
    echo "⚠️  Mismatched: $total_mismatch indices"
    echo "❌ Missing: $total_missing indices"
    echo ""
    
    if [ $total_source -gt 0 ]; then
        local overall_pct=$((total_target * 100 / total_source))
        echo "Overall Progress: ${overall_pct}% ($total_target / $total_source documents)"
    fi
    echo ""
    
    if [ $total_mismatch -gt 0 ] || [ $total_missing -gt 0 ]; then
        echo "⚠️  Migration incomplete or in progress"
    else
        echo "✅ Migration complete! All indices match."
    fi
    echo ""
}

# Main execution
if [ "$WATCH_MODE" = true ]; then
    echo "Watch mode enabled. Press Ctrl+C to exit."
    echo "Refreshing every 60 seconds..."
    echo ""
    
    while true; do
        check_status
        echo "Next refresh in 60 seconds... (Ctrl+C to exit)"
        sleep 60
    done
else
    check_status
    echo "Tip: Run with --watch to monitor in real-time:"
    echo "  ./check-status.sh --watch"
    echo ""
fi

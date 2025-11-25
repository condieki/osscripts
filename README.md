# Elasticsearch -> OpenSearch Migration Helper

This repository contains simple scripts to help migrate **business indices** from an Elasticsearch source cluster to an OpenSearch target cluster. It handles fetching settings and mappings from the source and preparing the target cluster for reindexing.

---

## Overview

The scripts help you:

1. Extract settings and mappings from your Elasticsearch cluster.
2. Prepare your OpenSearch cluster with compatible indices.
3. Optionally perform reindexing from source → target.
4. Monitor reindexing tasks.

---

## Prerequisites

- `bash`
- `curl`
- `jq`

---

## Setup

1. Edit the scripts to configure your source (ie Elasticsearch) and target (OpenSearch) credentials and hosts.
2. Make scripts executable:

```bash
chmod +x migrate-mapping.sh
chmod +x reindex.sh
chmod +x reindex-status.sh


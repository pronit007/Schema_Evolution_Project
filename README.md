
# Discussion Points for End-to-End Schema Evolution Project (Snowflake + Snowpipe):

# Why Schema Evolution Was Needed
1.Source systems send CSV files with frequently changing column structures (added, removed, or reordered columns).
2.Traditional ETL pipelines break when column mismatch occurs.
3.Business teams insisted on zero downtime and auto-adjusting pipelines.
4.Needed a system that can auto-detect schema drift and load data without manual intervention.

# Choosing Snowflake for Dynamic Schema Handling
1.Snowflake provides metadata-driven schema evolution.
2.No physical rewrite of tables → cost-efficient.
3.Handles new columns by adding them automatically as VARCHAR.
4.Fills missing columns with NULL, preventing load failures.
5.Snowpipe ensures continuous ingestion rather than batch pipelines.

# Architecture Decisions:
# 1.Landing Zone → S3
    Raw files kept untouched to maintain data lineage.
# 2.External Stage + Storage Integration
    Secure IAM-based connection, avoids credential management.
# 3.File Format
    Supports CSV variation, header handling, delimiter, null handling.
# 4.INFER_SCHEMA + USING TEMPLATE
    Auto-generation of initial table from first incoming file.
# 5.Snowpipe + AUTO_INGEST
    Handles streaming ingestion as soon as files arrive in S3.
# 6. MATCH_BY_COLUMN_NAME
    Prevent errors when columns are reordered.
# 7.ENABLE_SCHEMA_EVOLUTION = TRUE
    Allows automatic ALTER TABLE ADD COLUMN.

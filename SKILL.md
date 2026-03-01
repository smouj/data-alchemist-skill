---
name: Data Alchemist
description: Transforms raw data into valuable insights through alchemical processes
version: 1.0.0
author: SMOUJBOT
tags:
  - data
  - alchemy
  - transformation
  - insights
  - etl
  - analytics
maintainer: ops@smouj.dev
last_updated: 2026-03-01
category: data-processing
status: stable
dependencies:
  - python>=3.9
  - pandas>=2.0.0
  - numpy>=1.24.0
  - pyarrow>=14.0.0
  - openpyxl>=3.1.0
  - great-expectations>=0.18.0
  - datamodel-code-generator>=0.21.0
  - jupyter>=1.0.0
  - scikit-learn>=1.3.0
  - fsspec>=2023.10.0
required_services:
  - postgresql:optional
  - mysql:optional
  - s3:optional
  - bigquery:optional
  - snowflake:optional
exposes_ports: []
volumes:
  - ./data
  - ./output
network_mode: none
---

# Data Alchemist Skill

## Purpose

Data Alchemist transforms raw, messy datasets into structured, actionable insights through automated ETL (Extract, Transform, Load) pipelines. It performs data validation, transformation, enrichment, and analysis without requiring manual scripting for common data operations.

### Real Use Cases

1. **CSV to PostgreSQL Pipeline**: Automatically ingest a messy CSV with inconsistent headers, missing values, and type mismatches, validate against a schema, clean, and load to PostgreSQL with full audit trail.

2. **API Data Enrichment**: Extract data from REST endpoints, merge with existing datasets, apply transformations (geocoding, categorization, sentiment analysis), and export to dashboard-ready Parquet files.

3. **Data Quality Audit**: Scan production databases for anomalies, check for null thresholds, validate foreign key relationships, and generate a comprehensive quality report with failed rows extracted.

4. **Time Series Aggregation**: Take high-frequency sensor data (millions of rows), resample to business intervals, calculate rolling statistics, detect outliers using IQR, and produce trend reports.

5. **Schema Extraction & Documentation**: Infer schema from unstructured data sources, generate JSON Schema or Pydantic models, create data dictionaries, and produce ER diagrams automatically.

## Scope

### Core Commands

```bash
# Transform a file with automatic type inference and cleaning
alchemy transform --input data/raw.csv --output data/clean.parquet --clean-modes deduplicate,standardize,impute

# Validate data against a schema (JSON Schema or Pydantic)
alchemy validate --input data/input.json --schema schemas/customer.schema.json --report-quality

# Extract from API with pagination and auth
alchemy extract --endpoint https://api.example.com/v1/orders --token $API_TOKEN --pages-all --output raw/api_orders.json

# Load to database with upsert strategy
alchemy load --input data/clean.parquet --table orders --db postgresql://user:pass@localhost/db --upsert-keys order_id,customer_id

# Run full ETL pipeline defined in YAML
alchemy pipeline --file pipelines/daily_sales.yaml --run-id 20260301_001

# Generate data profile and insights
alchemy profile --input data/sales.csv --output reports/sales_profile.html --correlation-threshold 0.8

# Merge multiple sources with conflict resolution
alchemy merge --sources data/jan.parquet,data/feb.parquet --on date,product_id --strategy latest --output data/merged.parquet

# Detect anomalies using statistical methods
alchemy anomaly --input data/metrics.parquet --column revenue --method iqr --threshold 1.5 --output anomalies.csv
```

### Extended Commands

```bash
# Create new pipeline template
alchemy init-pipeline --name daily_sales --template postgres-to-parquet

# List available transformations
alchemy list-transforms

# Test pipeline without execution
alchemy dry-run --file pipelines/daily_sales.yaml

# Rollback last successful load
alchemy rollback --table orders --to-timestamp "2026-03-01 02:00:00"

# Generate schema from sample data
alchemy infer-schema --input data/sample.csv --format pydantic --output schemas/sample.py

# Create data quality tests
alchemy generate-tests --table customers --expectation-type not-null --columns email,phone
```

## Work Process

### Step 1: Assessment
```bash
# Profile raw data to understand structure and quality
alchemy profile --input data/raw/input.csv --output assessment/profile.json ---sample-rows 10000

# Infer schema from sample
alchemy infer-schema --input data/raw/input.csv --format json-schema --output schemas/inferred.json
```

Review generated profile:
- Column types detected
- Null percentage per column
- Unique value counts
- Min/max ranges for numeric columns
- Cardinality for categorical fields
- Outlier detection

### Step 2: Schema Definition
Create explicit schema based on inference:
```bash
# Use inferred schema as starting point
cp schemas/inferred.json schemas/customer.v1.json
# Edit to add constraints, descriptions, required fields
```

### Step 3: Validation
```bash
# Validate source against schema
alchemy validate \
  --input data/raw/input.csv \
  --schema schemas/customer.v1.json \
  --fail-on-error \
  --output validation/results.json \
  --extract-failures data/failures/
```

### Step 4: Transformation Pipeline
Create pipeline YAML:
```yaml
# pipelines/daily_sales.yaml
name: daily_sales_pipeline
version: 1.0.0
description: Daily sales ETL from CSV to data warehouse

stages:
  - name: extract
    type: extract
    config:
      source: data/raw/sales_$(date +%Y%m%d).csv
      format: csv
      options:
        encoding: utf-8
        delimiter: ","
    
  - name: clean
    type: transform
    config:
      operations:
        - drop_duplicates:
            subset: ["transaction_id"]
        - standardize_columns:
            case: snake
        - type_convert:
            columns:
              - name: amount
                type: decimal
                precision: 10
                scale: 2
              - name: transaction_date
                type: datetime
                format: "%Y-%m-%d %H:%M:%S"
        - impute_missing:
            strategy: forward_fill
            columns: ["customer_id"]
            
  - name: validate
    type: validate
    config:
      schema: schemas/sales.v1.json
      threshold: 0.99  # 99% valid rows required
      
  - name: enrich
    type: transform
    config:
      operations:
        - add_column:
            name: day_of_week
            expression: "transaction_date.dt.dayofweek"
        - lookup:
            table: dim_customers
            on: customer_id
            columns: ["customer_tier", "region"]
            
  - name: load
    type: load
    config:
      destination: postgresql://user:pass@localhost/warehouse
      table: fact_sales_daily
      strategy: upsert
      keys: ["transaction_id"]
      partition_by: ["transaction_date"]
```

Execute pipeline:
```bash
alchemy pipeline --file pipelines/daily_sales.yaml --run-id 20260301_001 --log-level DEBUG
```

### Step 5: Quality Reporting
```bash
# Generate quality report
alchemy report \
  --pipeline-run runs/20260301_001/ \
  --output reports/quality_20260301.html \
  --include-metrics coverage,validity,uniqueness \
  --compare-previous 3

# Extract rejected rows for manual review
alchemy extract-failures \
  --validation-file runs/20260301_001/validation/results.json \
  --output data/rejected/20260301/
```

### Step 6: Profiling & Insights
```bash
# Generate comprehensive profile of output
alchemy profile \
  --input data/warehouse/fact_sales_daily.parquet \
  --output reports/sales_insights_$(date +%Y%m%d).html \
  --correlation-threshold 0.7 \
  --detect-outliers \
  --trend-analysis columns=amount,quantity

# Generate SQL for common insights
alchemy sql-insights \
  --table fact_sales_daily \
  --output reports/insights.sql \
  --queries: top-customers,weekly-trends,seasonality
```

## Golden Rules

1. **Always validate before load**: Never load data without validation step in pipeline. Use `--fail-threshold` to control sensitivity.

2. **Preserve raw data**: Never overwrite source files. Pipeline outputs go to `data/processed/YYYYMMDD/` with run ID tracking.

3. **Make transformations idempotent**: Design transforms to produce same output given same input. Avoid random sampling without seeds.

4. **Schema evolution requires versioning**: When schema changes, increment version in filename (`customer.v1.json` → `customer.v2.json`) and maintain backward compatibility.

5. **Log lineage automatically**: Every pipeline run creates `runs/<run_id>/lineage.json` with source→transform→load mapping.

6. **Extract failures separately**: All validation failures extracted to `data/failures/` with reason codes, never silently dropped.

7. **Parameterize dates**: Never hardcode dates. Use `$(date +%Y%m%d)` or pipeline parameters.

8. **Test in dry-run first**: Always run `alchemy dry-run` before production execution.

9. **Use version control for schemas and pipelines**: Never edit directly in production; always create PR with changes.

10. **Monitor data drift**: Schedule weekly `alchemy profile --compare-baseline` to detect distribution shifts.

## Examples

### Example 1: Simple CSV Cleanup
**User Input**:
```bash
alchemy transform \
  --input data/raw/customers.csv \
  --output data/clean/customers_$(date +%Y%m%d).parquet \
  --clean-modes deduplicate,standardize,impute,kickoutliers \
  --deduplicate-on email,phone \
  --impute-strategy median:age,mode:country \
  --outlier-threshold 3.0
```

**Generated Output**:
```
Transformation complete: data/clean/customers_20260301.parquet
Rows: 15,234 → 14,987 (247 duplicates removed)
Columns: 12 → 12 (standardized to snake_case)
Missing values imputed: age=23, country=5
Outliers removed: 12 (z-score > 3.0)
Schema inferred: customers.d20260301.schema.json
```

### Example 2: Full ETL Pipeline
**User Input** (pipeline.yaml):
```yaml
name: api_to_dashboard
stages:
  - type: extract
    config:
      endpoint: https://api.salesforce.com/v1/opportunities
      auth: bearer ${SALESFORCE_TOKEN}
      pagination: cursor
      batch_size: 2000
  - type: transform
    config:
      operations:
        - parse_json: field=payload
        - flatten: field=payload
        - rename: {"Opportunity ID": "opportunity_id", "Amount": "amount"}
        - currency_convert: field=amount,from=USD,to=EUR,rate=${EXCHANGE_RATE}
  - type: validate
    config:
      schema: schemas/opportunity.v2.json
      sample: 0.1  # Validate 10% for speed
  - type: load
    config:
      table: dw.opportunity_staging
      mode: append
```

**Command**:
```bash
export SALESFORCE_TOKEN="00Dxxx...!"
export EXCHANGE_RATE=0.92
alchemy pipeline --file pipelines/salesforce_ops.yaml --run-id sf_20260301 --parallel-jobs 4
```

**Output**:
```
Pipeline sf_20260301 started at 2026-03-01 04:30:00 UTC
Stage 1/4 (extract): Fetched 24,532 records across 13 pages (12.4 MB)
Stage 2/4 (transform): Applied 4 operations, output: 24,532 rows → 24,532 rows
Stage 3/4 (validate): Sample validation passed 2,453/2,453 (100%) ✓
Stage 4/4 (load): Inserted 24,532 rows into dw.opportunity_staging (45.1 s)
Pipeline completed in 3m 27s | Run ID: sf_20260301 | logs: runs/sf_20260301/pipeline.log
```

### Example 3: Data Quality Test Generation
**User Input**:
```bash
alchemy generate-tests \
  --table staging.customers \
  --db postgresql://localhost/warehouse \
  --output tests/customers_quality.py \
  --expectations: range-age-0-120, regex-email, not-null-email, unique-customer_id, date-valid-signup_date
```

**Generated tests** (partial):
```python
import great_expectations as gx

def test_customers_quality():
    context = gx.get_context()
    datasource = context.sources.add_postgresql(
        name="warehouse",
        connection_string="postgresql://localhost/warehouse"
    )
    asset = datasource.add_query_asset(name="customers", query="SELECT * FROM staging.customers")
    
    batch_request = asset.build_batch_request()
    validator = context.get_validator(batch_request=batch_request)
    
    # not-null email
    validator.expect_column_values_to_not_be_null(column="email")
    
    # unique customer_id
    validator.expect_column_values_to_be_unique(column="customer_id")
    
    # age range 0-120
    validator.expect_column_values_to_be_between(column="age", min_value=0, max_value=120)
    
    # valid email format
    validator.expect_column_values_to_match_regex(column="email", regex=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
```

### Example 4: Schema Inference & Model Generation
**User Input**:
```bash
alchemy infer-schema \
  --input data/raw/transactions.jsonl \
  --format pydantic \
  --output schemas/transaction.py \
  --class-name Transaction \
  --detect-nullable \
  --add-descriptions
```

**Generated schema**:
```python
from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, validator

class Transaction(BaseModel):
    """Transaction record from source system"""
    transaction_id: str = Field(..., description="Unique transaction identifier", alias="id")
    amount: Decimal = Field(..., ge=0, description="Transaction amount in USD", max_digits=10, decimal_places=2)
    currency: str = Field(default="USD", description="ISO currency code", min_length=3, max_length=3)
    timestamp: datetime = Field(..., description="Transaction timestamp in UTC")
    customer_id: Optional[str] = Field(None, description="Customer identifier if logged in")
    product_sku: str = Field(..., description="Product SKU")
    quantity: int = Field(..., ge=1, description="Quantity purchased")
    
    @validator("timestamp", pre=True)
    def parse_timestamp(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v
```

### Example 5: Anomaly Detection on Sensor Data
**User Input**:
```bash
alchemy anomaly \
  --input data/sensors/raw/202603/*.parquet \
  --output reports/anomalies_20260301.csv \
  --method ensemble \
  --sensors temperature,pressure,vibration \
  --sensitivity 2.5 \
  --min-consecutive 3 \
  --timeseries \
  --window 1h \
  --export-images plots/
```

**Output**:
```
Analyzing 2,147,483,647 rows across 3 sensors
Method: ensemble (IQR + Seasonal + STL)
Detected: 1,247 anomaly events
- Temperature spikes: 432
- Pressure drops: 287
- Vibration anomalies: 528
Export: reports/anomalies_20260301.csv (detailed events)
Plots: plots/temperature_anomalies_20260301.png (sample)
```

## Dependencies & Requirements

### Python Packages (auto-installed)
```bash
pip install pandas numpy pyarrow openpyxl great-expectations datamodel-code-generator scikit-learn fsspec[json] fsspec[aws] fsspec[gcs]
```

### Optional Database Drivers
```bash
# PostgreSQL
pip install psycopg2-binary sqlalchemy

# MySQL
pip install pymysql sqlalchemy

# SQL Server
pip install pyodbc sqlalchemy

# BigQuery
pip install pandas-gbq google-cloud-bigquery

# Snowflake
pip install snowflake-sqlalchemy snowflake-connector-python
```

### Environment Variables
```bash
# Database connections (use connection strings instead where possible)
export POSTGRES_URL="postgresql://user:pass@host:5432/db"
export MYSQL_URL="mysql://user:pass@host:3306/db"
export SNOWFLAKE_URL="snowflake://user:pass@account/db/schema"

# Cloud storage
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# API tokens
export SALESFORCE_TOKEN="00D..."
export STRIPE_API_KEY="sk_live_..."

# Exchange rates for currency conversion
export EXCHANGE_RATES_URL="https://api.exchangerate-api.com/v4/latest/USD"
```

### System Requirements
- Python 3.9+ with venv support
- 4 GB RAM minimum (16 GB recommended for large datasets)
- 10 GB free disk space for temporary storage
- Network access for API extraction and cloud storage

## Verification Steps

After any transformation or load:

```bash
# 1. Check row counts
alchemy verify-count \
  --source data/raw/input.csv \
  --target data/warehouse/fact_table \
  --tolerance 0.01  # Allow 1% difference for filtered rows

# 2. Validate checksums
alchemy checksum \
  --input data/clean/output.parquet \
  --algorithm sha256 \
  --output verification/checksums.txt

# 3. Spot check records
alchemy sample \
  --table dw.fact_sales \
  --where "transaction_date >= '2026-03-01'" \
  --format markdown \
  --limit 5

# 4. Run quality tests
alchemy test-run \
  --tests tests/quality_suite.py \
  --report-format html \
  --output reports/quality_$(date +%Y%m%d).html

# 5. Compare distributions (statistical test)
alchemy distribution-test \
  --baseline data/baseline/profile.json \
  --current data/current/profile.json \
  --test ks  # Kolmogorov-Smirnov test
  --p-threshold 0.05
```

Expected success criteria:
- Row count within tolerance
- No nulls in required columns (according to schema)
- All unique constraints satisfied
- No duplicate business keys
- Column value distributions stable (p > 0.05)

## Rollback Commands

### Full Pipeline Rollback
```bash
# Rollback entire pipeline run (restores database to pre-run state)
alchemy rollback \
  --run-id 20260301_001 \
  --strategy database-point-in-time \
  --to-timestamp "2026-03-01 03:15:00 UTC" \
  --confirm
```

### Table-Level Rollback
```bash
# Restore table from backup Parquet file
alchemy rollback-table \
  --table fact_sales_daily \
  --from-backup backups/fact_sales_daily/20260301_030000.parquet \
  --mode replace  # or 'merge' for upsert
```

### Partition Rollback (Time-based)
```bash
# Rollback specific date partition only
alchemy rollback-partition \
  --table fact_sales \
  --partition transaction_date=2026-03-01 \
  --from-run sf_20260301_002 \
  --mode delete-and-reload
```

### Schema Rollback
```bash
# Revert schema to previous version
alchemy schema-rollback \
  --schema schemas/customer.v2.json \
  --to-version v1.0 \
  --backup-current schemas/backups/customer.v2.20260301.bak.json
```

### Transaction Log Rollback (PostgreSQL)
```bash
# Rollback using WAL if point-in-time recovery available
alchemy rollback-wal \
  --db postgresql://localhost/warehouse \
  --to-lsn "0/16B6C0A0" \
  --tables orders,customers \
  --output-dir ./rollback_restores/
```

### Dry-run Rollback Preview
```bash
# See what would be rolled back without executing
alchemy rollback \
  --run-id 20260301_001 \
  --dry-run \
  --verbosity detailed
```

**Rollback Output Example**:
```
DRY RUN: Rollback plan for run-id=20260301_001
================================================
Operations to execute: 3

1. DROP staging.sales_20260301_001
   Affected rows: 24,532
   Duration estimate: 2s

2. DELETE FROM warehouse.fact_sales 
   WHERE load_run_id = '20260301_001'
   Affected rows: 24,532
   Duration estimate: 8s

3. RESTORE warehouse.dim_product FROM BACKUP runs/20260228_001/backups/dim_product.parquet
   Backup timestamp: 2026-02-28 23:45:12 UTC
   Row count: 1,247 (matching baseline)

Total estimated time: 15s
Confirm with: alchemy rollback --run-id 20260301_001 --confirm
```

## Troubleshooting

### Issue: MemoryError during transform
**Cause**: Dataset too large for memory
**Fix**:
```bash
alchemy transform \
  --input huge_file.csv \
  --output cleaned.parquet \
  --chunk-size 10000 \
  --memory-limit 4GB \
  --temp-dir /tmp/alchemy/
```

### Issue: Validation fails on date parsing
**Cause**: Multiple date formats in source
**Fix**:
```bash
alchemy validate \
  --input data.csv \
  --schema schema.json \
  --parse-dates column=timestamp,formats=["%Y-%m-%d","%m/%d/%Y %H:%M","ISO8601"]
```

### Issue: Load performance slow
**Cause**: Missing indexes or wrong batch size
**Fix**:
```bash
alchemy load \
  --input data.parquet \
  --table target \
  --batch-size 5000 \
  --create-indexes \
  --disable-fk-checks \
  --parallel-load-jobs 4
```

### Issue: API extraction rate-limited
**Cause**: Too many requests per second
**Fix**:
```bash
alchemy extract \
  --endpoint https://api.example.com \
  --rate-limit 10/sec \
  --retry-attempts 5 \
  --retry-backoff exponential \
  --timeout 300
```

### Issue: Schema drift after source update
**Cause**: Source columns changed (added/dropped)
**Fix**:
```bash
alchemy detect-schema-drift \
  --baseline schemas/customer.v1.json \
  --current data/new_sample.csv \
  --mode report  # or 'auto-update' with --allow-minor-changes
```

## Quick Reference

```bash
# Transform with common options
alchemy transform -i raw.csv -o clean.parquet --deduplicate --standardize --impute-missing

# Full pipeline
alchemy pipeline -f my_pipeline.yaml --run-id $(date +%Y%m%d_%H%M) --log-file runs/$(date +%Y%m%d).log

# Profile data
alchemy profile -i dataset.parquet -o report.html --correlation --outliers --trends

# Validate
alchemy validate -i data.json -s schema.json --extract-failures failed_rows/

# Generate tests (Great Expectations)
alchemy test generate --table sales --output tests/ --auto-expectations

# Load to database
alchemy load -i clean.parquet -t fact_sales --db $POSTGRES_URL --upsert-keys id --create-indexes

# Extract from API
alchemy extract --url https://api.example.com/v1/data --token $TOKEN --pages all --output raw.json

# Merge sources
alchemy merge -i source1.parquet source2.parquet -o merged.parquet --on id --conflict strategy=latest

# Dry run (preview without execution)
alchemy dry-run -f pipeline.yaml --show-stages --estimate-cost
```
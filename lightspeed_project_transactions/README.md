# Transaction Lifecycle Modeling with dbt

This project models financial transaction data using a medallion-style dbt architecture across Bronze, Silver, and Gold layers. The goal is to transform raw user, card, and transaction datasets into analytics-ready fact tables, dimensions, and metric marts that support retention, behavioral, and segmentation analysis.

The project currently includes:

- Bronze models that preserve source-aligned raw data
- Silver models that clean and standardize users, cards, and transactions
- An intermediate joined Silver model for enriched transaction analysis
- Gold fact and dimension tables for downstream analytics
- Gold metric marts for daily performance, cohort retention, and time-of-day analysis
- Generic dbt tests and custom reconciliation tests across layers
- A dbt snapshot to demonstrate SCD Type 2 history tracking for users

## Architecture

This project follows a standard three-layer analytics engineering structure:

1. Bronze
   Raw source-aligned models with minimal transformation
2. Silver
   Cleaned, typed, standardized, and enriched analytical base models
3. Gold
   Business-facing semantic models, fact and dimension tables, and metric marts

The project is configured in [dbt_project.yml](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/dbt_project.yml) to build models into separate schemas:

- `bronze`
- `silver`
- `gold`

## Source Data

The project uses three core source tables defined in [sources.yml](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/source/sources.yml):

- `transactions_data`
- `users_data`
- `cards_data`

These sources are treated as the raw system of record for Bronze ingestion.

## Data Model Overview

### Users

The users dataset acts as a user-level dimension with:

- demographic attributes
- geographic attributes
- financial attributes
- credit profile attributes

Key fields include:

- `id`
- `current_age`
- `retirement_age`
- `birth_year`
- `birth_month`
- `gender`
- `address`
- `latitude`
- `longitude`
- `per_capita_income`
- `yearly_income`
- `total_debt`
- `credit_score`
- `num_credit_cards`

### Transactions

The transactions dataset acts as the core event-level fact source with:

- user linkage through `client_id`
- card linkage through `card_id`
- merchant and payment behavior fields
- transaction value and error signals

Key fields include:

- `id`
- `date`
- `client_id`
- `card_id`
- `amount`
- `use_chip`
- `merchant_id`
- `merchant_city`
- `merchant_state`
- `zip`
- `mcc`
- `errors`

### Cards

The cards dataset provides card-level descriptive attributes such as:

- card brand and type
- chip availability
- account open date
- credit limit
- security indicators

Key fields include:

- `id`
- `client_id`
- `card_brand`
- `card_type`
- `card_number`
- `expires`
- `cvv`
- `has_chip`
- `num_cards_issued`
- `credit_limit`
- `acct_open_date`
- `year_pin_last_changed`
- `card_on_dark_web`

## Bronze Layer

The Bronze layer keeps the source-aligned datasets as close to raw as possible.

Models:

- [bronze_users.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/bronze/bronze_users.sql)
- [bronze_transactions.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/bronze/bronze_transactions.sql)
- [bronze_cards.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/bronze/bronze_cards.sql)

Behavior:

- direct ingestion from declared dbt sources
- no business logic
- no heavy cleaning
- suitable as a reproducible raw layer for debugging and lineage

Metadata and model tests are documented in [properties.yml](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/bronze/properties.yml).

## Silver Layer

The Silver layer standardizes and cleans the Bronze data. This is where typing, trimming, normalization, and core validation logic are applied.

### Silver Models

- [silver_users.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/silver/silver_users.sql)
- [silver_transactions.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/silver/silver_transactions.sql)
- [silver_cards.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/silver/silver_cards.sql)
- [int_user_transactions.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/silver/int_user_transactions.sql)

### Cleaning and Standardization Performed

Users:

- casts identifiers and numeric attributes
- cleans and casts income and debt fields
- standardizes latitude and longitude types
- trims text fields such as gender and address

Transactions:

- casts identifiers and merchant fields
- cleans and casts `amount`
- parses transaction timestamps
- normalizes state formatting
- standardizes null handling for `errors`

Cards:

- casts identifiers
- parses expiration and account open dates
- cleans and casts `credit_limit`
- standardizes boolean-style fields such as `has_chip` and `card_on_dark_web`

### Intermediate Silver Model

[int_user_transactions.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/silver/int_user_transactions.sql) is an enriched intermediate model at transaction grain. It joins:

- transactions to users on `client_id = id`
- transactions to cards on `card_id = id`

This model is useful for:

- analytical exploration
- debugging joins before publishing Gold models
- validating user and card enrichment at transaction grain

Silver metadata and tests are documented in [properties.yml](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/silver/properties.yml).

## Gold Layer

The Gold layer exposes the business-facing semantic models used for reporting, KPI generation, and downstream analysis.

### Core Semantic Models

- [dim_users.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/gold/dim_users.sql)
- [dim_cards.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/gold/dim_cards.sql)
- [fact_transactions.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/gold/fact_transactions.sql)

#### `dim_users`

One row per user with demographic, geographic, financial, and credit attributes.

#### `dim_cards`

One row per card with card type, limit, chip/security flags, and account details.

#### `fact_transactions`

One row per transaction with:

- transaction timestamp
- user and card foreign keys
- merchant attributes
- amount
- payment behavior
- error indicators

This model is configured as an incremental model to demonstrate scalable event-table loading. It uses:

- `transaction_id` as the `unique_key`
- `transaction_date` as the incremental watermark boundary

### Gold Metric Marts

- [fct_daily_transactions.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/gold/fct_daily_transactions.sql)
- [dim_user_segments.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/gold/dim_user_segments.sql)
- [fct_cohort_retention_monthly.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/gold/fct_cohort_retention_monthly.sql)
- [fct_transactions_by_time_bucket.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/gold/fct_transactions_by_time_bucket.sql)

#### `fct_daily_transactions`

Daily rollup of:

- transaction count
- active users
- active cards
- total amount
- average transaction amount
- error counts
- chip vs non-chip usage

#### `dim_user_segments`

Derived segmentation table with bands for:

- age
- income
- credit score
- debt

#### `fct_cohort_retention_monthly`

Monthly cohort retention model where:

- `cohort_month` is the first month a user transacted
- `activity_month` is a month in which that cohort returned
- `months_since_cohort` measures elapsed months from acquisition
- `retention_rate` is calculated as `retained_users / cohort_size`

This supports standard cohort analysis such as:

- “Of users first active in January, how many returned in February, March, and later months?”

#### `fct_transactions_by_time_bucket`

Aggregates transactions into readable time-of-day buckets:

- `12 AM - 6 AM`
- `6 AM - 9 AM`
- `9 AM - 12 PM`
- `12 PM - 3 PM`
- `3 PM - 6 PM`
- `6 PM - 9 PM`
- `9 PM - 12 AM`

This mart helps identify peak transaction windows and compare volume, spend, and errors by time bucket.

Gold metadata and tests are documented in [properties.yml](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/models/gold/properties.yml).

## Data Quality Strategy

The project uses both dbt generic tests and singular reconciliation tests.

### Generic Tests

Configured in model `properties.yml` files across Bronze, Silver, Gold, and snapshots.

Examples:

- `not_null`
- `unique`
- `relationships`
- `accepted_values`

### Bronze to Silver Reconciliation Tests

Located in [tests](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/tests).

These check:

- row-count consistency
- ID coverage between layers

Examples:

- [test_bronze_vs_silver_users_rowcount.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/tests/test_bronze_vs_silver_users_rowcount.sql)
- [test_bronze_vs_silver_transactions_id_coverage.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/tests/test_bronze_vs_silver_transactions_id_coverage.sql)

### Silver to Gold Reconciliation Tests

These validate that semantic models preserve expected coverage from cleaned Silver models.

Examples:

- [test_silver_vs_gold_dim_users_rowcount.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/tests/test_silver_vs_gold_dim_users_rowcount.sql)
- [test_silver_vs_gold_fact_transactions_id_coverage.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/tests/test_silver_vs_gold_fact_transactions_id_coverage.sql)

### Gold Mart Reconciliation and Sanity Tests

These validate aggregates and business-facing marts.

Examples:

- [test_fact_transactions_vs_fct_daily_transactions_reconciliation.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/tests/test_fact_transactions_vs_fct_daily_transactions_reconciliation.sql)
- [test_fact_transactions_vs_fct_transactions_by_time_bucket_reconciliation.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/tests/test_fact_transactions_vs_fct_transactions_by_time_bucket_reconciliation.sql)
- [test_fct_cohort_retention_monthly_sanity.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/tests/test_fct_cohort_retention_monthly_sanity.sql)

## Snapshot Strategy

The project includes a dbt snapshot to demonstrate slowly changing dimension tracking:

- [snapshot_users.sql](/Users/yashsinghai/Desktop/Lightspeed_Project/lightspeed_project_transactions/snapshots/snapshot_users.sql)

This snapshot implements SCD Type 2 behavior for users by versioning changes in user descriptive attributes over time.

Key points:

- source model: `silver_users`
- business key: `id`
- snapshot strategy: `check`
- tracked columns include demographic, address, financial, and credit fields

When executed with `dbt snapshot`, dbt manages:

- `dbt_valid_from`
- `dbt_valid_to`

This enables point-in-time analysis of changing user attributes.

## Project Structure

```text
lightspeed_project_transactions/
├── models/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── source/
├── snapshots/
├── tests/
├── macros/
├── dbt_project.yml
└── README.md
```

## How to Run

From the project root:

```bash
dbt deps
dbt debug
dbt run
dbt test
```

To run specific layers:

```bash
dbt run --select bronze
dbt run --select silver
dbt run --select gold
```

To build and test a specific model:

```bash
dbt run --select fact_transactions
dbt test --select fact_transactions
```

To execute the user snapshot:

```bash
dbt snapshot --select snapshot_users
dbt test --select snapshot_users
```

To run a specific singular test:

```bash
dbt test --select test_fact_transactions_vs_fct_daily_transactions_reconciliation
```

## Notes and Assumptions

- This project is currently configured to use the `lightspeed_project_transactions` dbt profile.
- The repository reflects a Databricks-oriented dbt setup.
- Source freshness checks are not configured because no reliable ingestion timestamp column has been established yet.
- Time-bucket analysis assumes `transaction_date` includes usable time-of-day information.
- Incremental logic in `fact_transactions` uses a timestamp watermark strategy suitable for append-heavy event data. If late-arriving updates exist, the strategy should be widened or adjusted.

## What This Project Demonstrates

This project demonstrates practical analytics engineering skills across:

- layered dbt model design
- data cleaning and type standardization
- fact and dimension modeling
- retention and behavioral analytics
- generic and custom data quality testing
- incremental modeling
- SCD Type 2 snapshot design

## Next Possible Enhancements

- add lifecycle-state models beyond cohort retention
- add a churn or inactivity mart
- add CI execution for `dbt run` and `dbt test`
- add seeds or mock data for local reproducibility
- add exposure definitions for dashboards
- add source freshness once ingestion metadata is available

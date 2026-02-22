# Retail Lakehouse Project 1 — Databricks

This project builds a batch retail data pipeline on Databricks using the classic Bronze → Silver → Gold medallion architecture. It handles slow-changing customer/product data with SCD2 tracking and runs automated quality checks at the end of every batch.

## What We're Using

The stack is Databricks with PySpark and Spark SQL, Unity Catalog for governance, and Delta tables living under `main.retail_p1`.

## The Data

We're working with the public [Kaggle Olist Ecommerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Drop these six CSV files into `/Volumes/main/retail_p1/raw/olist/` before running anything:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`
- `product_category_name_translation.csv`

## Project Structure

The project has four notebooks and a SQL file for the dashboard:

- `notebooks/01_ingest.ipynb` — lands raw data into Bronze
- `notebooks/02_transform.ipynb` — cleans and shapes data into Silver
- `notebooks/03_serve.ipynb` — aggregates business metrics into Gold
- `notebooks/04_tests.ipynb` — runs data quality checks
- `sql/dashboard_queries.sql` — queries wired to the BI dashboard

**Run the notebooks in order, top to bottom.** Each one depends on the previous.

## Notebook Widgets (Optional Overrides)

You don't need to touch these — they have sensible defaults — but they're there if you need to reprocess a specific batch or tune the test thresholds.

| Notebook       | Widget            | Default           |
| -------------- | ----------------- | ----------------- |
| `01_ingest`    | `batch_id`        | today's UTC date  |
| `01_ingest`    | `source_prefix`   | `olist`           |
| `02_transform` | `batch_id`        | today's UTC date  |
| `04_tests`     | `run_id`          | current timestamp |
| `04_tests`     | `freshness_hours` | `168` (7 days)    |
| `04_tests`     | `fail_on_error`   | `false`           |

## Tables and What They Hold

### Bronze — Raw Ingestion

Minimal transformation. We just land the data and stamp it with a `_batch_id` and `_ingest_ts` so we always know when and where a row came from.

- **`bronze_orders`** — order_id, customer_id, product_id, order timestamp, quantity, price, status, channel
- **`bronze_customers`** — customer_id, email, city, country, last updated
- **`bronze_products`** — product_id, category, brand, list price, last updated

### Silver — Cleaned and Shaped

This is where business logic kicks in. Bad rows get quarantined, customer history is tracked across changes, and products are deduplicated.

- **`silver_orders_clean`** — validated orders ready for aggregation
- **`silver_orders_rejects`** — rows that failed validation (kept for auditing)
- **`silver_customers_scd2`** — full customer history with SCD2; every change to a customer record gets a new row, and the old one is closed out with an end date and `is_current = false`
- **`silver_products_latest`** — one row per product, latest version only

### Gold — Business Metrics

Aggregated and ready for dashboards or downstream consumption.

- **`gold_daily_revenue`** — gross revenue, net revenue, order count, and AOV per day
- **`gold_customer_ltv`** — customer lifetime value at 90-day, 180-day, and all-time windows
- **`gold_category_performance`** — revenue, units sold, and return rate per category per day

### Data Quality Log

- **`dq_results`** — every check that ran: its name, type, pass/fail status, how many rows failed, the threshold it was tested against, and when it ran

## How to Know It's Working

Run these checks after a full pipeline execution:

**No duplicate order lines in Silver** — this should return 0:

```sql
SELECT COUNT(*) AS dup_groups
FROM (
  SELECT order_id, product_id, COUNT(*) c
  FROM main.retail_p1.silver_orders_clean
  GROUP BY order_id, product_id
  HAVING c > 1
);
```

**SCD2 is closing old rows correctly** — customers with more than one history row should have exactly one `is_current = true`:

```sql
SELECT customer_id, COUNT(*) AS rows_total,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
FROM main.retail_p1.silver_customers_scd2
GROUP BY customer_id
HAVING rows_total > 1
ORDER BY rows_total DESC;
```

**At least 8 DQ checks logged today:**

```sql
SELECT COUNT(*) AS checks_logged
FROM main.retail_p1.dq_results
WHERE run_ts >= current_timestamp() - INTERVAL 1 DAY;
```

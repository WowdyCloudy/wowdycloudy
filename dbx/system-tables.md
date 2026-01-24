# Databricks System Tables

## Accessing System Tables
Workspace users need to have _SELECT_ and _USE_ permissions on the `system` schemata before they can query system tables. An account
administrator can grant these rights or, alternatively, create a copy or View under a schema that users can already access.
The following SQL query creates a View on top of the system table `system.billing.list_prices` in an example schema 
`your_catalog.your_schema`:
```SQL
CREATE VIEW your_catalog.your_schema.list_prices_view AS (
  SELECT * FROM system.billing.list_prices
)
```

Some system tables like `system.billing.usage` have a retention period, their contents can be preserved by appending their 
records to a dedicated table periodically.

## SKU Queries
Collecting a list of region suffixes by filtering the SKU name for a widely available service, serverless job comput
```SQL
SELECT DISTINCT sku_name FROM system.billing.list_prices
WHERE sku_name LIKE SOME('%JOBS_SERVERLESS_COMPUTE%', '%AUTOMATED_SERVERLESS_COMPUTE%')
ORDER BY sku_name
```
RESULT:

A list of all serverless SKUs for a specific workspace region and tier can be retrieved with this query:
```SQL
DECLARE VARIABLE region STRING DEFAULT 'US_EAST_N_VIRGINIA'; -- Adapt
DECLARE VARIABLE tier STRING DEFAULT 'PREMIUM'; -- or 'ENTERPRISE'
DECLARE VARIABLE sku_pattern STRING DEFAULT tier || "%SERVERLESS%" || region; 

SELECT sku_name, pricing.default AS DBU_list_price, pricing.effective_list.default
AS DBU_effective_price, currency_code, price_start_time
FROM system.billing.list_prices lp_1 
WHERE price_start_time = (
  SELECT MAX(price_start_time) 
  FROM system.billing.list_prices lp_2
  WHERE lp_1.sku_name = lp_2.sku_name AND lp_2.sku_name LIKE sku_pattern
)
```

Sample results:
```text
sku_name	                                                        DBU_list_price	DBU_effective_price currency_code	price_start_time
PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_US_EAST_N_VIRGINIA	        0.07            0.07	            USD	                2013-01-01T00:00:00.000Z
PREMIUM_DATABASE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA                  0.40	        0.20	            USD	                2025-02-02T08:00:00.000Z
PREMIUM_SERVERLESS_SQL_COMPUTE_US_EAST_N_VIRGINIA	                0.70	        0.70	            USD	                2022-05-12T00:00:00.000Z
PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA	        0.75	        0.75	            USD	                2025-04-30T23:59:59.000Z
PREMIUM_SERVERLESS_REAL_TIME_INFERENCE_LAUNCH_US_EAST_N_VIRGINIA	0.07	        0.07	            USD	                2013-01-01T00:00:00.000Z
PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA	                0.35	        0.35	            USD	                2025-04-30T23:59:59.000Z
```

## Serverless Queries

The total DBUs and costs for all job runs using serverless resources during the last two months:
```SQL
WITH job_serverless_consumption AS (
    SELECT usage_metadata.job_id, usage_metadata.job_run_id, usage_quantity, usage.usage_unit,
        (usage_quantity * pricing.effective_list.default) AS cost, currency_code AS currency, usage.sku_name,
        usage_start_time, usage_end_time, concat(usage.account_id, '#', workspace_id) AS workspace
    FROM system.billing.usage usage LEFT JOIN system.billing.list_prices prices
    ON usage.sku_name = prices.sku_name AND price_start_time <= usage_start_time AND (price_end_time IS NULL
        OR price_end_time >= usage_start_time)
    WHERE usage_date >= CURRENT_DATE() - INTERVAL 2 MONTHS AND product_features.is_serverless = true
        AND usage_metadata.job_id IS NOT NULL AND usage_metadata.job_run_id IS NOT NULL
)

SELECT job_id, job_run_id, SUM(usage_quantity) AS total_usage, usage_unit, SUM(cost) AS total_cost,
    currency, sku_name, MIN(usage_start_time) AS usage_start, MAX(usage_end_time) AS usage_end, workspace
FROM job_serverless_consumption
GROUP BY ALL ORDER BY total_cost DESC
```
Sample result:
```text
job_id	        job_run_id	total_usage usage_unit  total_cost  currency       sku_name                                        usage_start                     usage_end                    workspace
7947669700	238850574	1.67	    DBU	        0.65        USD	        PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND	2026-01-08T20:00:00.000Z	2026-01-08T21:00:00.000Z	123#456
7947669700	315090347	1.51	    DBU	        0.59	    USD	        PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND	2026-01-08T20:00:00.000Z	2026-01-08T21:00:00.000Z	123#456
1072763629	847260285	0.25	    DBU	        0.10	    USD	        PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND	2026-01-08T20:00:00.000Z	2026-01-08T21:00:00.000Z	123#456
10727636298	385370758	0.25	    DBU	        0.10	    USD	        PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND	2026-01-08T21:00:00.000Z	2026-01-08T22:00:00.000Z	123#456
10727636298	906176848	0.25	    DBU	        0.09	    USD	        PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND	2026-01-08T20:00:00.000Z	2026-01-08T21:00:00.000Z	123#456
```

A query for aggregating costs per serverless DLT pipeline and warehouse for the last two months, subsuming multiple pipeline
runs or queries from different users:
```sql
WITH dlt_warehouse_consumption AS (
    SELECT coalesce(usage_metadata.dlt_pipeline_id, usage_metadata.warehouse_id) AS id, billing_origin_product
        AS product, usage_quantity, usage.usage_unit, (usage_quantity * pricing.effective_list.default) AS cost,
        currency_code AS currency, usage.sku_name, concat(usage.account_id, '#', workspace_id) AS workspace
    FROM system.billing.usage usage LEFT JOIN system.billing.list_prices prices
    ON usage.sku_name = prices.sku_name AND price_start_time <= usage_start_time
        AND (price_end_time IS NULL or price_end_time >= usage_start_time)
    WHERE usage_date >= CURRENT_DATE() - INTERVAL 2 MONTHS AND product_features.is_serverless = true AND
        (usage_metadata.dlt_pipeline_id IS NOT NULL OR usage_metadata.warehouse_id IS NOT NULL)
)

SELECT id, SUM(usage_quantity) AS usage, usage_unit, SUM(cost) AS total_cost, currency, sku_name,
  product, workspace
FROM dlt_warehouse_consumption GROUP BY ALL
```

Sample result:
```text
id	        usage	usage_unit  total_cost  currency	sku_name                                        product workspace
8985e30b-7cfe	5.98	DBU	    2.33	    USD         PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND  DLT     123#456
ae104b9d-d5ab	1.10	DBU	    0.43	    USD         PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND  DLT     123#456
d42b5ddfb81f6	14.42	DBU	    13.13	    USD         PREMIUM_SERVERLESS_SQL_COMPUTE_EUROPE_IRELAND   SQL     123#456
3ed4c14032113	12.76	DBU	    11.61	    USD         PREMIUM_SERVERLESS_SQL_COMPUTE_EUROPE_IRELAND   SQL     123#456

```

## Classic Compute Queries
Coming Soon

## Optimization Queries
Coming Soon

## Metric Queries
Coming Soon

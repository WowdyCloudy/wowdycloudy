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
PREMIUM_DATABASE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA              0.40	        0.20	            USD	                2025-02-02T08:00:00.000Z
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
job_id          job_run_id  total_usage usage_unit  total_cost  currency       sku_name                                        usage_start                     usage_end            workspace
7947669700      238850574   1.67	    DBU	        0.65        USD	        PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND	2026-01-08T20:00:00.000Z	2026-01-08T21:00:00.000Z    123#456
7947669700      315090347   1.51	    DBU	        0.59	    USD	        PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND	2026-01-08T20:00:00.000Z	2026-01-08T21:00:00.000Z    123#456
1072763629      847260285   0.25	    DBU	        0.10	    USD	        PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND	2026-01-08T20:00:00.000Z	2026-01-08T21:00:00.000Z    123#456
10727636298     385370758   0.25	    DBU	        0.10	    USD	        PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND	2026-01-08T21:00:00.000Z	2026-01-08T22:00:00.000Z    123#456
10727636298     906176848   0.25	    DBU	        0.09	    USD	        PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND	2026-01-08T20:00:00.000Z	2026-01-08T21:00:00.000Z    123#456
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

## Databricks SQL Warehouse Queries

Retrieving the latest configurations for all active SQL warehouses:
```sql
WITH wh_configs AS (
    SELECT warehouse_id, warehouse_name, change_time, warehouse_type, warehouse_size, min_clusters, max_clusters, auto_stop_minutes, account_id, workspace_id, ROW_NUMBER() OVER (PARTITION BY account_id, workspace_id, warehouse_id ORDER BY change_time DESC) change, delete_time
    FROM system.compute.warehouses
    QUALIFY change = 1
)

SELECT * FROM wh_configs WHERE delete_time IS NULL
```

Sample result:
```text
warehouse_id	warehouse_name      change_time                 warehouse_type  warehouse_size  min_clusters	max_clusters    auto_stop_minutes	account_id	        workspace_id	change	delete_time
89d81113221c9	MyActiveWarehouse   2026-02-12T07:49:28.534Z    SERVERLESS      2X_SMALL        1               2               5	                923dc6cb-...    	320000000000	1       null
```

The total warehouse costs and DBU consumption during the last 3 months can be determined with the following query:
```sql
WITH wh_usage AS (
    SELECT CONCAT(u.account_id, '#', workspace_id) workspace, u.usage_metadata.warehouse_id, usage_quantity DBUs, (pricing.effective_list.default * usage_quantity) costs, currency_code currency
    FROM system.billing.usage u JOIN system.billing.list_prices lp
    ON u.sku_name = lp.sku_name AND price_start_time <= usage_start_time AND (price_end_time IS NULL OR price_end_time >= usage_start_time)
    WHERE u.usage_metadata.warehouse_id IS NOT NULL AND usage_date >= CURRENT_DATE() - INTERVAL 3 MONTHS
)

SELECT warehouse_id, SUM(costs) total_cost, currency, SUM(DBUs) total_DBUs, workspace
FROM wh_usage GROUP BY ALL
```

Sample result:
```text
warehouse_id	total_cost  currency    total_DBUs  workspace
89d81113221c9	41.51238    USD         45.61       123#456
x42b5ddvb81f6	17.73463    USD         19.48       123#456
5ed4c14032123	11.61275    USD         12.76       123#456
```

The query below aggregates the time spent per cluster size for each warehouse during the last three months:
```sql
WITH time_spent AS (
    SELECT account_id, workspace_id, warehouse_id, cluster_count, TIMESTAMPDIFF(SECOND, event_time, LEAD(event_time) OVER (PARTITION BY account_id, workspace_id, warehouse_id ORDER BY event_time)) duration_sec
    FROM system.compute.warehouse_events
    WHERE event_time >= CURRENT_DATE() - INTERVAL 3 MONTHS
)

SELECT warehouse_id, cluster_count, ROUND(SUM(duration_sec) / 60, 2) minutes_running, CONCAT(account_id, '#', workspace_id) workspace
FROM time_spent WHERE cluster_count > 0 GROUP BY ALL ORDER BY workspace, warehouse_id, minutes_running DESC
```

Sample result:
```text
warehouse_id	       cluster_count  minutes_running   workspace
3ed4c14032113579       2              39.18             123#456
3ed4c14032113579       1              19.10             123#456
8d81113021c93257       1              73.52             123#456
8d81113021c93257       2              21.53             123#456
d42b5ddfb81f6e38       1              29.17             123#456
```

An approximation for comparing the time warehouses spent on executing workloads versus being active but in an "idle"
state:

```sql
DECLARE OR REPLACE VARIABLE interv_start STRING DEFAULT '2026-01-01'; -- adjust timeframe start
DECLARE OR REPLACE VARIABLE interv_end STRING DEFAULT '2026-03-15'; -- adjust timeframe end

WITH wh_queries AS ( -- warehouse queries within timeframe
    SELECT account_id, workspace_id, compute.warehouse_id, statement_id, start_time, end_time
    FROM system.query.history WHERE compute.warehouse_id IS NOT NULL AND execution_duration_ms IS NOT NULL AND start_time < end_time AND start_time >= interv_start AND end_time <= interv_end
), wh_events AS ( -- warehouse events with their subsequent event timestamps
    SELECT account_id, workspace_id, warehouse_id, event_time, LEAD(event_time) OVER (PARTITION BY account_id, workspace_id, warehouse_id ORDER BY event_time) next_event, cluster_count
    FROM system.compute.warehouse_events
    WHERE event_time BETWEEN(interv_start) AND (interv_end)
), running_events AS ( -- warehouse events other than starting & stopped with overlapping queries
    SELECT * EXCEPT (q.account_id, q.workspace_id, q.warehouse_id)
    FROM wh_events e LEFT JOIN wh_queries q ON q.account_id = e.account_id AND q.workspace_id = e.workspace_id AND q.warehouse_id = e.warehouse_id AND start_time < next_event AND end_time > event_time
    WHERE cluster_count > 0
), queried_events AS ( -- one second slots with overlapping queries
    SELECT CONCAT(account_id, '#', workspace_id) workspace, warehouse_id, explode(sequence(event_time, next_event, INTERVAL 1 SECOND)) slot_ts, date_trunc('SECOND', slot_ts) slot_sec, CASE WHEN start_time <= slot_ts AND end_time >= slot_ts THEN 1 ELSE 0 END covered
    FROM running_events
), agg_slots AS ( -- aggregating executed queries of seconds slots 
    SELECT workspace, warehouse_id, slot_sec, SUM(covered) exec_queries FROM queried_events GROUP BY ALL
)

SELECT warehouse_id, ROUND((COUNT(exec_queries) FILTER (WHERE exec_queries = 0)) / 60, 2) minutes_active_but_idle, ROUND((COUNT(exec_queries) FILTER(WHERE exec_queries > 0)) / 60, 2) minutes_active_busy, workspace
FROM agg_slots GROUP BY ALL
```

Sample result:
```text
warehouse_id        minutes_active_but_idle    minutes_active_busy      workspace
8d81113021c93257    616.89                      68.81                   123#456
3ed4c14032113579    185.20                      6.80                    123#456
d42b5ddfb81f6e38    277.87                      16.54                   123#456

```

## Cost Optimization Queries
The query below identifies the most expensive serverless job runs (using list prices) which were **performance optimized** and may become cheaper after switching
to the **standard performance mode**. The performance mode can be switched on the Databricks workspace UI which is shown in [this screenshot](https://github.com/WowdyCloudy/wowdycloudy/blob/main/resources/images/PerformanceOptimized.png).

```sql
WITH job_usage_opt AS (
 SELECT usage_metadata.job_name, usage_metadata.job_id, usage_metadata.job_run_id, identity_metadata.run_as,
 CONCAT(account_id, '#', workspace_id) workspace, * EXCEPT(account_id, workspace_id, usage_metadata, identity_metadata, product_features)
 FROM system.billing.usage
 WHERE usage_metadata.job_id IS NOT NULL AND usage_metadata.job_run_id IS NOT NULL AND product_features.performance_target = "PERFORMANCE_OPTIMIZED"
), job_usage_cost AS (
 SELECT * EXCEPT (prices.sku_name), job_usage_opt.usage_quantity * prices.pricing.default costs
 FROM job_usage_opt LEFT JOIN system.billing.list_prices prices ON job_usage_opt.sku_name = prices.sku_name
 AND prices.price_start_time <= job_usage_opt.usage_start_time AND (prices.price_end_time >= job_usage_opt.usage_start_time OR prices.price_end_time IS NULL)
)

SELECT job_name, job_id, job_run_id, run_as, SUM(costs) costs, currency_code currency, SUM(usage_quantity) DBUs, sku_name, workspace
FROM job_usage_cost
GROUP BY ALL
ORDER BY costs DESC
```

Sample output:
```text
job_name        job_id      job_run_id  run_as	    costs   currency    DBUs    sku_name                                        workspace
ETL_Ingestion	684766970   238850      myself@     6.51    USD         16.0    PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND  123#456
ETL_Ingestion	684766970   315090      myself@     5.91    USD         15.1    PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND  123#456
Benchmarking	594751973   929337      myself@     0.63    USD         1.61    PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND  123#456
ML_Training     484066270   773466      myself@     0.62    USD         1.50    PREMIUM_JOBS_SERVERLESS_COMPUTE_EUROPE_IRELAND  123#456
```

## Classic Compute Queries
Coming Soon

## Optimization Queries
Coming Soon

## Metric Queries
Coming Soon

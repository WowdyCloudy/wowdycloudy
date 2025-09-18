## Latest Serverless List Prices

```SQL
SELECT
  sku_name,
  pricing.default AS DBU_list_price,
  currency_code,
  account_id
FROM
  system.billing.list_prices lp1
WHERE
  price_start_time = (
    SELECT
      MAX(price_start_time)
    FROM
      system.billing.list_prices lp2
    WHERE
      lp1.sku_name = lp2.sku_name
      AND lp2.sku_name LIKE '%SERVERLES%'
  )
SORT BY
  sku_name
```

Sample results:
```bash
sku_name	                                      DBU_list_price    currency_code account_id
PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_EUROPE_LONDON        0.79	USD             5d02e-...
PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA   0.75	USD             5d02e-...
PREMIUM_DATABASE_SERVERLESS_COMPUTE_AP_JAKARTA              0.50	USD             5d02e-...
PREMIUM_DATABASE_SERVERLESS_COMPUTE_AP_SEOUL                0.48	USD             5d02e-...
```

## Most Compute-Intensive Queries
```SQL
SELECT
  statement_id,
  compute.type AS compute_type,
  executed_by_user_id AS user,
  SUM(total_duration_ms) AS wall_clock_time,
  SUM(total_task_duration_ms) AS compute_time,
  MIN(start_time) AS start_time,
  MAX(end_time) AS end_time,
  CONCAT(account_id, '#', workspace_id) AS workspace
FROM
  system.query.history
GROUP BY
  ALL
ORDER BY
  compute_time DESC
```

Sample results:
```bash
statement_id    compute_type      user  wall_clock_time compute_time  start_time                    end_time                      workspace
01f094c7-...	WAREHOUSE	        71	  19920	          11421	        2025-09-18T19:34:39.741Z	2025-09-18T19:34:59.661Z	5d01e-...
f16cd024-...	SERVERLESS_COMPUTE	71	  18479	          6145	        2025-09-18T20:02:35.060Z	2025-09-18T20:02:53.539Z	5d01e-...
8dc80bb5-...	SERVERLESS_COMPUTE	71	  7737	          1521	        2025-09-18T19:07:38.119Z	2025-09-18T19:07:45.856Z	5d01e-...
```

## Approximate Cost Per Warehouse Query
```SQL
with warehouse_queries AS (
  SELECT
    statement_id,
    compute.warehouse_id,
    executed_by_user_id AS user,
    SUM(total_duration_ms) AS wall_clock_time,
    SUM(total_task_duration_ms) AS compute_time,
    MIN(start_time) AS start_time,
    MAX(end_time) AS end_time,
    account_id,
    workspace_id
  FROM
    system.query.history
  WHERE
    compute.type = "WAREHOUSE"
  GROUP BY
    ALL
),
warehouse_skus AS (
  SELECT
    usage_metadata.warehouse_id,
    sku_name,
    account_id,
    workspace_id
  FROM
    system.billing.usage
  WHERE
    usage_metadata.warehouse_id IS NOT null
  GROUP BY
    ALL
),
wh_joined AS (
  SELECT
    *
  FROM
    warehouse_queries wq
      LEFT JOIN warehouse_skus skus USING (warehouse_id, account_id, workspace_id)
)
SELECT
  statement_id,
  warehouse_id,
  user,
  start_time,
  end_time,
  wh_joined.sku_name,
  usage_unit,
  pricing.effective_list.default AS actual_dbu_price,
  ((compute_time / 3600000) * actual_dbu_price) AS approx_cost,
  currency_code,
  wall_clock_time,
  compute_time,
  CONCAT(wh_joined.account_id, '#', workspace_id) AS workspace
FROM
  wh_joined
    JOIN system.billing.list_prices lp
      ON wh_joined.sku_name = lp.sku_name
      AND wh_joined.account_id = lp.account_id
      AND (
        wh_joined.end_time <= lp.price_end_time
        or lp.price_end_time IS null
      )
ORDER BY
  approx_cost DESC
```

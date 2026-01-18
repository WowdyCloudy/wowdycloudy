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

Some system tables like `system.billing.usage` have a retention period, their contents can be preserved by copying them periodically.

## SKU Queries
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


## Classic Compute Queries
Coming Soon

## Optimization Queries
Coming Soon

## Metric Queries
Coming Soon

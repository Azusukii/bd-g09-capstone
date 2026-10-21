CREATE OR REPLACE TABLE staging_orders AS
SELECT o_orderkey, o_custkey, CAST(o_totalprice AS DOUBLE) AS o_totalprice
FROM raw_orders
WHERE o_orderkey IS NOT NULL;

CREATE OR REPLACE TABLE staging_lineitem AS
SELECT l_orderkey, CAST(l_extendedprice AS DOUBLE) AS l_extendedprice
FROM raw_lineitem
WHERE l_orderkey IS NOT NULL;

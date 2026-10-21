CREATE OR REPLACE TABLE order_totals AS
SELECT o.o_orderkey, SUM(CAST(l.l_extendedprice AS DOUBLE)) AS total_price
FROM staging_orders o
JOIN staging_lineitem l ON l.l_orderkey = o.o_orderkey
GROUP BY o.o_orderkey;

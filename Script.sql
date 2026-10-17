CREATE SCHEMA bronze.lakehouse;
CREATE SCHEMA silver.lakehouse;
CREATE SCHEMA gold.lakehouse;

DROP TABLE IF EXISTS bronze.lakehouse.customer;
DROP TABLE IF EXISTS bronze.lakehouse.orders;
DROP TABLE IF EXISTS bronze.lakehouse.lineitem;
DROP TABLE IF EXISTS bronze.lakehouse.nation;

SET SESSION bronze.compression_codec = 'GZIP';

CREATE TABLE bronze.lakehouse.customer
WITH (format = 'PARQUET')
AS SELECT custkey, name, address, nationkey, phone, acctbal, comment
FROM tpch.sf10.customer;

CREATE TABLE bronze.lakehouse.orders
WITH (format = 'PARQUET')
AS SELECT orderkey, custkey, orderdate
FROM tpch.sf10.orders;

CREATE TABLE bronze.lakehouse.lineitem
WITH (format = 'PARQUET')
AS SELECT orderkey, returnflag, extendedprice, discount
FROM tpch.sf10.lineitem;

CREATE TABLE bronze.lakehouse.nation
WITH (format = 'PARQUET')
AS SELECT nationkey, name
FROM tpch.sf10.nation;

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM bronze.lakehouse."customer$files";

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM bronze.lakehouse."orders$files";

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM bronze.lakehouse."lineitem$files";

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM bronze.lakehouse."nation$files";

SELECT count(*) FROM bronze.lakehouse.customer;  
SELECT count(*) FROM bronze.lakehouse.orders;    
SELECT count(*) FROM bronze.lakehouse.lineitem;  
SELECT count(*) FROM bronze.lakehouse.nation;    

SET SESSION silver.compression_codec = 'ZSTD';
CREATE TABLE silver.lakehouse.customer_nation
WITH (format = 'PARQUET')
AS
SELECT c.custkey, c.name, c.address, c.phone, c.acctbal, c.comment,
       n.name AS nation
FROM bronze.lakehouse.customer c
JOIN bronze.lakehouse.nation n ON c.nationkey = n.nationkey;

CREATE TABLE silver.lakehouse.orders_q4_1993
WITH (format = 'PARQUET', partitioning = ARRAY['month(orderdate)'])
AS
SELECT orderkey, custkey, orderdate
FROM bronze.lakehouse.orders
WHERE orderdate >= DATE '1993-10-01'
  AND orderdate <  DATE '1994-01-01';

CREATE TABLE silver.lakehouse.lineitem_returned
WITH (format = 'PARQUET')
AS
SELECT orderkey, extendedprice, discount
FROM bronze.lakehouse.lineitem
WHERE returnflag = 'R';

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM silver.lakehouse."customer_nation$files";

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM silver.lakehouse."orders_q4_1993$files";

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM silver.lakehouse."lineitem_returned$files";

SELECT count(*) FROM silver.lakehouse.customer_nation;
SELECT count(*) FROM silver.lakehouse.orders_q4_1993;
SELECT count(*) FROM silver.lakehouse.lineitem_returned;

SET SESSION gold.compression_codec = 'ZSTD';

CREATE TABLE gold.lakehouse.top20_customers
WITH (format = 'PARQUET')
AS
SELECT
  c.custkey,
  c.name,
  sum(l.extendedprice * (1 - l.discount)) AS revenue,
  c.acctbal,
  c.nation,
  c.address,
  c.phone,
  c.comment
FROM silver.lakehouse.customer_nation c
JOIN silver.lakehouse.orders_q4_1993 o ON c.custkey = o.custkey
JOIN silver.lakehouse.lineitem_returned l ON l.orderkey = o.orderkey
GROUP BY c.custkey, c.name, c.acctbal, c.nation, c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20;

SELECT count(*)                AS files,
       sum(file_size_in_bytes) AS bytes
FROM gold.lakehouse."top20_customers$files";

SELECT count(*) FROM gold.lakehouse.top20_customers;

SELECT
  c.custkey,
  c.name,
  sum(l.extendedprice * (1 - l.discount)) AS revenue,
  c.acctbal,
  n.name AS nation,
  c.address,
  c.phone,
  c.comment
FROM bronze.lakehouse.customer AS c
JOIN bronze.lakehouse.orders   AS o ON c.custkey = o.custkey
JOIN bronze.lakehouse.lineitem AS l ON l.orderkey = o.orderkey
JOIN bronze.lakehouse.nation   AS n ON c.nationkey = n.nationkey
WHERE o.orderdate >= DATE '1993-10-01'
  AND o.orderdate <  DATE '1994-01-01'
  AND l.returnflag = 'R'
GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20;

SELECT *FROM silver.lakehouse."customer_nation$snapshots"    # Все снапшоты
SELECT * FROM silver.lakehouse.customer_nation
FOR VERSION AS OF 7765250944670597700                        # Данные конкретного снапшота
ALTER TABLE silver.lakehouse.customer_nation
EXECUTE rollback_to_snapshot(7765250944670597700)            # Откат
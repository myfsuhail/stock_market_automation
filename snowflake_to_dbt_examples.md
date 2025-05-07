# Input and Output Examples

This document provides detailed examples of input SQL statements and their corresponding DBT models.

---

## Example 1: Create DBT Model by Passing SQL Statements

### Input

The following SQL statements are used for creating a staging table, deduplicating records, updating the target table, and creating a post-hook view.

```sql
-- Step 1: Create a staging table to hold the latest stock records
CREATE TABLE stocks_staging (LIKE stocks INCLUDING ALL);

-- Step 2: Insert deduplicated records into the staging table
INSERT INTO stocks_staging
WITH
unioned_stock_data AS (
    SELECT *
    FROM historical_stock_data_daily
    WHERE record_created_on > (SELECT COALESCE(MAX(record_created_on), TIMESTAMP '1900-01-01') FROM stocks)

    UNION ALL

    SELECT *
    FROM current_stock_data_daily
    WHERE record_created_on > (SELECT COALESCE(MAX(record_created_on), TIMESTAMP '1900-01-01') FROM stocks)
),
joined_dataset AS (
    SELECT 
        a.pair_id, 
        TO_CHAR(a.row_date_timestamp, 'YYYYMMDD') AS transaction_id,
        a.row_date_timestamp AS transaction_date,
        REPLACE(a.last_open, ',', '')::DECIMAL(10,4) AS last_open,
        REPLACE(a.last_close, ',', '')::DECIMAL(10,4) AS last_close,
        REPLACE(a.last_min, ',', '')::DECIMAL(10,4) AS last_min,
        REPLACE(a.last_max, ',', '')::DECIMAL(10,4) AS last_max,
        a.volume_raw::BIGINT AS volume,
        a.change_percent_raw AS change_percent,
        b.uid,
        b.primary_uid,
        b.ticker,
        b.stock_name,
        b.sector,
        b.primary_sector,
        b.secondary_sector,
        b.market_cap,
        CASE
            WHEN b.market_cap IS NULL THEN 'Unknown'
            WHEN b.market_cap > 2000000000000 THEN 'Large Cap'
            WHEN b.market_cap BETWEEN 500000000000 AND 2000000000000 THEN 'Mid Cap'
            WHEN b.market_cap BETWEEN 50000000000 AND 500000000000 THEN 'Small Cap'
            WHEN b.market_cap < 50000000000 THEN 'Micro Cap'
            ELSE 'Other'
        END AS market_cap_category,
        DATE_TRUNC('quarter', a.row_date_timestamp) AS quarter_start,
        a.record_created_on,
        ROW_NUMBER() OVER (PARTITION BY a.pair_id, TO_CHAR(a.row_date_timestamp, 'YYYYMMDD') ORDER BY a.record_created_on DESC) AS row_num 
    FROM unioned_stock_data a
    INNER JOIN assets_info b
    ON a.pair_id = b.pair_id
)
SELECT *
FROM joined_dataset
WHERE row_num = 1;

-- Step 3: Delete matching keys from target
DELETE FROM stocks
WHERE EXISTS (
    SELECT 1
    FROM stocks_staging ss
    WHERE stocks.pair_id = ss.pair_id AND stocks.transaction_id = ss.transaction_id
);

-- Step 4: Insert the fresh rows from staging into target
INSERT INTO stocks
SELECT * FROM stocks_staging;

-- Post-hook statement to create a view
CREATE OR REPLACE VIEW stock_vw AS 
SELECT * 
FROM stocks 
WHERE market_cap_category != 'Unknown';

# Example for the Code Converter: Snowflake SQL to DBT Model

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

```

### Output

After processing the above SQL statements, the resulting DBT model looks like this:

**NOTICE:** The model is created as incremental, **but unique_key is not provided in the config block**. Therefore, developer understanding of the code, and validation is unavoidable.

```sql

{{ config(
    materialized='incremental',  -- Materialization type
    post_hook="CREATE OR REPLACE VIEW stock_vw AS SELECT * FROM {{ this }} WHERE market_cap_category != 'Unknown';"
) }}

-- Incremental model to process stock data
WITH
-- Step 1: Union historical and current stock data
unioned_stock_data AS (
    SELECT *
    FROM {{ ref('historical_stock_data_daily') }}
    WHERE record_created_on > (SELECT COALESCE(MAX(record_created_on), TIMESTAMP '1900-01-01') FROM {{ this }})

    UNION ALL

    SELECT *
    FROM {{ ref('current_stock_data_daily') }}
    WHERE record_created_on > (SELECT COALESCE(MAX(record_created_on), TIMESTAMP '1900-01-01') FROM {{ this }})
),

-- Step 2: Join with asset information and deduplicate
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
    INNER JOIN {{ ref('assets_info') }} b
    ON a.pair_id = b.pair_id
)

-- Step 3: Final selection of deduplicated records
SELECT *
FROM joined_dataset
WHERE row_num = 1;

```


---


## Example 2: Create DBT Model with & w/o design hints

### Input: Approach 1: Without design hints

The following statements needs to be converted into DBT Model.

```sql

INSERT INTO supply_chain.tables_analytics
SELECT 'supply_chain' AS database_name
    , 'table1' AS table_name
    , COUNT(*) AS records
    , SUM(CASE WHEN source_desc = 'TC1' THEN 1 ELSE 0 END) AS tc1
    , SUM(CASE WHEN source_desc = 'TC2' THEN 1 ELSE 0 END) AS tc2
    , NOW() AS evaluation_time	
FROM supply_chain.table1;

INSERT INTO supply_chain.tables_analytics
SELECT 'supply_chain' AS database_name
    , 'table2' AS table_name
    , COUNT(*) AS records
    , SUM(CASE WHEN source_desc = 'TC1' THEN 1 ELSE 0 END) AS tc1
    , SUM(CASE WHEN source_desc = 'TC2' THEN 1 ELSE 0 END) AS tc2
    , NOW() AS evaluation_time	
FROM supply_chain.table2;

INSERT INTO supply_chain.tables_analytics
SELECT 'supply_chain' AS database_name
    , 'table3' AS table_name
    , COUNT(*) AS records
    , SUM(CASE WHEN source_desc = 'TC1' THEN 1 ELSE 0 END) AS tc1
    , SUM(CASE WHEN source_desc = 'TC2' THEN 1 ELSE 0 END) AS tc2
    , NOW() AS evaluation_time	
FROM supply_chain.table3;

```

### Output:

```sql

{{ config(
    materialized='table'
) }}

-- Combine the logic for all tables into a single dbt model
WITH table1_data AS (
    SELECT 
        'supply_chain' AS database_name,
        'table1' AS table_name,
        COUNT(*) AS records,
        SUM(CASE WHEN source_desc = 'TC1' THEN 1 ELSE 0 END) AS tc1,
        SUM(CASE WHEN source_desc = 'TC2' THEN 1 ELSE 0 END) AS tc2,
        NOW() AS evaluation_time
    FROM {{ ref('table1') }}
),
table2_data AS (
    SELECT 
        'supply_chain' AS database_name,
        'table2' AS table_name,
        COUNT(*) AS records,
        SUM(CASE WHEN source_desc = 'TC1' THEN 1 ELSE 0 END) AS tc1,
        SUM(CASE WHEN source_desc = 'TC2' THEN 1 ELSE 0 END) AS tc2,
        NOW() AS evaluation_time
    FROM {{ ref('table2') }}
),
table3_data AS (
    SELECT 
        'supply_chain' AS database_name,
        'table3' AS table_name,
        COUNT(*) AS records,
        SUM(CASE WHEN source_desc = 'TC1' THEN 1 ELSE 0 END) AS tc1,
        SUM(CASE WHEN source_desc = 'TC2' THEN 1 ELSE 0 END) AS tc2,
        NOW() AS evaluation_time
    FROM {{ ref('table3') }}
)

-- Union all the data together
SELECT * FROM table1_data
UNION ALL
SELECT * FROM table2_data
UNION ALL
SELECT * FROM table3_data


```

---

### Input: Approach 2: With design hints as per developer

**Notice the developer hints in the below code snippet. And compare the results to understand how the output is aligned towards the developer's requirement**

```sql

Please consider below design/hint points during conversion:
1. Table should be materialized as incremental with unique key as database_name, table_name, evaluation_time
2. Use FOR Loop and List to iterate table name dont use multiple select statements.


INSERT INTO supply_chain.tables_analytics
SELECT 'supply_chain' AS database_name
    , 'table1' AS table_name
    , COUNT(*) AS records
    , SUM(CASE WHEN source_desc = 'TC1' THEN 1 ELSE 0 END) AS tc1
    , SUM(CASE WHEN source_desc = 'TC2' THEN 1 ELSE 0 END) AS tc2
    , NOW() AS evaluation_time	
FROM supply_chain.table1;

INSERT INTO supply_chain.tables_analytics
SELECT 'supply_chain' AS database_name
    , 'table2' AS table_name
    , COUNT(*) AS records
    , SUM(CASE WHEN source_desc = 'TC1' THEN 1 ELSE 0 END) AS tc1
    , SUM(CASE WHEN source_desc = 'TC2' THEN 1 ELSE 0 END) AS tc2
    , NOW() AS evaluation_time	
FROM supply_chain.table2;

INSERT INTO supply_chain.tables_analytics
SELECT 'supply_chain' AS database_name
    , 'table3' AS table_name
    , COUNT(*) AS records
    , SUM(CASE WHEN source_desc = 'TC1' THEN 1 ELSE 0 END) AS tc1
    , SUM(CASE WHEN source_desc = 'TC2' THEN 1 ELSE 0 END) AS tc2
    , NOW() AS evaluation_time	
FROM supply_chain.table3;


```


### Output

**NOTICE: The Output of the Converter aligns with the design hints provided by the developer.**

```sql

{{ 
    config(
        materialized='incremental',
        unique_key=['database_name', 'table_name', 'evaluation_time']
    ) 
}}

-- List of tables to iterate over
{% set tables = ['table1', 'table2', 'table3'] %}

-- Union all the results for each table
with aggregated_data as (
    {% for table in tables %}
    select 
        'supply_chain' as database_name,
        '{{ table }}' as table_name,
        count(*) as records,
        sum(case when source_desc = 'TC1' then 1 else 0 end) as tc1,
        sum(case when source_desc = 'TC2' then 1 else 0 end) as tc2,
        now() as evaluation_time
    from {{ ref(table) }}
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

-- Insert into the final table
select * from aggregated_data

```


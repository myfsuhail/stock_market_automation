{{
    config(materialized='table')
}}

WITH 
volumed_stocks AS (
    SELECT pair_id
    FROM (
        SELECT 
            pair_id, 
            AVG(volume) AS volume
        FROM {{ ref('stock_data') }}
        WHERE frequency = 'daily'
        GROUP BY pair_id
    ) ss
    WHERE volume > 500000
),
-- Step 1: Rank data and calculate the SMA values for 9 and 21 periods
ranked_data AS (
    SELECT 
        pair_id,
        ticker,
        stock_name,
        primary_uid,
        primary_sector,
        secondary_sector,
        market_cap,
        market_cap_category,
        transaction_id,
        quarter_start,
        transaction_date,
        volume,
        last_min,
        last_max,
        last_open,
        last_close,
        ROW_NUMBER() OVER (PARTITION BY pair_id, ticker ORDER BY transaction_date) AS seq_num
    FROM {{ref('stock_data')}}
    WHERE frequency = 'daily'
    AND pair_id IN (SELECT pair_id FROM volumed_stocks)
),

-- Step 2: Calculate SMA for 9-period and 21-period
sma_calculations AS (
    SELECT 
        r.pair_id,
        r.ticker,
        r.stock_name,
        r.primary_uid,
        r.primary_sector,
        r.secondary_sector,
        r.market_cap,
        r.market_cap_category,
        r.quarter_start,
        r.transaction_id,
        r.transaction_date,
        r.volume,
        r.last_min,
        r.last_max,
        r.last_open,
        r.last_close,
        -- Calculate SMA 5
        AVG(r.last_close) OVER (PARTITION BY r.pair_id, r.ticker ORDER BY r.transaction_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sma_5, 
        -- Calculate SMA 9
        AVG(r.last_close) OVER (PARTITION BY r.pair_id, r.ticker ORDER BY r.transaction_date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS sma_9, 
        -- Calculate SMA 15
        AVG(r.last_close) OVER (PARTITION BY r.pair_id, r.ticker ORDER BY r.transaction_date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS sma_15, 
        -- Calculate SMA 21
        AVG(r.last_close) OVER (PARTITION BY r.pair_id, r.ticker ORDER BY r.transaction_date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS sma_21,
        -- Calculate SMA 50
        AVG(r.last_close) OVER (PARTITION BY r.pair_id, r.ticker ORDER BY r.transaction_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_50,
        -- Calculate SMA 150
        AVG(r.last_close) OVER (PARTITION BY r.pair_id, r.ticker ORDER BY r.transaction_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW) AS sma_150
    FROM ranked_data r
),

-- Step 3: Detect buy/sell signals based on SMA crossovers
sma_and_prev_sma_calc AS (
    SELECT 
        sc.pair_id,
        sc.ticker,
        sc.stock_name,
        sc.primary_uid,
        sc.primary_sector,
        sc.secondary_sector,
        sc.market_cap,
        sc.market_cap_category,
        sc.transaction_id,
        sc.quarter_start,
        sc.transaction_date,
        sc.volume,
        sc.last_min,
        sc.last_max,
        sc.last_open,
        sc.last_close,
        sc.sma_5,
        sc.sma_9,
        sc.sma_15,
        sc.sma_21,
        sc.sma_50,
        sc.sma_150,
        LAG(sc.sma_5) OVER (PARTITION BY sc.pair_id, sc.ticker ORDER BY sc.transaction_date) as prev_sma_5,
        LAG(sc.sma_9) OVER (PARTITION BY sc.pair_id, sc.ticker ORDER BY sc.transaction_date) as prev_sma_9,
        LAG(sc.sma_15) OVER (PARTITION BY sc.pair_id, sc.ticker ORDER BY sc.transaction_date) as prev_sma_15,
        LAG(sc.sma_21) OVER (PARTITION BY sc.pair_id, sc.ticker ORDER BY sc.transaction_date) as prev_sma_21,
        LAG(sc.sma_50) OVER (PARTITION BY sc.pair_id, sc.ticker ORDER BY sc.transaction_date) as prev_sma_50,
        LAG(sc.sma_150) OVER (PARTITION BY sc.pair_id, sc.ticker ORDER BY sc.transaction_date) as prev_sma_150
    FROM sma_calculations sc
)

-- Final Output: Select the buy and sell signals
SELECT *
FROM sma_and_prev_sma_calc
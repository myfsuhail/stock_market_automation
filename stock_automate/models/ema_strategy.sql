{{
    config(materialized='table')
}}

-- Step 1: Define smoothing factors for EMA calculations
WITH recursive params AS (
    SELECT 
        2.0 / (9 + 1) AS alpha_9,   -- Smoothing factor for EMA 9
        2.0 / (21 + 1) AS alpha_21, -- Smoothing factor for EMA 21
        2.0 / (50 + 1) AS alpha_50  -- Smoothing factor for EMA 50
),
-- Step 2: Filter out stocks with volume > 500000
volumed_stocks AS (
    SELECT pair_id
    FROM (
        SELECT 
            pair_id, 
            AVG(volume) AS volume
        FROM {{ ref('stock_data') }}
        WHERE frequency = 'daily'
        GROUP BY pair_id
    )
    WHERE volume > 500000
),
-- Step 3: Rank data for each stock and calculate row numbers
ranked_data AS (
    SELECT 
        * ,
        -- SMA for initial EMA calculations
        AVG(r.last_close) OVER (PARTITION BY r.pair_id, r.ticker ORDER BY r.transaction_date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS sma_9,
        AVG(r.last_close) OVER (PARTITION BY r.pair_id, r.ticker ORDER BY r.transaction_date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS sma_21,
        AVG(r.last_close) OVER (PARTITION BY r.pair_id, r.ticker ORDER BY r.transaction_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_50,
        ROW_NUMBER() OVER (PARTITION BY pair_id, ticker ORDER BY transaction_date) AS seq_num
    FROM {{ ref('stock_data') }} r
    WHERE frequency = 'daily'
    AND pair_id IN (SELECT pair_id FROM volumed_stocks)
),
-- Step 4: Calculate EMAs using window functions for each row
ema_calculations AS materialized (
    SELECT 
        r.pair_id,
        r.ticker,
        r.stock_name,
        r.primary_uid,
        r.primary_sector,
        r.secondary_sector,
        r.market_cap,
        r.market_cap_category,
        r.transaction_id,
        r.transaction_date,
        r.volume,
        r.last_min,
        r.last_max,
        r.last_open,
        r.last_close,
        r.sma_9 as ema_9,
        r.sma_21 as ema_21,
        r.sma_50 as ema_50,
        r.seq_num
    FROM ranked_data r
    cross join params
    where r.seq_num = 1

    union all

    SELECT 
        r2.pair_id,
        r2.ticker,
        r2.stock_name,
        r2.primary_uid,
        r2.primary_sector,
        r2.secondary_sector,
        r2.market_cap,
        r2.market_cap_category,
        r2.transaction_id,
        r2.transaction_date,
        r2.volume,
        r2.last_min,
        r2.last_max,
        r2.last_open,
        r2.last_close,
        (r2.last_close * alpha_9) + (ec.ema_9 * (1 - alpha_9)) as ema_9,
        (r2.last_close * alpha_21) + (ec.ema_21 * (1 - alpha_21))  as ema_21,
        (r2.last_close * alpha_50) + (ec.ema_50 * (1 - alpha_50))  as ema_50,
        r2.seq_num
    FROM ranked_data r2
    cross join params
    inner join ema_calculations ec
    on ec.pair_id = r2.pair_id
    where r2.seq_num = ec.seq_num + 1
),

-- Step 5: Detect crossovers
ema_and_prev_ema_calc AS (
    SELECT 
        ec.pair_id,
        ec.ticker,
        ec.stock_name,
        ec.primary_uid,
        ec.primary_sector,
        ec.secondary_sector,
        ec.market_cap,
        ec.market_cap_category,
        ec.transaction_id,
        ec.transaction_date,
        ec.volume,
        ec.last_min,
        ec.last_max,
        ec.last_open,
        ec.last_close,
        ec.ema_9,
        ec.ema_21,
        ec.ema_50,
        -- Detect previous EMA values using LAG
        LAG(ec.ema_9) OVER (PARTITION BY ec.pair_id, ec.ticker ORDER BY ec.transaction_date) AS prev_ema_9,
        LAG(ec.ema_21) OVER (PARTITION BY ec.pair_id, ec.ticker ORDER BY ec.transaction_date) AS prev_ema_21,
        LAG(ec.ema_50) OVER (PARTITION BY ec.pair_id, ec.ticker ORDER BY ec.transaction_date) AS prev_ema_50
    from ema_calculations ec
)
-- Final Output: Select the crossover signals
SELECT *
FROM ema_and_prev_ema_calc

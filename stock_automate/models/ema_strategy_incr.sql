{{
    config(
        materialized = 'incremental'
    )
}}

-- This dbt model calculates EMAs and supports both initial and incremental loads


-- Step 1: Define parameters
WITH recursive params AS (
    SELECT 
        2.0 / (9 + 1) AS alpha_9,    -- Smoothing factor for EMA 9
        2.0 / (21 + 1) AS alpha_21,  -- Smoothing factor for EMA 21
        2.0 / (50 + 1) AS alpha_50   -- Smoothing factor for EMA 50
),

-- Step 2: Filter out high-volume stocks
volumed_stocks AS (
    SELECT pair_id
    FROM (
        SELECT 
            pair_id, 
            AVG(volume) AS avg_volume
        FROM {{ ref('stock_data') }}  -- Replace with your source table
        WHERE frequency = 'daily'
        GROUP BY pair_id
    )
    WHERE avg_volume > 500000
),

-- Step 3: Identify new or updated data
ranked_data AS (
    SELECT 
        *,
        AVG(last_close) OVER (PARTITION BY pair_id, ticker ORDER BY transaction_date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) AS sma_9,
        AVG(last_close) OVER (PARTITION BY pair_id, ticker ORDER BY transaction_date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS sma_21,
        AVG(last_close) OVER (PARTITION BY pair_id, ticker ORDER BY transaction_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_50,
        ROW_NUMBER() OVER (PARTITION BY pair_id, ticker ORDER BY transaction_date) AS seq_num
    FROM {{ ref('stock_data') }} r
    WHERE frequency = 'daily'
      AND pair_id IN (SELECT pair_id FROM volumed_stocks)
      {% if is_incremental() %}
      AND transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
      {% endif %}
),

-- Step 4: Calculate EMAs incrementally
ema_calculations AS (
    SELECT 
        r.pair_id,
        r.ticker,
        r.stock_name,
        r.transaction_date,
        r.seq_num,
        r.sma_9 AS ema_9,
        r.sma_21 AS ema_21,
        r.sma_50 AS ema_50
    FROM ranked_data r
    CROSS JOIN params
    WHERE r.seq_num = 1

    UNION ALL

    SELECT 
        r2.pair_id,
        r2.ticker,
        r2.stock_name,
        r2.transaction_date,
        r2.seq_num,
        (r2.last_close * p.alpha_9) + (ec.ema_9 * (1 - p.alpha_9)) AS ema_9,
        (r2.last_close * p.alpha_21) + (ec.ema_21 * (1 - p.alpha_21)) AS ema_21,
        (r2.last_close * p.alpha_50) + (ec.ema_50 * (1 - p.alpha_50)) AS ema_50
    FROM ranked_data r2
    CROSS JOIN params p
    INNER JOIN ema_calculations ec
    ON ec.pair_id = r2.pair_id AND ec.seq_num + 1 = r2.seq_num
),

-- Step 5: Detect crossovers
detect_crossover_stocks AS (
    SELECT 
        ec.pair_id,
        ec.ticker,
        ec.stock_name,
        ec.transaction_date,
        ec.ema_9,
        ec.ema_21,
        ec.ema_50,
        LAG(ec.ema_9) OVER (PARTITION BY ec.pair_id, ec.ticker ORDER BY ec.transaction_date) AS prev_ema_9,
        LAG(ec.ema_21) OVER (PARTITION BY ec.pair_id, ec.ticker ORDER BY ec.transaction_date) AS prev_ema_21,
        CASE
            WHEN ec.ema_9 > ec.ema_21 AND prev_ema_9 <= prev_ema_21 THEN 'BUY'
            WHEN ec.ema_9 < ec.ema_21 AND prev_ema_9 >= prev_ema_21 THEN 'SELL'
        END AS crossover_signal
    FROM ema_calculations ec
)

-- Final Output: Combine historical and new records
SELECT *
FROM detect_crossover_stocks

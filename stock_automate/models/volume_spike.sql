WITH volume_data AS (
    -- Calculate the 30-day average volume for each stock
    SELECT 
        pair_id,
        ticker,
        stock_name,
        transaction_date,
        volume,
        AVG(volume) OVER (PARTITION BY pair_id, ticker ORDER BY transaction_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_volume_30
    FROM main_stage.stock_data
    WHERE frequency = 'daily'
),

volume_spikes AS (
    -- Identify volume spikes: Current volume exceeds 1.5 times the 30-day average volume
    SELECT 
        vd.pair_id,
        vd.ticker,
        vd.stock_name,
        vd.transaction_date,
        vd.volume,
        vd.avg_volume_30,
        CASE 
            WHEN vd.volume > 1.5 * vd.avg_volume_30 THEN 'VOLUME SPIKE'
            ELSE 'NORMAL VOLUME'
        END AS volume_status
    FROM volume_data vd
)

-- Select stocks with volume spikes
SELECT 
    pair_id,
    ticker,
    stock_name,
    transaction_date,
    volume,
    avg_volume_30,
    volume_status
FROM volume_spikes
WHERE volume_status = 'VOLUME SPIKE'
ORDER BY transaction_date DESC
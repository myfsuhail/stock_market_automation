{{
    config(materialized='table')
}}



with buy_signals as (
    select *,'sma_signal_5_15' as strategy
    from {{ref('sma_crossover')}}
    where sma_signal_5_15 = 'BUY'
    union all
    select *,'sma_signal_9_21' as strategy
    from {{ref('sma_crossover')}}
    where sma_signal_9_21 = 'BUY'
    union all
    select *,'sma_signal_21_50' as strategy
    from {{ref('sma_crossover')}}
    where sma_signal_21_50 = 'BUY'
    union all
    select *,'sma_signal_50_150' as strategy
    from {{ref('sma_crossover')}}
    where sma_signal_50_150 = 'BUY'
),
sell_signals as (
    select *,'sma_signal_5_15' as strategy
    from {{ref('sma_crossover')}}
    where sma_signal_5_15 = 'SELL'
    union all
    select *,'sma_signal_9_21' as strategy
    from {{ref('sma_crossover')}}
    where sma_signal_9_21 = 'SELL'
    union all
    select *,'sma_signal_21_50' as strategy
    from {{ref('sma_crossover')}}
    where sma_signal_21_50 = 'SELL'
    union all
    select *,'sma_signal_50_150' as strategy
    from {{ref('sma_crossover')}}
    where sma_signal_50_150 = 'SELL'
),
closed_buy_and_sell as (
    select 
        b.pair_id, 
        b.ticker, 
        b.stock_name,
        b.primary_uid,
        b.primary_sector,
        b.secondary_sector,
        b.market_cap,
        b.market_cap_category,
        b.transaction_date as buy_date, 
        s.transaction_date as sell_date,
        b.volume,
        b.last_close as buy_price,
        s.last_close as sell_price,
        to_char(b.transaction_date,'YYYY') as buy_year,
        to_char(s.transaction_date,'YYYY') as sell_year,
        'CLOSED' as status,
        b.strategy,
        row_number() over(partition by b.pair_id,b.transaction_date order by s.transaction_date) as rn
    from buy_signals b
    inner join sell_signals s
    on b.pair_id = s.pair_id
    and b.strategy = s.strategy
    and s.transaction_date > b.transaction_date
),
calculate_profit_loss AS (
    SELECT 
        *,
        -- Calculate the number of shares bought
        ROUND(100000.0 / buy_price, 2) AS shares_bought,
        -- Calculate profit or loss
        ROUND((100000.0 / buy_price) * sell_price - 100000.0, 2) AS profit_or_loss,
        -- Calculate percentage profit or loss
        ROUND(((sell_price - buy_price) / buy_price) * 100, 2) AS percent_profit_loss
    FROM closed_buy_and_sell
    where rn = 1
)
SELECT *
FROM calculate_profit_loss
ORDER BY pair_id, buy_date desc
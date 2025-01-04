with buy_signals as (
select *
from {{ ref('sma_strategy') }}
where sma_crossover_signal_21_50 = 'BUY'
),
sell_signals as (
select *
from {{ ref('sma_strategy') }}
where sma_crossover_signal_21_50 = 'SELL'
),
closed_buy_and_sell as (
select 
    b.pair_id, 
    b.ticker, 
    b.stock_name, 
    b.primary_sector,
    b.secondary_sector,
    b.market_cap_category,
    b.transaction_date as buy_date, 
    s.transaction_date as sell_date,
    b.last_close as buy_price,
    s.last_close as sell_price,
    'CLOSED' as status
from buy_signals b
inner join sell_signals s
on b.pair_id = s.pair_id
and s.transaction_date > b.transaction_date
qualify row_number() over(partition by b.pair_id,buy_date order by sell_date)=1
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
)
SELECT *
FROM calculate_profit_loss
ORDER BY pair_id, buy_date desc
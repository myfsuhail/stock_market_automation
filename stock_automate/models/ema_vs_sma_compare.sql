{{
    config(materialized='table')
}}

with ema_cte as (
    select buy_year,market_cap_category,strategy, sum(profit_or_loss) as pl, count(*) as no_of_trades
    from {{ref('ema_backtest')}}
    group by market_cap_category, strategy,  buy_year
    order by buy_year ,market_cap_category,strategy
),
sma_cte as (
    select buy_year,market_cap_category,strategy, sum(profit_or_loss) as pl, count(*) as no_of_trades
    from {{ref('sma_backtest')}}
    group by market_cap_category, strategy,  buy_year
    order by buy_year ,market_cap_category,strategy
),
union_sma_ema as (
    select *
    from ema_cte
    union all
    select *
    from sma_cte
),
temp as (
    select *,
	    row_number() over (partition by market_cap_category,buy_year order by pl desc) as rank_best_strategy_for_market,
	    row_number() over (partition by strategy,buy_year order by pl desc) as rank_best_market_for_strategy,
	    row_number() over (partition by strategy order by pl desc) as rank_best_market_for_strategy_overall,
	    row_number() over (partition by market_cap_category order by pl desc) as rank_best_strategy_for_market_overall
    from union_sma_ema se
    order by buy_year, market_cap_category, pl desc
)
select * from temp
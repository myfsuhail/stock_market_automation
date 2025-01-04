{{
    config(materialized='table')
}}

select 
    *,
    case 
        when ema_9 > ema_21 and prev_ema_9 < prev_ema_21
        then 'BUY'
        when ema_9 < ema_21 and prev_ema_9 > prev_ema_21
        then 'SELL'
    end as ema_signal_9_21,
    case 
        when ema_21 > ema_50 and prev_ema_21 < prev_ema_50
        then 'BUY'
        when ema_21 < ema_50 and prev_ema_21 > prev_ema_50
        then 'SELL'
    end as ema_signal_21_50
from {{ref('ema_strategy')}}
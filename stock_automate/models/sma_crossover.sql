{{
    config(materialized='table')
}}

select 
    *,
    case 
        when sma_5 > sma_15 and prev_sma_5 < prev_sma_15
        then 'BUY'
        when sma_5 < sma_15 and prev_sma_5 > prev_sma_15
        then 'SELL'
    end as sma_signal_5_15,
    case 
        when sma_5 > sma_21 and prev_sma_5 < prev_sma_21
        then 'BUY'
        when sma_5 < sma_21 and prev_sma_5 > prev_sma_21
        then 'SELL'
    end as sma_signal_5_21,
    case 
        when sma_9 > sma_21 and prev_sma_9 < prev_sma_21
        then 'BUY'
        when sma_9 < sma_21 and prev_sma_9 > prev_sma_21
        then 'SELL'
    end as sma_signal_9_21,
    case 
        when sma_9 > sma_50 and prev_sma_9 < prev_sma_50
        then 'BUY'
        when sma_9 < sma_50 and prev_sma_9 > prev_sma_50
        then 'SELL'
    end as sma_signal_9_50,
    case 
        when sma_21 > sma_50 and prev_sma_21 < prev_sma_50
        then 'BUY'
        when sma_21 < sma_50 and prev_sma_21 > prev_sma_50
        then 'SELL'
    end as sma_signal_21_50,
    case 
        when sma_50 > sma_150 and prev_sma_50 < prev_sma_150
        then 'BUY'
        when sma_50 < sma_150 and prev_sma_50 > prev_sma_150
        then 'SELL'
    end as sma_signal_50_150
from {{ref('sma_strategy')}}
{{	
	config(
		materialized = 'table'
)
}}

with 
historical_data_cte as (
	select *, 'daily' as frequency
	from {{ source('main','historical_stock_data_daily') }}
	--where record_created_on > (select coalesce(max(record_created_on),'1900-01-01') from stock_data)
	union all
	select *, 'weekly' as frequency
	from {{ source('main','historical_stock_data_weekly') }}
	--where record_created_on > (select coalesce(max(record_created_on),'1900-01-01') from stock_data)
),
current_data_cte as (
	select *, 'daily' as frequency
	from {{ source('main','current_stock_data_daily') }}
	--where record_created_on > (select coalesce(max(record_created_on),'1900-01-01') from stock_data)
	union all
	select *, 'weekly' as frequency
	from {{ source('main','current_stock_data_weekly') }}
	--where record_created_on > (select coalesce(max(record_created_on),'1900-01-01') from stock_data)
),
unioned_stock_data as (
	select * from historical_data_cte
	union all
	select * from current_data_cte
),
joined_dataset as (
    select 
	    a.pair_id, 
		to_char(a.row_date_timestamp,'YYYYMMDD') as transaction_id,
		--strftime(a.row_date_timestamp, '%Y%m%d') AS transaction_id,
	    a.row_date_timestamp as transaction_date,
	    replace(a.last_open,',','')::decimal(10,4) as last_open,
	    replace(a.last_close,',','')::decimal(10,4) as last_close,
	    replace(a.last_min,',','')::decimal(10,4) as last_min,
	    replace(a.last_max,',','')::decimal(10,4) as last_max,
	    a.volume_raw::bigint as volume,
	    a.change_percent_raw as change_percent,
	    a.frequency,
	    b.uid,
	    b.primary_uid,
	    b.ticker,
	    b.stock_name,
	    b.sector,
		b.primary_sector,
		b.secondary_sector,
		b.market_cap,
		CASE
        	WHEN market_cap IS NULL THEN 'Unknown'
        	WHEN market_cap > 2000000000000 THEN 'Large Cap'
        	WHEN market_cap BETWEEN 500000000000 AND 2000000000000 THEN 'Mid Cap'
        	WHEN market_cap BETWEEN 50000000000 AND 500000000000 THEN 'Small Cap'
        	WHEN market_cap < 50000000000 THEN 'Micro Cap'
        	ELSE 'Other'
    	END AS market_cap_category,
		DATE_TRUNC('quarter', a.row_date_timestamp) AS quarter_start,
	    a.record_created_on,
		row_number() over (partition by a.pair_id, to_char(a.row_date_timestamp,'YYYYMMDD'), a.frequency order by a.record_created_on desc) as row_num 
    from unioned_stock_data a
    inner join {{ source('main', 'assets_info') }} b
    on a.pair_id = b.pair_id
)
select *
from joined_dataset
where row_num = 1
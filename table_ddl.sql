
create table halal_stocks (
halal_ind varchar(5),
stock_name varchar(255),
bse_code varchar(20),
nse_code varchar(20),
industry varchar(500),
record_created_on timestamp
);


create table assets_info (
uid varchar(100),
pair_id integer,
ticker varchar(20),
stock_name varchar(255),
primary_uid varchar(255),
path varchar(500),
sector varchar(500),
record_created_on timestamp
);


create table historical_stock_data_daily (
    pair_id numeric,
    direction_color text,
    row_date text,
    row_date_raw text,
    row_date_timestamp timestamp,
    last_close text,
    last_open text,
    last_max text,
    last_min text,
    volume text,
    volume_raw text,
    change_percent text,
    last_close_raw text,
    last_open_raw text,
    last_max_raw text,
    last_min_raw text,
    change_percent_raw text,
    record_created_on timestamp
);

create table historical_stock_data_weekly (
    pair_id numeric,
    direction_color text,
    row_date text,
    row_date_raw text,
    row_date_timestamp timestamp,
    last_close text,
    last_open text,
    last_max text,
    last_min text,
    volume text,
    volume_raw text,
    change_percent text,
    last_close_raw text,
    last_open_raw text,
    last_max_raw text,
    last_min_raw text,
    change_percent_raw text,
    record_created_on timestamp
);


create table current_stock_data_daily (
    pair_id numeric,
    direction_color text,
    row_date text,
    row_date_raw text,
    row_date_timestamp timestamp,
    last_close text,
    last_open text,
    last_max text,
    last_min text,
    volume text,
    volume_raw text,
    change_percent text,
    last_close_raw text,
    last_open_raw text,
    last_max_raw text,
    last_min_raw text,
    change_percent_raw text,
    record_created_on timestamp
);

create table current_stock_data_weekly (
    pair_id numeric,
    direction_color text,
    row_date text,
    row_date_raw text,
    row_date_timestamp timestamp,
    last_close text,
    last_open text,
    last_max text,
    last_min text,
    volume text,
    volume_raw text,
    change_percent text,
    last_close_raw text,
    last_open_raw text,
    last_max_raw text,
    last_min_raw text,
    change_percent_raw text,
    record_created_on timestamp
);
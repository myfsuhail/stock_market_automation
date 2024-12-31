create table buy_sell_buddy.public.halal_stocks (
halal_ind varchar(5),
stock_name varchar(255),
bse_code varchar(20),
nse_code varchar(20),
industry varchar(500),
record_created_on timestamp
);

create table buy_sell_buddy.public.assets_info (
uid varchar(100),
pair_id integer,
ticker varchar(20),
stock_name varchar(255),
primary_uid varchar(255),
path varchar(500),
sector varchar(500),
record_created_on timestamp
);
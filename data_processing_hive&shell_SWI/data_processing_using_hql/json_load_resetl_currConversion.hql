--############################################################################################################################
--# File Name      : json_load_resetl_currConversion.hql
--# Prerequisite   :
--# Description    : 
--# Change history
--# Name           date         Modification
--# ====           ====         ============
--# Vidya           02/13/2018     Initial Version
--############################################################################################################################

set mapred.job.queue.name=${hiveconf:queue_name};

-- The amount of memory to request from the scheduler for each map task.
Set mapreduce.map.memory.mb=4384;
Set mapreduce.map.java.opts=-Xmx4288m;

-- The amount of memory to request from the scheduler for each reduce task.
Set mapreduce.reduce.memory.mb=4384;
Set mapreduce.reduce.java.opts=-Xmx4288m;

create table dimCurrencyJoin
as
select curr_conv.currency_iso_cd,
       curr_conv.date_currency_key,
       curr_conv.date_key as date_create_key,
       curr_conv.exch_per_us_dlr_rte,
       curr.decimal_positions_marsha_qty,
       b.creation_date 
from mrdw_dim_currency_conversion curr_conv
	 join dimdate b
       on curr_conv.date_key = b.date_create_key
     join mrdw_dim_currency curr
       on curr.currency_iso_cd = curr_conv.currency_iso_cd;

create table currencyRate
as
select a.*,b.date_currency_key,b.date_create_key,b.exch_per_us_dlr_rte,
       b.decimal_positions_marsha_qty,
       a.base_amount/pow(10,nvl(case when a.decimal_places = 0 then 1 else a.decimal_places end ,b.decimal_positions_marsha_qty)) * b.exch_per_us_dlr_rte as baseAmountLocal,
       sum(baseAmountLocal) as baseAmountLocal,
       sum(base_amount) as base_amount,
       collect_list(date_arrival_key) as date_arrival_key
 from  dimArrivalDateKey a
left outer join dimCurrencyJoin b
                on  b.creation_date = a.create_date
               and a.currency_iso_cd = b.currency_code
       group by a.create_date,
	            a.confo_num_orig,
				a.confo_num_curr,
				b.date_create_key;
				
create table reservationDF
as
select curr.confo_num_orig,
curr.confo_num_curr,
curr.date_create_key,
curr.baseAmountLocal,
curr.base_amount,
curr.date_arrival_key,
dq.errorDescription
 from currencyRate curr
left outer join dataQualityErrors dq
on curr.confo_num_orig = dq.confo_num_orig
and curr.confo_num_curr = dq.confo_num_curr
and curr.create_date = dq.create_date;

create table reservationDFFinal
as
select accom.*,
res.date_create_key,
res.baseAmountLocal,
res.base_amount,
res.date_arrival_key,
res.errorDescription
from accommSegmentAllDF accom
join reservationDF res
on accom.confo_num_orig = res.confo_num_orig
and accom.confo_num_curr = res.confo_num_curr
and accom.create_date = res.create_date;

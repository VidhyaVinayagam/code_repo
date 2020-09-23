--############################################################################################################################
--# File Name      : json_load_resetl_reservconfirm.hql
--# Prerequisite   :
--# Description    : 
--# Change history
--# Name           date         Modification
--# ====           ====         ============
--# Vidya           02/13/2018     Initial Version
--############################################################################################################################

set mapred.job.queue.name=${hiveconf:queue_name};

Set mapreduce.map.memory.mb=4384;
Set mapreduce.map.java.opts=-Xmx4288m;

-- The amount of memory to request from the scheduler for each reduce task.
Set mapreduce.reduce.memory.mb=4384;
Set mapreduce.reduce.java.opts=-Xmx4288m;

-------------------------------------------------------------------------------------
--Creating table for reservation details from table 'resetl_json_table' exploding --the array reservationConfirmations,reservationSegments and roompools
-------------------------------------------------------------------------------------
create table reservConfirm
as
select cast(PublishReservation2.reservation.creationDateTime as date) as create_date,
lastupdatetime as transaction_timestamp,
PublishReservation2.reservation.creatorID as booking_office,
exploded_table.reservconfo.reservationCode as confo_num_orig,
exploded_table.reservconfo.reservationCode as confo_num_curr,
exploded_table.reservconfo.reservationInstance as reservationInstance,
property.propertyCode as property_code,
property_id,
exploded_table.guestname.givenName as guest_name,
exploded_table.rooms.roomPoolCode as room_pool,
exploded_table.rooms.numberOfUnits as no_of_rooms,
exploded_table.reservseg.segmentStatus as reservation_Status,
exploded_table.reservseg.startDate as arrival_date,
exploded_table.reservseg.endDate as end_date,
exploded_table.reservseg.rate.ratePlanCode as rate_program,
exploded_table.reservseg.rate.baseRateUnit as base_rate_unit,
exploded_table.reservseg.rate.baseAmount as revenue,
exploded_table.reservseg.rate.currencyCode as currency_code,
exploded_table.reservseg.rate.decimalPlaces as decimal_places
from resetl_json_table_1 a
lateral view posexplode (PublishReservation2.reservation.reservationConfirmations)
exploded_table as seqa,reservconfo
lateral view posexplode (PublishReservation2.reservationSegments)
exploded_table as seqb,reservseg
lateral view posexplode(PublishReservation2.roompools)
exploded_table as seqc,rooms
lateral view posexplode(PublishReservation2.guest)
exploded_table as seqd,guests
lateral view posexplode(exploded_table.guests.profiles.customer)
exploded_table as seqd,customer
lateral view posexplode(exploded_table.customer.name)
exploded_table as seqe,guestname
where seqa = seqb = seqc = seqd = seqe
;

-------------------------------------------------------------------------------------
--Verifying the reservation data for existing property and not null create_date
-------------------------------------------------------------------------------------
create table accommSegmentAllDF
as
select reserv.create_date,
       reserv.confo_num_orig,
       reserv.confo_num_curr,
       reserv.reservationInstance,
       reserv.propertyCode,
       reserv.property_id,
       reserv.roomPoolCode,
       reserv.segmentStatus,
       reserv.start_date,
       reserv.end_date,
       reserv.rate_plan_code,
       reserv.base_rate_unit,
       reserv.base_amount,
       reserv.currency_code,
       reserv.decimal_places
  from reservConfirm reserv
       left outer join aw_mrdw_tgt_dbo.mrdw_dim_property prop
					on reserv.propertyCode = prop.property_cd
       left outer join aw_mrdw_tgt_dbo.mrdw_dim_date tdate
					on tdate.date_dt = reserv.start_date;
--where reserv.create_date != NULL ;


create table dimdate
as
select b.date_key as date_create_key,
	   b.date_dt as creation_date,
	   b.year_acctg
  from resetl_json_table a
  join mrdw_dim_date b
       on coalesce(a.create_date,cast('2010-06-22' as date)) = b.date_dt;

create table dimArrivalDateKey
as
select accom.*,b.date_key as date_arrival_key
from accommSegmentAllDF accom
join mrdw_dim_date b
on accom.start_date = b.date_dt;

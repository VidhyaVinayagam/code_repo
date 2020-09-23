--##########################################################################################################################################
--# File Name      : validate_mrdw_parsed_reservation.hql
--# Prerequisite   :
--# Description    : This script validates the fields from table mrdw_parsed_reservation 
--# Change history
--# Name           date         Modification
--# ====           ====         ============
--# Vidya           02/26/2018     Initial Version
--##########################################################################################################################################


set mapred.job.queue.name=${hiveconf:queue_name};

Set mapreduce.map.memory.mb=4384;
Set mapreduce.map.java.opts=-Xmx4288m;

-- The amount of memory to request from the scheduler for each reduce task.
Set mapreduce.reduce.memory.mb=4384;
Set mapreduce.reduce.java.opts=-Xmx4288m;

-------------------------------------------------------------------------------------
--New table with parsed reservation details from table 'it_res_sec_dbo.mrdw_reservation_repository_orc' exploding --the array reservationConfirmations,reservationSegments and roompools
-------------------------------------------------------------------------------------

CREATE TABLE it_res_sec_dbo.mrdw_reservation_repository_parsed
as
SELECT CAST(PublishReservation2.reservation.creationDateTime as date) as create_date,
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
  FROM it_res_sec_dbo.mrdw_reservation_repository_orc a
LATERAL VIEW POSEXPLODE (PublishReservation2.reservation.reservationConfirmations) exploded_table as seqa,reservconfo
LATERAL VIEW POSEXPLODE (PublishReservation2.reservationSegments) exploded_table as seqb,reservseg
LATERAL VIEW POSEXPLODE (PublishReservation2.roompools) exploded_table as seqc,rooms
LATERAL VIEW POSEXPLODE (PublishReservation2.guest) exploded_table as seqd,guests
LATERAL VIEW POSEXPLODE (exploded_table.guests.profiles.customer) exploded_table as seqe,customer
LATERAL VIEW POSEXPLODE (exploded_table.customer.name) exploded_table as seqf,guestname
  WHERE seqa = seqb = seqc = seqd = seqe = seqf;

!echo "*****************************************************************************************************************************";
!echo "DQ CHECK 1 -  Validates if the confo_num_orig does start with 7,8 or 9.";
!echo "*****************************************************************************************************************************";

INSERT OVERWRITE TABLE resetl_valiadation_table
SELECT
	  'reservationCode' as errorField,
      b.confo_num_orig as errorFieldValue,
	  'confo_num_orig doesnt start with 7,8 or 9' as errorField_description
FROM
(SELECT a.reservationCode,
		case when substring(a.confo_num_orig,0,1) < 7 and substring(a.confo_num_orig,0,1) > 9    
	       then 'FAIL'
           else 'PASS' 
      end as result
FROM
it_res_sec_dbo.mrdw_reservation_repository_parsed a) b where b.result='FAIL';

!echo "*****************************************************************************************************************************";
!echo "DQ CHECK 4 -  Validates if the currency code is NULL.";
!echo "*****************************************************************************************************************************";

INSERT INTO resetl_valiadation_table
SELECT
	  'currencyCode' as errorField,
      b.currency_code as errorFieldValue,
	  'currency_code is NULL' as errorField_description
FROM
(SELECT a.currency_code,
		case when currency_code is NULL or currency_code = ''    
	       then 'FAIL'
           else 'PASS' 
      end as result
FROM
it_res_sec_dbo.mrdw_reservation_repository_parsed a) b where b.result='FAIL';

!echo "*****************************************************************************************************************************";
!echo "DQ CHECK 5 -  Validates if the reservation_Status is NULL.";
!echo "*****************************************************************************************************************************";

INSERT INTO resetl_valiadation_table
SELECT
	  'reservationStatus' as errorField,
      b.reservation_status as errorFieldValue,
	  'reservation_status is NULL' as errorField_description
FROM
(SELECT a.reservation_status,
		case when reservation_status is NULL or reservation_status=' '    
	       then 'FAIL'
           else 'PASS' 
      end as result
FROM
it_res_sec_dbo.mrdw_reservation_repository_parsed a) b where b.result='FAIL';

!echo "*****************************************************************************************************************************";
!echo "DQ CHECK 6 -  Validates if the guest name is NULL.";
!echo "*****************************************************************************************************************************";

INSERT INTO resetl_valiadation_table
SELECT
	  'guest_givenName' as errorField,
      b.given_name as errorFieldValue,
	  'Guest name is NULL' as errorField_description
FROM
(SELECT a.guest_name,
		case when guest_name is NULL    
	       then 'FAIL'
           else 'PASS' 
      end as result
FROM
it_res_sec_dbo.mrdw_reservation_repository_parsed a) b where b.result='FAIL';





              

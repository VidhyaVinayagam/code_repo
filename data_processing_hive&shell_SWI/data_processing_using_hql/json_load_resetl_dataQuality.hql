--############################################################################################################################
--# File Name      : json_load_resetl_dataQuality.hql
--# Prerequisite   :
--# Description    : Data quality checks to ensure all required data elements of a --#             reservation are included in every JSON message received
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
-------------------------------------------------------------------------------------
--Validation table 'dataQualityErrors' with error field and error description for each
--reservation
-------------------------------------------------------------------------------------

create table dataQualityErrors
(
create_date date,  
confo_num_orig string,
confo_num_curr string,
errorDescription array<struct< errorField : string , errorDesc : string>>
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','  
STORED AS PARQUET;

--create table errorDesc
--(
--create_date date,  
--confo_num_orig string,
--confo_num_curr string,
--errordesc struct< errorField : string , errorDesc : string>
--)
--ROW FORMAT DELIMITED 
--FIELDS TERMINATED BY ','  
--STORED AS PARQUET;

create table errorDesc
as
select a.create_date,  
       a.confo_num_orig,
       a.confo_num_curr,
       named_struct('errorField',a.Field,'errorDesc',a.Description) as errordesc
  from ( select create_date,  
                confo_num_orig,
                confo_num_curr,
                exploded_table.roompool.roompoolcode,
                exploded_table.reserv.rate.currencyCode,
                case 
					when exploded_table.roompool.roompoolcode is NULL
						 then 'roompoolcode'
					when property_id is NULL then 'property_id'
					when exploded_table.reserv.rate.currencyCode is NULL then 'currency_code'
                  else ' ' 
				end as Field,
                case 
					when exploded_table.roompool.roompoolcode is NULL
						 then 'Column is NULL'
					when property_id is NULL then 'property does not Exist'
					when exploded_table.reserv.rate.currencyCode is NULL then 'currency_code'
				  else ' ' 
				end as Description
          from accommSegmentAllDF
----lateral view posexplode (accommSegmentAllDF.PublishReservation2.roompools) exploded_table as roomspos,roompool
--lateral view posexplode (accommSegmentAllDF.PublishReservation2.reservationSegments) exploded_table as ratespos,reserv
--where roomspos=ratespos) a
group by a.create_date,
		 a.confo_num_orig,
		 a.confo_num_curr;

Insert into dataQualityErrors 
select err_desc.create_date,  
       err_desc.confo_num_orig,
       err_desc.confo_num_curr,
       min(array(errordesc)) as errorDescription
  from errorDesc err_desc
group by err_desc.create_date,
		 err_desc.confo_num_orig,
		 err_desc.confo_num_curr;

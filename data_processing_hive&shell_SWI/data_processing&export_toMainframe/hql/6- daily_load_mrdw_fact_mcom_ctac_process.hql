--/*
--############################################################################################################################
--# File Name      : daily_load_mrdw_fact_mcom_ctac_process.hql
--# Prerequisite   : historical_load_mrdw_fact_mcom_ctac_process.hql
--# Description    : This script will populate MRDW_FACT_MCOM_ATRB_CTAC_DAILY_EX with bookings from
--#					 MRDW_FACT_MCOM_ATRB_MATCH_EX_REPL that match the CTAC IATA IDs
--#                  This script will insert new records into the audit table for the batch dt, insert ts, and record COUNT
--#				     This script will update the ctac_ind column in MRDW_FACT_MCOM_ATRB_MATCH_EX for records loaded into
--#					 MRDW_FACT_MCOM_ATRB_CTAC_DAILY_EX.
--# Change history
--# Name           date         Modification
--# ====           ====         ============
--# TCS         07/28/2017      Initial Version
--#
--############################################################################################################################
--*/

set mapred.job.queue.name=${hiveconf:queue_name};

-- The amount of memory to request from the scheduler for each map task.
Set mapreduce.map.memory.mb=4384;
Set mapreduce.map.java.opts=-Xmx4288m;

-- The amount of memory to request from the scheduler for each reduce task.
Set mapreduce.reduce.memory.mb=4384;
Set mapreduce.reduce.java.opts=-Xmx4288m;

set hive.auto.convert.join=true;

ADD JAR ${hiveconf:jenkins_path}/JenkinsUDF.jar;
CREATE  TEMPORARY FUNCTION Jenkins as 'com.marriott.ddm.hash.Jenkins.JenkinsUDF';

!echo "******************************************************************************************************************************************";
!echo "***  STEP1: insert data filtered based on ctac's iata ids into temp table of MRDW_FACT_MCOM_ATTRIB_MATCH_EX_REPL";
!date;
!echo "*****************************************************************************************************************************************";

INSERT OVERWRITE TABLE ${hiveconf:ETL_SCHEMA}.tmp_match_ctac
SELECT MATCH.atrb_key,
       MATCH.atrb_stay_key,
       MATCH.confo_num_orig_id,
       MATCH.confo_num_curr_id,
       MATCH.date_create_key,
       MATCH.property_cd,
       MATCH.camp_source_abrv_txt,
       MATCH.atrb_iata_id,
       MATCH.book_iata_id,
       MATCH.arrival_dt,
       MATCH.market_cd,
       MATCH.atrb_amt,
       MATCH.dw_load_ts
       FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl MATCH
            JOIN ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_iata_ex IATA1
              ON MATCH.book_iata_id = IATA1.atrb_iata_id
                 AND IATA1.active_ind = 'A'
            JOIN ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_iata_ex IATA2
              ON MATCH.atrb_iata_id = IATA2.atrb_iata_id
                 AND MATCH.camp_source_abrv_txt = IATA2.camp_source_abrv_txt  --to ensure each iata sent to CTAC has only 1 camp_source_abrv_txt value
                 AND IATA2.active_ind = 'A';

!echo "******************************************************************************************************************************************";
!echo "***  STEP2: INSERT INTO MRDW_FACT_MCOM_ATRB_CTAC_DAILY_EX_REPL where the booking doesn't already exist in the fact table";
!date;
!echo "******************************************************************************************************************************************";

INSERT OVERWRITE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex_repl
SELECT atrb_key,
       atrb_stay_key,
       confo_num_orig_id,
       confo_num_curr_id,
       date_create_key,
       property_cd,
       camp_source_abrv_txt, --CTAC only wants the 3 character abreviated text (does not include branded/unbranded)
       atrb_iata_id,
       book_iata_id,
       arrival_dt,
       market_cd,
       atrb_amt,
       dw_load_ts
  FROM ${hiveconf:ETL_SCHEMA}.tmp_match_ctac TMP
 WHERE substr(camp_source_abrv_txt,1,3) in ('AFF', 'PAI', 'REF');
--Data should already be filtered by the join to the IATA dim table

!echo "******************************************************************************************************************************************";
!echo "*DQ CHECKS*"
!echo "******************************************************************************************************************************************";

--- DQ_CHECK 3.1 - Verify booking_stay_key does not exist in previously loaded data

--#INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
--#SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END as error_code,
--#       'BOOKING_STAY_KEY_EXISTS'     error_abv,
--#       'mrdw_fact_mcom_atrb_ctac_daily_ex_repl' error_table,
--#       COUNT(*) row_cnt
--# FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex_repl repl
--#WHERE EXISTS (SELECT 'x'
--#                FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex fact
--#               WHERE repl.booking_stay_key = fact.booking_stay_key)
--#;

--- DQ_CHECK 3.2 - Verify confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt, market_cd,
--camp_source_abrv_txt, atrb_iata_id combination is unique

INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END AS error_code,
       'DUPLICATE_BUSINESS_KEY' error_abv,
       'mrdw_fact_mcom_atrb_ctac_daily_ex_repl' error_table,
       COUNT(*) row_cnt
  FROM (SELECT confo_num_orig_id,
               confo_num_curr_id,
               date_create_key,
               arrival_dt,
               market_cd,
               camp_source_abrv_txt,
               atrb_iata_id
          FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex_repl
      GROUP BY confo_num_orig_id,
               confo_num_curr_id,
               date_create_key,
               arrival_dt,
               market_cd,
               camp_source_abrv_txt,
               atrb_iata_id
        HAVING COUNT(*) > 1 ) foo;

--- DQ_CHECK 3.3 - Verify confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt, market_cd,
--camp_source_abrv_txt, atrb_iata_id combination does not exist in previously loaded data

--#INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
--#SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END AS error_code,
--#       'BUSINESS_KEY_EXISTS'     error_abv,
--#       'mrdw_fact_mcom_atrb_ctac_daily_ex_repl' error_table,
--#       COUNT(*) row_cnt
--#  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex_repl repl
--# WHERE EXISTS ( SELECT 'x'
--#                  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex fact
--#                 WHERE repl.confo_num_orig_id = fact.confo_num_orig_id
--#	               AND repl.confo_num_curr_id = fact.confo_num_curr_id
--#	               AND repl.date_create_key = fact.date_create_key
--#	               AND repl.arrival_dt = fact.arrival_dt
--#	               AND repl.market_cd = fact.market_cd
--#	               AND repl.camp_source_abrv_txt = fact.camp_source_abrv_txt
--#	               AND repl.atrb_iata_id = fact.atrb_iata_id)
--#;

--- DQ_CHECK 3.4 - Verify primary key is unique
INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END AS error_code,
       'DUPLICATE_PRIMARY_KEY'  error_abv,
       'mrdw_fact_mcom_atrb_ctac_daily_ex_repl' error_table,
       COUNT(*) row_cnt
  FROM ( SELECT atrb_key
           FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex_repl
       GROUP BY atrb_key
         HAVING COUNT(*) > 1 ) foo
;

--- DQ_CHECK 3.5 - Verify primary key does not exist in previously loaded data
--#INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
--#SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END AS error_code,
--#       'PRIMARY_KEY_EXISTS' error_abv,
--#       'mrdw_fact_mcom_atrb_ctac_daily_ex_repl' error_table,
--#       COUNT(*) row_cnt
--#  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex_repl repl
--# WHERE EXISTS ( SELECT 'x'
--#                  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex fact
--#                 WHERE fact.atrb_key = repl.atrb_key)
--#;

--- DQ_CHECK 3.6 - For each confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt combination, the atrb_amt should sum to 100 or less
INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END AS error_code,
       'INVALID_ATRB_ALLOCATION_AMT' error_abv,
       'mrdw_fact_mcom_atrb_ctac_daily_ex_repl' error_table,
       COUNT(*) row_cnt
  FROM ( SELECT confo_num_orig_id,
                confo_num_curr_id,
                date_create_key,
                arrival_dt,
                SUM(atrb_amt) sum_atrb_amt
           FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex_repl
       GROUP BY confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt
         HAVING ROUND(sum_atrb_amt) > 100 
) foo;

!echo "******************************************************************************************************************************************";
!echo "*STEP 3: Insert values into MRDW_FACT_MCOM_ATRB_CTAC_DAILY_EX from MRDW_FACT_MCOM_ATRB_CTAC_DAILY_EX_REPL*"
!echo "******************************************************************************************************************************************";

INSERT INTO ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex
SELECT atrb_key,            
       booking_stay_key,    
       confo_num_orig_id,   
       confo_num_curr_id,   
       date_create_key,     
       property_cd,        
       camp_source_abrv_txt,
       atrb_iata_id,        
       book_iata_id,        
       arrival_dt,          
       market_cd,           
       atrb_amt,           
       dw_load_ts          
  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex_repl a,
       (SELECT COUNT(*) AS cnt FROM ${hiveconf:ETL_SCHEMA}.temp_fail_ctac WHERE error_code = 1) b
 WHERE b.cnt < 1;

!echo "******************************************************************************************************************************************";
!echo "*STEP 4: Insert values into MRDW_ETL_TABLE_AUDIT for MRDW_FACT_MCOM_ATRB_CTAC_DAILY_EX*"
!echo "******************************************************************************************************************************************";

INSERT INTO ${hiveconf:TGT_SCHEMA}.mrdw_etl_table_audit
SELECT JENKINS(NVL(a.dw_load_ts,CURRENT_TIMESTAMP())) AS audit_key,
       NVL(a.dw_load_ts,CURRENT_TIMESTAMP()) AS initial_load_ts,
       CURRENT_TIMESTAMP() AS data_batch_dt,
       'MRDW_FACT_MCOM_ATRB_CTAC_DAILY_EX' AS table_nm,
       NULL AS min_data_ts,
       NULL AS max_data_ts,
       a.record_cnt AS record_amt,
       'ATRBCTAC' AS sys_id,
       CURRENT_TIMESTAMP() AS dw_load_ts
  FROM (SELECT MAX(dw_load_ts) AS dw_load_ts, 
               MAX(arrival_dt) AS arrival_dt, 
               COUNT(*) AS record_cnt 
          FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex_repl ) a,
       (SELECT COUNT(*) AS cnt 
	      FROM ${hiveconf:ETL_SCHEMA}.temp_fail_ctac 
		 WHERE error_code = 1) b 
 WHERE b.cnt < 1;

!echo "******************************************************************************************************************************************";
!echo "*STEP 5: Update the CTAC_IND in MRDW_FACT_MCOM_ATRB_MATCH_EX*"
!echo "******************************************************************************************************************************************";

INSERT OVERWRITE TABLE ${hiveconf:ETL_SCHEMA}.tmp_ctac_update
SELECT MATCH.atrb_key,
       MATCH.atrb_allocation_key,
       MATCH.atrb_stay_key,
       MATCH.confo_num_orig_id,
       MATCH.confo_num_curr_id,
       MATCH.date_create_key,
       MATCH.property_cd,
       MATCH.camp_source_abrv_txt,
       MATCH.atrb_iata_id,
       MATCH.book_iata_id,
       MATCH.arrival_dt,
       MATCH.market_cd,
       MATCH.atrb_amt,
       '1' as ctac_ind,
       MATCH.dw_load_ts --dw_load_ts is not updated when the ctac_ind is updated
  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex MATCH
  JOIN ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex_repl CTAC
    ON MATCH.atrb_key = CTAC.atrb_key
 WHERE CAST(MATCH.ctac_ind AS int)= '0';
 
 --STEP 5.1 Update the mrdw_fact_mcom_atrb_match_ex with CTAC_IND as '1'  bookings exist in the fact table

INSERT OVERWRITE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex
SELECT * FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex a
        WHERE NOT EXISTS (SELECT 1 FROM ${hiveconf:ETL_SCHEMA}.tmp_ctac_update b
	                       WHERE a.atrb_key = b.atrb_key);

INSERT INTO ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex
SELECT atrb_key,
       atrb_allocation_key,
       atrb_stay_key,
       confo_num_orig_id,
       confo_num_curr_id,
       date_create_key,
       property_cd,
       camp_source_abrv_txt,
       atrb_iata_id,
       book_iata_id,
       arrival_dt,
       market_cd,
       atrb_amt,
       ctac_ind,
       dw_load_ts
  FROM ${hiveconf:ETL_SCHEMA}.tmp_ctac_update a,
       (SELECT COUNT(*) AS cnt FROM ${hiveconf:ETL_SCHEMA}.temp_fail_ctac WHERE error_code = 1) b
 WHERE b.cnt < 1;
 
 !echo "******************************************************************************************************************************************";
!echo "***  STEP 6: Generate Statistics for Result Tables"
!date;
!echo "******************************************************************************************************************************************";

Analyze table ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex COMPUTE STATISTICS;
Analyze table ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_ctac_daily_ex COMPUTE STATISTICS;

--#/*
--#This select must stay in place so that the shell script can capture the table output.
--#*/
SELECT 'CAPTURE_TABLE_BEGIN';
SELECT * from ${hiveconf:ETL_SCHEMA}.temp_fail_ctac  where error_code > 0;


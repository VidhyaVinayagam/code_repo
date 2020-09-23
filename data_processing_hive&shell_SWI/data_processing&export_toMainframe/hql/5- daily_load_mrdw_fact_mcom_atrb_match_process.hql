--############################################################################################################################
--# File Name      : daily_load_mrdw_fact_mcom_atrb_match_process.hql
--# Prerequisite   :
--# Description    : This script will populate MRDW_FACT_MCOM_ATRB_MATCH_EX with bookings by connecting bookings made online
--#                  with MRDW data for all data currently loaded (historical data)
--#                  This script will insert new records into the audit table for the batch dt, insert ts, and record COUNT
--# Change history
--# Name           date         Modification
--# ====           ====         ============
--# Vidya           07/20/2017     Initial Version
--############################################################################################################################

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
!echo "***  STEP 1 : Inserting distinct child records into temp table";
!date;
!echo "******************************************************************************************************************************************";
--Insert distinct accoms in temp table to handle a bug in MMRS that is causing cancelled records to be sent to MRDW as active
--Causes 2 active market_cds for the same accom when it was cancelled and resubmitted with a new market_cd

INSERT OVERWRITE TABLE ${hiveconf:ETL_SCHEMA}.tmp_accoms
SELECT DISTINCT a.market_cd,
                a.confo_num_orig_id,
                a.confo_num_curr_id,
                a.date_create_key,
                a.property_cd,
                a.book_iata_id,
                a.arrival_dt,
                a.atrb_stay_key
  FROM (SELECT FIRST_VALUE(STAY.market_cd) over (PARTITION BY STAY.confo_num_orig_id, STAY.confo_num_curr_id, STAY.date_create_key, STAY.property_cd, STAY.arrival_dt ORDER BY STAY.market_cd ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS market_cd,
               STAY.confo_num_orig_id AS confo_num_orig_id,
               STAY.confo_num_curr_id AS confo_num_curr_id,
               STAY.date_create_key AS date_create_key,
               STAY.property_cd AS property_cd,
               NVL(STAY.book_iata_id,'') AS book_iata_id,
               STAY.arrival_dt AS arrival_dt,
               FIRST_VALUE(STAY.atrb_stay_key) over (PARTITION BY STAY.confo_num_orig_id, STAY.confo_num_curr_id, STAY.date_create_key, STAY.property_cd, STAY.arrival_dt ORDER BY STAY.market_cd ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS atrb_stay_key
         FROM ${hiveconf:ALIAS_SCHEMA}.mrdw_fact_mcom_atrb_stay_ex_repl STAY DISTRIBUTE BY (confo_num_orig_id, date_create_key)) a;

!echo "******************************************************************************************************************************************";
!echo "***  STEP 2 : Insert into MRDW_FACT_MCOM_ATRB_MATCH_EX_REPL where the booking doesn't already exist in the fact table";
!date;
!echo "******************************************************************************************************************************************";
-- STEP 2.1 - Inserting separate columns of Attribute allocation and stay keys into temp table for Reference.

INSERT OVERWRITE TABLE ${hiveconf:ETL_SCHEMA}.aw_temp
SELECT  MAX(ALLOC.atrb_allocation_key) AS atrb_allocation_key, --selecting the max key when there are multiple records for a 3 character source (i.e. NATU/NATB, PAIU/PAIB)
        STAY.atrb_stay_key,
        STAY.confo_num_orig_id,
        STAY.confo_num_curr_id,
        STAY.date_create_key,
        STAY.property_cd,
--summing atrb_amt based on the first 3 characters of camp_source_drvd_txt to acCOUNT for PAIB vs PAIU which CTAC doesn't care about
        SUBSTR(ALLOC.camp_source_drvd_txt,1,3) AS camp_source_abrv_txt,
        ALLOC.atrb_iata_id,
        STAY.book_iata_id,
        STAY.arrival_dt,
--selecting 1 value for market_cd for the accom because of a bug in MMRS that is causing cancelled records to be sent to MRDW as active
--this causes 2 active market_cds for the same accom when it was cancelled and resubmitted with a new market_cd
        STAY.market_cd,
        SUM(ALLOC.atrb_amt) AS atrb_amt
  FROM ${hiveconf:ETL_SCHEMA}.tmp_accoms STAY JOIN ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_allocation_ex ALLOC
    ON STAY.confo_num_orig_id = ALLOC.confo_num_orig_id
   AND STAY.date_create_key = ALLOC.date_create_key
GROUP BY STAY.atrb_stay_key,
         STAY.confo_num_orig_id,	
         STAY.confo_num_curr_id,
         STAY.date_create_key,
         STAY.property_cd,
         SUBSTR(ALLOC.camp_source_drvd_txt,1,3),
         ALLOC.atrb_iata_id,
         STAY.book_iata_id,
         STAY.arrival_dt,
         STAY.market_cd;

--STEP 2.2 Inserting records from TEMP table (aw_trmp) to mrdw_fact_mcom_atrb_match_ex_repl

INSERT OVERWRITE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl
SELECT Jenkins(CONCAT(a.atrb_allocation_key,a.atrb_stay_key)) AS atrb_key,
       a.atrb_allocation_key,
       a.atrb_stay_key,
       a.confo_num_orig_id,
       a.confo_num_curr_id,
       a.date_create_key,
       a.property_cd,
       a.camp_source_abrv_txt,
       a.atrb_iata_id,
       a.book_iata_id,
       a.arrival_dt,
       a.market_cd,
       a.atrb_amt,
       '0' AS ctac_ind,
       CURRENT_TIMESTAMP() AS dw_load_ts
  FROM ${hiveconf:ETL_SCHEMA}.aw_temp a;
  
!echo "******************************************************************************************************************************************";
!echo "*DQ CHECKS*"
!echo "******************************************************************************************************************************************";

--- DQ_CHECK 3.1 - Verify atrb_allocation_key and atrb_stay_key combination is unique

INSERT OVERWRITE TABLE ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END AS error_code,
       'DUPLICATE_BUSINESS_KEY' error_abv,
       'mrdw_fact_mcom_atrb_match_ex_repl' error_table,
       COUNT(*) row_cnt
  FROM ( SELECT atrb_allocation_key, atrb_stay_key
           FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl
       GROUP BY atrb_allocation_key, atrb_stay_key
         HAVING COUNT(*) > 1 ) foo;

--#--- DQ_CHECK 3.2 - Verify atrb_allocation_key and atrb_stay_key combination does not exist in previously loaded data
--#INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
--#SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END AS error_code,
--#       'BUSINESS_KEY_EXISTS' error_abv,
--#       'mrdw_fact_mcom_atrb_match_ex_repl' error_table,
--#       COUNT(*) row_cnt
--#  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl repl
--# WHERE EXISTS ( SELECT 'x'
--#                  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex fact
--#                 WHERE fact.atrb_allocation_key = repl.atrb_allocation_key
--#                   AND fact.atrb_stay_key = repl.atrb_stay_key );

--- DQ_CHECK 3.3 - Verify confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt, market_cd,
--- camp_source_abrv_txt, atrb_iata_id combination is unique
INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END as error_code,
       'DUPLICATE_CCC_KEY' error_abv,
       'mrdw_fact_mcom_atrb_match_ex_repl' error_table,
       COUNT(*) row_cnt
  FROM ( SELECT confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt, market_cd, camp_source_abrv_txt, atrb_iata_id
           FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl
       GROUP BY confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt, market_cd, camp_source_abrv_txt, atrb_iata_id
         HAVING COUNT(*) > 1 ) foo;

--#--- DQ_CHECK 3.4 - Verify confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt, market_cd,
--#--camp_source_abrv_txt, atrb_iata_id combination does not exist in previously loaded data
--#INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
--#SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END as error_code,
--#       'CCC_KEY_EXISTS' error_abv,
--#       'mrdw_fact_mcom_atrb_match_ex_repl' error_table,
--#       COUNT(*) row_cnt
--#  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl repl
--# WHERE EXISTS ( SELECT 'x'
--#                  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex fact
--#                 WHERE fact.confo_num_orig_id = repl.confo_num_orig_id
--#                   AND fact.confo_num_curr_id = repl.confo_num_curr_id
--#                   AND fact.date_create_key = repl.date_create_key
--#                   AND fact.arrival_dt = repl.arrival_dt
--#                   AND fact.market_cd = repl.market_cd
--#                   AND fact.camp_source_abrv_txt = repl.camp_source_abrv_txt
--#                   AND nvl(fact.atrb_iata_id,'') = nvl(repl.atrb_iata_id,'')
--#               )
--#;

--- DQ_CHECK 3.5 - Verify primary key is unique
INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END AS error_code,
       'DUPLICATE_PRIMARY_KEY'     error_abv,
       'mrdw_fact_mcom_atrb_match_ex_repl' error_table,
       COUNT(*) row_cnt
  FROM ( SELECT atrb_key
           FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl
       GROUP BY atrb_key
         HAVING COUNT(*) > 1 ) foo
;

--#--- DQ_CHECK 3.6 - Verify primary key does not exist in previously loaded data
--#INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
--#SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END AS error_code,
--#       'PRIMARY_KEY_EXISTS'     error_abv,
--#       'mrdw_fact_mcom_atrb_match_ex_repl' error_table,
--#       COUNT(*) row_cnt
--#  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl repl
--# WHERE EXISTS ( SELECT 'x'
--#                  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex fact
--#                 WHERE fact.atrb_key = repl.atrb_key)
--#;

--- DQ_CHECK 3.7 - For each confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt combination, the atrb_amt should sum to 100
INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END AS error_code,
       'INVALID_ATRB_ALLOCATION_AMT'     error_abv,
       'mrdw_fact_mcom_atrb_match_ex_repl' error_table,
       COUNT(*) row_cnt
FROM ( SELECT confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt, ROUND(SUM(atrb_amt),0) sum_atrb_amt
         FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl
     GROUP BY confo_num_orig_id, confo_num_curr_id, date_create_key, arrival_dt
       HAVING (ROUND(SUM(atrb_amt),0) <> '100' )) foo
;

!echo "******************************************************************************************************************************************";
!echo "***  STEP 4 : Insert values into MRDW_FACT_MCOM_ATRB_MATCH_EX from MRDW_FACT_MCOM_ATRB_MATCH_EX_REPL";
!date;
!echo "******************************************************************************************************************************************";

INSERT INTO ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex
SELECT  atrb_key,
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
        CAST(ctac_ind AS BOOLEAN) AS ctac_ind,
        dw_load_ts
  FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl a,
       (SELECT COUNT(*) AS cnt FROM ${hiveconf:ETL_SCHEMA}.temp_fail_ctac WHERE error_code = 1) b
 WHERE b.cnt < 1;
;

!echo "******************************************************************************************************************************************";
!echo "***  STEP 5 : Update audit table for new MRDW_FACT_MCOM_ATRB_MATCH_EX inserts";
!date;
!echo "******************************************************************************************************************************************";

INSERT INTO ${hiveconf:TGT_SCHEMA}.mrdw_etl_table_audit
SELECT JENKINS(NVL(a.dw_load_ts,CURRENT_TIMESTAMP())) AS audit_key,
       NVL(a.dw_load_ts,CURRENT_TIMESTAMP()) AS dw_load_ts,
       CURRENT_TIMESTAMP() AS arrival_dt,
       'MRDW_FACT_MCOM_ATRB_MATCH_EX',
       NULL,
       NULL,
       a.record_cnt,
       'ATRBCTAC',
       CURRENT_TIMESTAMP()
  FROM ( SELECT MAX(dw_load_ts) AS dw_load_ts, max(arrival_dt) AS arrival_dt, COUNT(*) AS record_cnt FROM ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl ) a,
       (SELECT COUNT(*) AS cnt FROM ${hiveconf:ETL_SCHEMA}.temp_fail_ctac WHERE error_code = 1)b 
 WHERE b.cnt < 1
 ;
 
!echo "******************************************************************************************************************************************";
!echo "***  STEP 6: Generate Statistics for Result Tables"
!date;
!echo "******************************************************************************************************************************************";

ANALYZE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_fact_mcom_atrb_match_ex_repl COMPUTE STATISTICS;

--#/*
--#This select must stay in place so that the shell script can capture the table output.
--#*/
SELECT 'CAPTURE_TABLE_BEGIN';
SELECT * from ${hiveconf:ETL_SCHEMA}.temp_fail_ctac  where error_code > 0;


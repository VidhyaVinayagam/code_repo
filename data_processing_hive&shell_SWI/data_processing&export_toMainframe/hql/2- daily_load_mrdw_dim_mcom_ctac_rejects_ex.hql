--/*
--############################################################################################################################
--# File Name      : 	daily_load_mrdw_dim_mcom_ctac_rejects_ex.hql
--# Prerequisite   : 
--# Description    : 	This script inserts CTAC REJECT data from mrdw_stg_mcom_ctac_rejects_ex to mrdw_dim_mcom_ctac_rejects_ex  
--#
--# Change history
--# Name           DATE         Modification
--# ====           ====         ============
--# Vidya	      20/08/2017   Initial Version
--############################################################################################################################
--*/

set mapred.job.queue.name=${hiveconf:queue_name};

-- The amount of memory to request from the scheduler for each map task.
Set mapreduce.map.memory.mb=4384;
Set mapreduce.map.java.opts=-Xmx4288m;

-- The amount of memory to request from the scheduler for each reduce task.
Set mapreduce.reduce.memory.mb=4384;
Set mapreduce.reduce.java.opts=-Xmx4288m;


ADD JAR ${hiveconf:jenkins_path}/JenkinsUDF.jar;
CREATE  TEMPORARY FUNCTION Jenkins as 'com.marriott.ddm.hash.Jenkins.JenkinsUDF';

!echo "******************************************************************************************************************************************";
!echo "***  STEP 1 : INSERT RECORDS INTO mrdw_dim_mcom_ctac_rejects_ex_repl FROM mrdw_stg_mcom_ctac_rejects_ex";
!date;
!echo "******************************************************************************************************************************************";

INSERT OVERWRITE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_rejects_ex_repl 
SELECT dtl_rec_type,
       sequence_num,
       connect_num,
       property_cd,
       confo_num_orig,
       confo_num_curr,
       date_create_key,
       booking_iata_num,
       attributing_iata_num,
       traffic_src_cd,
       --arrival_dt is in string format, convert to DATE format
       DATE_ADD('arrival_dt', 0) AS arrival_dt,
       market_cd,
       --percentage does not include the decimal, which always occurs after the first three digits, so divide by 100
       percentage/100 AS percentage,
       reject_reASon_cd_01,
       reject_reASon_cd_02,
       reject_reASon_cd_03,
       reject_reASon_cd_04,
       reject_reASon_cd_05,
       reject_reASon_cd_06,
       reject_reASon_cd_07,
       reject_reASon_cd_08,
       reject_reASon_cd_09,
       reject_reASon_cd_10,
       reject_reASon_cd_11,
       reject_reASon_cd_12,
       reject_reASon_cd_13,
       reject_reASon_cd_14,
       reject_reASon_cd_15,
       reject_reASon_cd_16,
       reject_reASon_cd_17,
       reject_reASon_cd_18,
       reject_reASon_cd_19,
       reject_reASon_cd_20,
       CURRENT_TIMESTAMP() AS dw_load_ts
  FROM ${hiveconf:STG_SCHEMA}.mrdw_stg_mcom_ctac_rejects_ex
;

!echo "******************************************************************************************************************************************";
!echo "****DQ CHECKS****"
!echo "******************************************************************************************************************************************";

-- DQ CHECK 1.1: Make sure the REPL table contains at least 1 record;
INSERT OVERWRITE TABLE ${hiveconf:ETL_SCHEMA}.error_report_table
SELECT CASE COUNT(*) WHEN 0 THEN 1 ELSE 0 END AS error_code,
       'NO_RECORDS_LOADED' error_abv,
       'mrdw_dim_mcom_ctac_rejects_ex_repl' error_table,
       COUNT(*) row_cnt
  FROM ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_rejects_ex_repl foo
;

-- DQ CHECK 1.2: Verify no duplicate records exist;
INSERT INTO ${hiveconf:ETL_SCHEMA}.error_report_table
SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END as error_code,
       'DUPLICATE_RECORDS' error_abv,
       'mrdw_dim_mcom_ctac_rejects_ex_repl' error_table,
       COUNT(*) row_cnt
  FROM (SELECT dtl_rec_type,
               sequence_num,
               connect_num,
               property_cd,
               confo_num_orig,
               confo_num_curr,
               date_create_key,
               booking_iata_num,
               attributing_iata_num,
               traffic_src_cd,
               arrival_dt,
               market_cd,
               percentage,
               reject_reASon_cd_01,
               reject_reASon_cd_02,
               reject_reASon_cd_03,
               reject_reASon_cd_04,
               reject_reASon_cd_05,
               reject_reASon_cd_06,
               reject_reASon_cd_07,
               reject_reASon_cd_08,
               reject_reASon_cd_09,
               reject_reASon_cd_10,
               reject_reASon_cd_11,
               reject_reASon_cd_12,
               reject_reASon_cd_13,
               reject_reASon_cd_14,
               reject_reASon_cd_15,
               reject_reASon_cd_16,
               reject_reASon_cd_17,
               reject_reASon_cd_18,
               reject_reASon_cd_19,
               reject_reASon_cd_20
          FROM ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_rejects_ex_repl
      GROUP BY dtl_rec_type,
               sequence_num,
               connect_num,
               property_cd,
               confo_num_orig,
               confo_num_curr,
               date_create_key,
               booking_iata_num,
               attributing_iata_num,
               traffic_src_cd,
               arrival_dt,
               market_cd,
               percentage,
               reject_reASon_cd_01,
               reject_reASon_cd_02,
               reject_reASon_cd_03,
               reject_reASon_cd_04,
               reject_reASon_cd_05,
               reject_reASon_cd_06,
               reject_reASon_cd_07,
               reject_reASon_cd_08,
               reject_reASon_cd_09,
               reject_reASon_cd_10,
               reject_reASon_cd_11,
               reject_reASon_cd_12,
               reject_reASon_cd_13,
               reject_reASon_cd_14,
               reject_reASon_cd_15,
               reject_reASon_cd_16,
               reject_reASon_cd_17,
               reject_reASon_cd_18,
               reject_reASon_cd_19,
               reject_reASon_cd_20
        HAVING COUNT(*) > 1 ) foo
;

!echo "******************************************************************************************************************************************";
!echo "***  STEP 3: POPULATE mrdw_dim_mcom_ctac_rejects_ex FROM REPLICA TABLE";
!date;
!echo "******************************************************************************************************************************************";

INSERT INTO ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_rejects_ex 
SELECT dtl_rec_type,
       sequence_num,
       connect_num,
       property_cd,
       confo_num_orig,
       confo_num_curr,
       DATE_create_key,
       booking_iata_num,
       attributing_iata_num,
       traffic_src_cd,
       arrival_dt,
       market_cd,
       percentage,
       reject_reASon_cd_01,
       reject_reASon_cd_02,
       reject_reASon_cd_03,
       reject_reASon_cd_04,
       reject_reASon_cd_05,
       reject_reASon_cd_06,
       reject_reASon_cd_07,
       reject_reASon_cd_08,
       reject_reASon_cd_09,
       reject_reASon_cd_10,
       reject_reASon_cd_11,
       reject_reASon_cd_12,
       reject_reASon_cd_13,
       reject_reASon_cd_14,
       reject_reASon_cd_15,
       reject_reASon_cd_16,
       reject_reASon_cd_17,
       reject_reASon_cd_18,
       reject_reASon_cd_19,
       reject_reASon_cd_20,
       dw_load_ts 
  FROM ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_rejects_ex_repl
;

!echo "******************************************************************************************************************************************";
!echo "***  STEP 4: Insert values into MRDW_ETL_TABLE_AUDIT for MRDW_DIM_MCOM_CTAC_REJECTS_EX";
!date;
!echo "******************************************************************************************************************************************";

INSERT INTO ${hiveconf:TGT_SCHEMA}.mrdw_etl_table_audit
SELECT JENKINS(NVL(a.dw_load_ts,CURRENT_TIMESTAMP())) AS audit_key,
       NVL(a.dw_load_ts,CURRENT_TIMESTAMP()) AS dw_load_ts,
       a.arrival_dt,
       'MRDW_DIM_MCOM_CTAC_REJECTS_EX',
       NULL,
       NULL,
       a.record_cnt,
       'ATRBCTAC',
       CURRENT_TIMESTAMP()
  FROM (SELECT MAX(dw_load_ts) AS dw_load_ts,
               MAX(arrival_dt) AS arrival_dt, 
			   COUNT(*) AS record_cnt 
	      FROM ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_rejects_ex_repl ) a,
       (SELECT COUNT(*) AS cnt 
	      FROM ${hiveconf:ETL_SCHEMA}.error_report_table 
		 WHERE error_code = 1)b 
 WHERE b.cnt < 1;
 
!echo "******************************************************************************************************************************************";
!echo "***  STEP 5: Generate Statistics for Result Tables"
!date;
!echo "******************************************************************************************************************************************";

ANALYZE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_rejects_ex COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_rejects_ex_repl COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_etl_table_audit COMPUTE STATISTICS;



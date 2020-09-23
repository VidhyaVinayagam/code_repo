--############################################################################################################################
--# File Name      :     daily_load_mrdw_dim_mcom_ctac_iata_ex.hql
--# Prerequisite   : 
--# Description    :     This script inserts CTAC IATA codes from mrdw_stg_mcom_ctac_iata_ex to mrdw_dim_mcom_ctac_iata_ex  
--#
--# Change history
--# Name           DATE         Modification
--# ====           ====         ============
--# Vidya           08/02/2017     Initial Version
--#############################################################################################################################

set mapred.job.queue.name=${hiveconf:queue_name};

-- The amount of memory to request from the scheduler for each map task.
Set mapreduce.map.memory.mb=4384;
Set mapreduce.map.java.opts=-Xmx4288m;

-- The amount of memory to request from the scheduler for each reduce task.
Set mapreduce.reduce.memory.mb=4384;
Set mapreduce.reduce.java.opts=-Xmx4288m;

set hive.auto.convert.join=true;
Set yarn.nodemanager.vmem-pmem-ratio=2.1;
set hive.auto.convert.sortmerge.join=true;

ADD JAR ${hiveconf:jenkins_path}/JenkinsUDF.jar;
CREATE  TEMPORARY FUNCTION Jenkins as 'com.marriott.ddm.hash.Jenkins.JenkinsUDF';

!echo "******************************************************************************************************************************************";
!echo "***  STEP 1 : INSERT RECORDS INTO mrdw_dim_mcom_ctac_iata_ex_repl FROM mrdw_stg_mcom_ctac_iata_ex";
!date;
!echo "******************************************************************************************************************************************";

INSERT OVERWRITE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_iata_ex_repl
SELECT mpm_iata_num as atrb_iata_id,
       mpm_iata_name as atrb_iata_nm,
       mpm_pgm_type as prgm_type_txt,
       mpm_active_ind as active_ind,
       mpm_traffic_src_cd as camp_source_abrv_txt,
       CURRENT_TIMESTAMP() as dw_load_ts
FROM ${hiveconf:STG_SCHEMA}.mrdw_stg_mcom_ctac_iata_ex;

!echo "******************************************************************************************************************************************";
!echo "*DQ CHECKS*"
!echo "******************************************************************************************************************************************";

-- DQ CHECK 1.1: Make sure the REPL table contains at least 1 record;
INSERT OVERWRITE TABLE ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
SELECT CASE COUNT(*) WHEN 0 THEN 1 ELSE 0 END AS error_code,
       'NO_RECORDS_LOADED' error_abv,
       'mrdw_dim_mcom_ctac_iata_ex_repl' error_table,
       COUNT(*) row_cnt
  FROM ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_iata_ex_repl foo
;

-- DQ CHECK 1.2: Verify no duplicate records exist;
INSERT INTO ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
SELECT CASE COUNT(*) WHEN 0 THEN 0 ELSE 1 END as error_code,
       'DUPLICATE_RECORDS' error_abv,
       'mrdw_dim_mcom_ctac_iata_ex_repl' error_table,
       COUNT(*) row_cnt
  FROM (SELECT atrb_iata_id,
               atrb_iata_nm, 
			   camp_source_abrv_txt, 
			   prgm_type_txt, 
			   active_ind
          FROM ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_iata_ex_repl
GROUP BY atrb_iata_id, 
         atrb_iata_nm, 
		 camp_source_abrv_txt, 
		 prgm_type_txt, 
		 active_ind
  HAVING COUNT(*) > 1 ) foo
;

!echo "******************************************************************************************************************************************";
!echo "***  STEP 2 : POPULATE mrdw_dim_mcom_ctac_iata_ex FROM REPLICA TABLE";
!date;
!echo "******************************************************************************************************************************************";

INSERT INTO TABLE ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_iata_ex
SELECT atrb_iata_id,
       atrb_iata_nm,
       prgm_type_txt,
       active_ind,
       CAMP_SOURCE_ABRV_TXT,
       dw_load_ts 
  FROM ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_iata_ex_repl a,
      (SELECT COUNT(*) AS cnt FROM ${hiveconf:ETL_SCHEMA}.temp_fail_ctac 
	    WHERE error_code = 1) b
 WHERE b.cnt < 1
;

!echo "******************************************************************************************************************************************";
!echo "***  STEP 3 : UpDATE audit table for new mrdw_dim_mcom_ctac_iata_ex inserts";
!date;
!echo "******************************************************************************************************************************************";

INSERT INTO ${hiveconf:TGT_SCHEMA}.mrdw_etl_table_audit
SELECT Jenkins(NVL(a.dw_load_ts,CURRENT_TIMESTAMP())) AS audit_key,
       NVL(a.dw_load_ts,CURRENT_TIMESTAMP()) AS initial_load_ts,
       b.run_dt AS data_batch_dt,
       'MRDW_DIM_MCOM_CTAC_IATA_EX'  AS table_nm,
       NULL AS min_data_ts,
       NULL AS max_data_ts,
       NVL(a.record_cnt,0) AS record_amt,
       'ATRBCTAC' AS sys_id,
       CURRENT_TIMESTAMP() AS dw_load_ts
  FROM (SELECT MAX(dw_load_ts) AS dw_load_ts, COUNT(*) AS record_cnt 
          FROM ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_iata_ex_repl ) a,
       (SELECT run_dt-2 AS run_dt 
	      FROM ${hiveconf:STG_SCHEMA}.mrdw_stg_run_DATE 
		 WHERE sys_id = 'ATRBCTAC' ) b,
       (SELECT COUNT(*) AS cnt 
	      FROM ${hiveconf:ETL_SCHEMA}.temp_fail_ctac
         WHERE error_code = 1) c
  WHERE c.cnt < 1
;

!echo "******************************************************************************************************************************************";
!echo "***  STEP 4: Generate Statistics for Result Tables"
!date;
!echo "******************************************************************************************************************************************";

ANALYZE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_iata_ex COMPUTE STATISTICS;
ANALYZE TABLE ${hiveconf:TGT_SCHEMA}.mrdw_dim_mcom_ctac_iata_ex_repl COMPUTE STATISTICS;

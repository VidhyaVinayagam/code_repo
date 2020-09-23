/*
##############################################################################################################################
# File Name      : swi_load_tgt_cus_cdactionreason.sql
# Description    : This script loads the target table SWI_CUS_CDACTIONREASON from stg table SWI_STG_CUS_CDACTIONREASON table.
# Load Type      : Full Refresh
# Change history
# Name                  Date          Modification
# ====                  ====          ============
# Vidya             12/30/2016    Initial Version 1.1
##############################################################################################################################
*/

/* Step 1: Create temporary table AW_TGT_BUS_DBO.SWI_CUS_CDACTIONREASON_TEMP1 similar to AW_TGT_BUS_DBO.SWI_CUS_CDACTIONREASON */
CREATE HADOOP TABLE aw_tgt_bus_dbo.swi_cus_cdactionreason_temp1 LIKE aw_tgt_bus_dbo.swi_cus_cdactionreason;
GRANT ALL ON aw_tgt_bus_dbo.swi_cus_cdactionreason_temp1 TO ALL;

/* Step 2: Load the temporary table AW_TGT_BUS_DBO.SWI_CUS_CDACTIONREASON_TEMP1 with all the records from AW_TGT_BUS_DBO.SWI_CUS_CDACTIONREASON */
/* except the ones in AW_STG_BUS_DBO.SWI_STG_CUS_CDACTIONREASON */
INSERT INTO aw_tgt_bus_dbo.swi_cus_cdactionreason_temp1 (
            actn_reason_cd,
            actn_reason_desc,
            load_dt )
     SELECT actn_reason_cd,
			actn_reason_desc,
			load_dt
       FROM aw_tgt_bus_dbo.swi_cus_cdactionreason 
      WHERE actn_reason_cd NOT IN ( SELECT actn_reason_cd 
									  FROM aw_stg_bus_dbo.swi_stg_cus_cdactionreason );

/* Step 3: Load the temp data from the stage table into the temp table with system load date */
INSERT INTO aw_tgt_bus_dbo.swi_cus_cdactionreason_temp1 (
			actn_reason_cd,
			actn_reason_desc,
			load_dt )
     SELECT b.actn_reason_cd,
			b.actn_reason_desc,
			NOW() AS load_dt  
	   FROM aw_stg_bus_dbo.swi_stg_cus_cdactionreason b;

/* Step 4: Truncate the actual target table */
TRUNCATE TABLE aw_tgt_bus_dbo.swi_cus_cdactionreason;

/* Step 5: Load the data from the temp table into the target table */
INSERT INTO aw_tgt_bus_dbo.swi_cus_cdactionreason (
			actn_reason_cd,
			actn_reason_desc,
			load_dt )
     SELECT actn_reason_cd,
			actn_reason_desc,
			load_dt 
	   FROM aw_tgt_bus_dbo.swi_cus_cdactionreason_temp1;

/* Step 6: Drop the temp table */
DROP TABLE aw_tgt_bus_dbo.swi_cus_cdactionreason_temp1;


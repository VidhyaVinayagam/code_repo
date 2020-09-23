/*
##############################################################################################################################
# File Name      : swi_load_tgt_cus_cdfreqtrvlaction.sql
# Description    : This script loads the target table SWI_CUS_CDFREQTRVLACTION from stg table SWI_STG_CUS_CDFREQTRVLACTION table.
# Load Type      : Full Refresh
# Change history
# Name                  Date          Modification
# ====                  ====          ============
# Vidya             12/30/2016    Initial Version 1.1
##############################################################################################################################
*/

/* Step 1: Create temporary table AW_TGT_BUS_DBO.SWI_CUS_CDFREQTRVLACTION_TEMP1 similar to AW_TGT_BUS_DBO.SWI_CUS_CDFREQTRVLACTION */
CREATE HADOOP TABLE aw_tgt_bus_dbo.swi_cus_cdfreqtrvlaction_temp1 LIKE aw_tgt_bus_dbo.swi_cus_cdfreqtrvlaction;
GRANT ALL ON aw_tgt_bus_dbo.swi_cus_cdfreqtrvlaction_temp1 TO ALL;

/* Step 2: Load the temporary table AW_TGT_BUS_DBO.SWI_CUS_CDFREQTRVLACTION_TEMP1 with all the records from AW_TGT_BUS_DBO.SWI_CUS_CDFREQTRVLACTION */
/* except the ones in AW_STG_BUS_DBO.SWI_STG_CUS_CDFREQTRVLACTION */
INSERT INTO aw_tgt_bus_dbo.swi_cus_cdfreqtrvlaction_temp1 (
			freq_travel_actn_cd,
			freq_travel_actn_desc,
			load_dt )
     SELECT freq_travel_actn_cd,
			freq_travel_actn_desc,
			load_dt 
	   FROM aw_tgt_bus_dbo.swi_cus_cdfreqtrvlaction 
      WHERE freq_travel_actn_cd NOT IN ( SELECT freq_travel_actn_cd 
										 FROM aw_stg_bus_dbo.swi_stg_cus_cdfreqtrvlaction );

/* Step 3: Load the temp data from the stage table into the temp table with system load date */
INSERT INTO aw_tgt_bus_dbo.swi_cus_cdfreqtrvlaction_temp1 (
			freq_travel_actn_cd,
			freq_travel_actn_desc,
			load_dt )
     SELECT b.freq_travel_actn_cd,
			b.freq_travel_actn_desc,
			NOW() AS load_dt  
	   FROM aw_stg_bus_dbo.swi_stg_cus_cdfreqtrvlaction b;

/* Step 4: Truncate the actual target table */
TRUNCATE TABLE aw_tgt_bus_dbo.swi_cus_cdfreqtrvlaction;

/* Step 5: Load the data from the temp table into the target table */
INSERT INTO aw_tgt_bus_dbo.swi_cus_cdfreqtrvlaction (
            freq_travel_actn_cd,
            freq_travel_actn_desc,
            load_dt )
     SELECT freq_travel_actn_cd,
			freq_travel_actn_desc,
			load_dt 
	   FROM aw_tgt_bus_dbo.swi_cus_cdfreqtrvlaction_temp1;

/* Drop the temp table */
DROP TABLE aw_tgt_bus_dbo.swi_cus_cdfreqtrvlaction_temp1;


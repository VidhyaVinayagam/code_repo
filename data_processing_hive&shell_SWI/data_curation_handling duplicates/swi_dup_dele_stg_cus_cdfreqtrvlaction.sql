/*
############################################################################################################################
# File Name      : swi_dup_dele_stg_cus_cdfreqtrvlaction.sql
# Description    : This script delete the duplicate data in stg table SWI_STG_CUS_CDFREQTRVLACTION table.
#
# Change history
# Name                  Date          Modification
# ====                  ====          ============
# Vidya             12/30/2016    Initial Version 1.1
############################################################################################################################
*/

/* Create a temp table that has unique set of records from stage table without any duplicates using row_number concept */

CREATE HADOOP TABLE AW_STG_BUS_DBO.SWI_STG_CUS_CDFREQTRVLACTION_TEMP LIKE AW_STG_BUS_DBO.SWI_STG_CUS_CDFREQTRVLACTION;
GRANT ALL ON AW_STG_BUS_DBO.SWI_STG_CUS_CDFREQTRVLACTION_TEMP TO ALL;

INSERT INTO AW_STG_BUS_DBO.SWI_STG_CUS_CDFREQTRVLACTION_TEMP
SELECT FREQ_TRAVEL_ACTN_CD,
FREQ_TRAVEL_ACTN_DESC FROM
(SELECT FREQ_TRAVEL_ACTN_CD,
FREQ_TRAVEL_ACTN_DESC,
row_number() over (PARTITION BY FREQ_TRAVEL_ACTN_CD,
FREQ_TRAVEL_ACTN_DESC
ORDER BY FREQ_TRAVEL_ACTN_CD DESC) row_num
FROM AW_STG_BUS_DBO.SWI_STG_CUS_CDFREQTRVLACTION) a 
WHERE a.row_num=1;

/* Truncate the actual stage table */

TRUNCATE TABLE AW_STG_BUS_DBO.SWI_STG_CUS_CDFREQTRVLACTION;

/* Load the records from temp table to stage table */

INSERT INTO AW_STG_BUS_DBO.SWI_STG_CUS_CDFREQTRVLACTION(
FREQ_TRAVEL_ACTN_CD,
FREQ_TRAVEL_ACTN_DESC)
SELECT FREQ_TRAVEL_ACTN_CD,
FREQ_TRAVEL_ACTN_DESC FROM AW_STG_BUS_DBO.SWI_STG_CUS_CDFREQTRVLACTION_TEMP;

COMMIT;

/* Drop the temp table */

DROP TABLE AW_STG_BUS_DBO.SWI_STG_CUS_CDFREQTRVLACTION_TEMP;

/*
############################################################################################################################
# File Name      : swi_dup_dele_stg_cus_cdactionreason.sql
# Description    : This script delete the duplicate data in stg table SWI_STG_CUS_CDACTIONREASON table.
#
# Change history
# Name                  Date          Modification
# ====                  ====          ============
# Vidya             12/30/2016    Initial Version 1.1
############################################################################################################################
*/

/* Create a temp table that has unique set of records from stage table without any duplicates using row_number concept */

CREATE HADOOP TABLE AW_STG_BUS_DBO.SWI_STG_CUS_CDACTIONREASON_TEMP LIKE AW_STG_BUS_DBO.SWI_STG_CUS_CDACTIONREASON;
GRANT ALL ON AW_STG_BUS_DBO.SWI_STG_CUS_CDACTIONREASON_TEMP TO ALL;

INSERT INTO AW_STG_BUS_DBO.SWI_STG_CUS_CDACTIONREASON_TEMP
SELECT ACTN_REASON_CD,
ACTN_REASON_DESC FROM
(SELECT ACTN_REASON_CD,
ACTN_REASON_DESC,
row_number() over (PARTITION BY ACTN_REASON_CD,
ACTN_REASON_DESC
ORDER BY ACTN_REASON_CD DESC) row_num
FROM AW_STG_BUS_DBO.SWI_STG_CUS_CDACTIONREASON) a 
WHERE a.row_num=1;

/* Truncate the actual stage table */

TRUNCATE TABLE AW_STG_BUS_DBO.SWI_STG_CUS_CDACTIONREASON;

/* Load the records from temp table to stage table */

INSERT INTO AW_STG_BUS_DBO.SWI_STG_CUS_CDACTIONREASON(
ACTN_REASON_CD,
ACTN_REASON_DESC)
SELECT ACTN_REASON_CD,
ACTN_REASON_DESC FROM AW_STG_BUS_DBO.SWI_STG_CUS_CDACTIONREASON_TEMP;

COMMIT;

/* Drop the temp table */

DROP TABLE AW_STG_BUS_DBO.SWI_STG_CUS_CDACTIONREASON_TEMP;

set hive.execution.engine=mr;
set hive.exec.parallel=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=405306368;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled =false;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
set hive.merge.mapfiles =true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=134217728;
set hive.merge.smallfiles.avgsize=44739242;
set mapreduce.job.reduce.slowstart.completedmaps=0.8;

DROP VIEW datahub_mstr_poc_dbo.datahub_poc_fcams;
CREATE VIEW datahub_mstr_poc_dbo.datahub_poc_fcams
AS
SELECT CAST(L1.A_FMCApplication_ID AS INT) AS App_No
,CAST(SUBSTRING(L1.A_Short_Name,1,50) AS VARCHAR(50)) AS  App_Name
,CAST(SUBSTRING(L1.A_FMCApplication_Acronym,1,10) AS VARCHAR(10)) AS App_Acronym
,family.App_Family_Name AS App_Family_Name
,Org.IT_Supp_Org_Name AS IT_Supp_Org_Name
,itms.app_created_dt
,itms.app_update_dt
,itms.App_Prac_Supvsr AS Support_Application_Supervisor
,CAST(L1.A_FMCApp_Deleted_Flag AS VARCHAR(10)) AS App_Deleted_Flag
,L1.A_FMCApplication_Lifecycle_State AS App_Status
,itms.App_AppGrp_Mgr AS BIT_Application_Group_Manager
,buscrt.ITMSA14_NAME_N AS Business_Criticality
,lookup.TMSA77_LOOKUP_N AS Application_SLA
FROM dsc60192_eams_tz_db.C_Application L1
LEFT JOIN dsc60172_itms_tz_db.application itms ON L1.A_FMCApplication_ID = itms.app_no
LEFT JOIN dsc60172_itms_tz_db.App_Family family
ON  itms.App_Family_No = family.App_Family_No
LEFT JOIN dsc60172_itms_tz_db.IT_Support_Org Org
ON itms.IT_Supp_Org_No = Org.IT_Supp_Org_No
LEFT JOIN dsc60172_itms_tz_db.ITMSA14_BUSCRT Buscrt
ON itms.ITMSA14_ID_R = Buscrt.ITMSA14_ID_R
LEFT JOIN dsc60172_itms_tz_db.ITMSA77_LOOKUP lookup
ON lookup.TMSA77_LOOKUP_K = itms.TMS77_APP_SLA_K and lookup.TMSA77_LOOKUP_TYPE_C = 'SLA';

--6138


DROP VIEW datahub_mstr_poc_dbo.itms_poc_fcams;
CREATE VIEW datahub_mstr_poc_dbo.itms_poc_fcams
AS
select itms.App_No,
itms.App_Name,
itms.App_Acronym,
fam.App_Family_Name,
supp_org.IT_Supp_Org_Name,
itms.App_Created_Dt,
itms.App_Update_Dt,
itms.App_Prac_Supvsr,
itms.App_Deleted_Flag,
stat.App_Status_Name,
itms.App_Retirement_Dt,
itms.App_AppGrp_Mgr,
buscrt.ITMSA14_NAME_N AS Business_Criticality,
ITMSA77.TMSA77_LOOKUP_N AS Application_SLA
from  dsc60172_itms_tz_db.Application itms
LEFT JOIN dsc60172_itms_tz_db.App_Family fam
ON  itms.App_Family_No = fam.App_Family_No
LEFT JOIN dsc60172_itms_tz_db.IT_Support_Org supp_org
ON itms.IT_Supp_Org_No = supp_org.IT_Supp_Org_No
LEFT JOIN dsc60172_itms_tz_db.App_Status stat
ON itms.Status_No = stat.App_Status_No
LEFT JOIN dsc60172_itms_tz_db.ITMSA14_BUSCRT buscrt
ON itms.ITMSA14_ID_R = buscrt.ITMSA14_ID_R
LEFT JOIN dsc60172_itms_tz_db.ITMSA77_LOOKUP ITMSA77
ON ITMSA77.TMSA77_LOOKUP_K = itms.TMS77_APP_SLA_K and ITMSA77.TMSA77_LOOKUP_TYPE_C = 'SLA';

select a.app_no,a.app_name,a.app_acronym,a.app_family_name,a.it_supp_org_name,a.app_created_dt,a.app_update_dt,a.app_prac_supvsr,a.app_deleted_flag,a.App_Status_Name,a.App_Retirement_Dt,a.app_appgrp_mgr,a.business_criticality,a.application_sla
,b.App_No,b.App_Name,b.App_Acronym,b.App_Family_Name,b.IT_Supp_Org_Name,b.app_created_dt,b.app_update_dt,b.Support_Application_Supervisor,b.App_Deleted_Flag,b.App_Status,b.BIT_Application_Group_Manager,b.Business_Criticality,b.application_sla
FROM datahub_mstr_poc_dbo.itms_poc_fcams a LEFT JOIN datahub_mstr_poc_dbo.datahub_poc_fcams b ON a.App_No = b.App_No
ORDER BY a.App_No;


select * FROM datahub_mstr_poc_dbo.itms_poc_fcams a LEFT JOIN datahub_mstr_poc_dbo.datahub_poc_fcams b ON a.App_No = b.App_No
WHERE b.app_no IS NOT NULL AND (a.it_supp_org_name <> b.IT_Supp_Org_Name
OR a.app_created_dt <> b.app_created_dt
OR a.app_update_dt <> b.app_update_dt
OR a.app_prac_supvsr <> b.Support_Application_Supervisor
OR a.App_Status_Name <> b.App_Status
OR a.app_appgrp_mgr <> b.BIT_Application_Group_Manager
OR a.business_criticality <> b.Business_Criticality
OR a.application_sla <> b.application_sla)
ORDER BY a.App_No;
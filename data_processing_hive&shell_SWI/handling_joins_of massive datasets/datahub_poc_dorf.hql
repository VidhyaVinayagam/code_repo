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

DROP VIEW datahub_mstr_poc_dbo.datahub_poc_dorf;
CREATE VIEW datahub_mstr_poc_dbo.datahub_poc_dorf
AS
SELECT CAST(L1.A_FMCApplication_ID AS INT)  AS A1
,CAST(SUBSTRING(L1.A_Short_Name,1,50) AS VARCHAR(50)) AS  A2
,CAST(SUBSTRING(L1.A_FMCApplication_Acronym,1,10) AS VARCHAR(10)) AS A3
,itms.application_family AS A4
,CAST(L1.A_FMCApp_Deleted_Flag AS VARCHAR(10)) AS A9
,L1.A_FMCApplication_Lifecycle_State AS A10
,itms.Support_Application_Supervisor AS Support_Application_Supervisor
,itms.BIT_Application_Group_Manager
,itms.IT_Support_Organization
,itms.app_created_dt
,itms.app_update_dt
FROM dsc10718_eams_tz_db.C_Application L1
LEFT JOIN datahub_mstr_poc_dbo.datahub_tgt_tbl_direct itms ON L1.A_FMCApplication_ID = itms.itms_application_num
union all
select ITMSA01_ID_R,
ITMSA01_NAME_N,
ITMSA01_ACRONYM_N,
null,
CAST(ITMSA01_DELETED_F AS VARCHAR(10)),
ITMSA35_OTHER_IT_N,
null,
null,
null,
null,
null
FROM dsc10626_itms_tz_db.ITMSA01_OTHIT O JOIN dsc10626_itms_lz_db.ITMSA35_OTHIT_TYPE T
ON O.ITMSA35_OTHIT_TYPE_ID_D = T.ITMSA35_OTHIT_TYPE_ID_D WHERE O.ITMSA35_OTHIT_TYPE_ID_D In (3,4,6,8);



CREATE view [dbo].[ITMS_VW_Applications_OthIT]
as

Select
Application.App_No A1,
Application.App_Name A2,
Application.App_Acronym A3,
App_Family.App_Family_Name A4,
IT_Support_Org.IT_Supp_Org_Name A5,
Application.App_Created_Dt A6,
Application.App_Update_Dt A7,
Application.App_Prac_Supvsr A8,
Application.App_Deleted_Flag A9,
App_Status.App_Status_Name A10,
Application.App_AppGrp_Mgr A11,
Application.APP_SOX_F A12,
Application.App_Launch_Dt A13,
Application.App_Tech_Lifecycle_Lan A14,
'A15' =(Select top 1 Attribute.Attrib_Name  From
App_Attrib  AppAttrib,
Attribute   Attribute,
Attrib_Type AttribType
Where AppAttrib.Attrib_No = Attribute.Attrib_No and
Attribute.Attrib_Type_No = AttribType.Attrib_Type_No and
AttribType.Attrib_Type_Name = '1.0-SLA-Service Level Agreement'
AND AppAttrib.App_No = Application.App_No),
'A16' =(Select top 1 Attribute.Attrib_Name  From
App_Attrib  AppAttrib,
Attribute   Attribute,
Attrib_Type AttribType
Where AppAttrib.Attrib_No = Attribute.Attrib_No and
Attribute.Attrib_Type_No = AttribType.Attrib_Type_No and
AttribType.Attrib_Type_Name = '1.2-SLA- Availability %'
AND AppAttrib.App_No = Application.App_No),
'A17' =(Select top 1 Attribute.Attrib_Name  From
App_Attrib  AppAttrib,
Attribute   Attribute,
Attrib_Type AttribType
Where AppAttrib.Attrib_No = Attribute.Attrib_No and
Attribute.Attrib_Type_No = AttribType.Attrib_Type_No and
AttribType.Attrib_Type_Name = '2.0 - Application Information - Release Schedule'
AND AppAttrib.App_No = Application.App_No),
APP_SOX_F A18,
APP_CONFIDENTIALITY_C A19,
APP_INTEGRITY_C A20,
APP_AVAILABILITY_C A21,
/*Code added based on the RQ#1235150*/
ITMSA89_ACR_MASTER.TMSA89_ACR_R A22,
ITMSA89_ACR_MASTER.TMSA89_INTRN_CTL_COORDINATOR_N A23,
ITMSA89_ACR_MASTER.TMSA89_SCC_USER_D A24,
ITMSA89_ACR_MASTER.TMSA89_STATUS_C A25,
ITMSA77_LOOKUP.TMSA77_LOOKUP_N A26
from   Application
INNER JOIN App_Status
ON Application.Status_No = App_Status.App_Status_No
LEFT JOIN App_Family
ON Application.App_Family_No = App_Family.App_Family_No
LEFT JOIN IT_Support_Org
ON Application.IT_Supp_Org_No = IT_Support_Org.IT_Supp_Org_No
/*Code added based on the RQ#1235150*/
LEFT JOIN ITMSA90_ACR_ITMS_MAP
ON ITMSA90_ACR_ITMS_MAP.APP_NO = Application.App_No
LEFT JOIN ITMSA89_ACR_MASTER
ON ITMSA89_ACR_MASTER.TMSA89_ACR_R = ITMSA90_ACR_ITMS_MAP.TMSA89_ACR_R
LEFT JOIN ITMSA77_LOOKUP
ON Application.TMSA77_ITMS_TYPE_K = ITMSA77_LOOKUP.TMSA77_LOOKUP_K AND TMSA77_LOOKUP_TYPE_C = 'ITM'


union all

select ITMSA01_ID_R,
ITMSA01_NAME_N,
left(ITMSA01_ACRONYM_N, 30) ITMSA01_ACRONYM_N,
null,
null,
null,
null,
null,
ITMSA01_DELETED_F,
ITMSA35_OTHER_IT_N,
null,
0,
null,
null,
null,
null,
null,
null,
null,
null,
null,
null,
null,
null,
null,
null
from   dbo.ITMSA01_OTHIT O,
dbo.ITMSA35_OTHIT_TYPE T
where  O.ITMSA35_OTHIT_TYPE_ID_D = T.ITMSA35_OTHIT_TYPE_ID_D and
O.ITMSA35_OTHIT_TYPE_ID_D In (3,4,6,8)




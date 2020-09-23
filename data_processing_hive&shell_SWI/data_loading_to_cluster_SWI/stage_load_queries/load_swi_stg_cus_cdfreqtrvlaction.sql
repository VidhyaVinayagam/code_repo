--/*
--############################################################################################################################
--# File Name      : swi_load_stg_cus_cdfreqtrvlaction.sql
--# Description    : This script loads the swi_stg_cus_cdfreqtrvlaction stage table in  aw_stg_bus_dbo database using file  
--#	         		 /temp/starwood/CUS_CDFREQTRVLACTION_SW_201609.txt.
--#
--# Change history
--# Name                  Date          Modification
--# ====                  ====          ============
--# Vidya           12/28/2016    Initial Version 1.1
--############################################################################################################################

--# Define Hadoop Properties #

\set expand=true

set hadoop property mapreduce.map.memory.mb=3400;
set hadoop property mapreduce.map.java.opts=-Xmx2720m;
set hadoop property mapreduce.reduce.memory.mb=9800;
set hadoop property mapreduce.reduce.java.opts=-Xmx7840m;
set hadoop property yarn.app.mapreduce.am.resource.mb=2000;
set hadoop property yarn.app.mapreduce.am.command-opts=-Xmx1600m;
set hadoop property yarn.nodemanager.resource.memory-mb=104448;
set hadoop property yarn.scheduler.minimum-allocation-mb=512;
set hadoop property yarn.scheduler.maximum-allocation-mb=104448;

--# Loading swi_stg_cus_cdfreqtrvlaction stage table in  aw_stg_bus_dbo database using file /temp/starwood/CUS_CDFREQTRVLACTION_SW_201609.txt

LOAD HADOOP USING FILE 
URL '/temp/starwood/CUS_CDFREQTRVLACTION_SW_201609.txt'
WITH SOURCE PROPERTIES ('field.delimiter'='|','ignore.extra.fields'='true','escape.char'='\\','skip.lines.count'='1','date.time.format'='yyyy-MM-dd')
INTO TABLE aw_stg_bus_dbo.swi_stg_cus_cdfreqtrvlaction OVERWRITE
WITH LOAD PROPERTIES ('num.map.tasks' = 50,'max.rejected.records'=0,'rejected.records.dir'='/temp/starwood/reject');
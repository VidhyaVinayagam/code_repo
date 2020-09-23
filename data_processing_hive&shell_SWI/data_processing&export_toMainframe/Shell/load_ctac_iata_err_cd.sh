#!/bin/ksh
#########################################################################################################
#  File Name: load_ctac_iata_err_cd.sh
#
#  Description : This script executes the sqoop command
#			 	 to load data from the MMT.MMT_MP_MASTER and MMT.MMT_CTACODES_BATCH
#                tables (DB2) into MRDW_STG_MCOM_IATA_CTAC_EX and MRDW_STG_MCOM_CTAC_ERR_CD_EX
#				 in Hadoop, respectively. 
#
#                There are 6 major steps in the script 
#
#                1: Check whether or not the control file $CTL_FILE exists, create 
#                   one if it does not exist with a value of 1 so that, the process always starts with 
#                   the first session unless previous run was not successful. 
#
#                2: Populate the parameter file $MRDW_PARAMFILES/CTAC_IATA/t_ctac_iata.par with
#					OWNER variable value, then start the sqoop command to populate staging table 
#					MRDW_STG_MCOM_CTAC_IATA_EX
#
#                3: Call Hive script to populate target table MRDW_DIM_MCOM_IATA_CTAC_EX 
#                4: Populate the parameter file $MRDW_PARAMFILES/CTAC_IATA/t_ctac_err_cd.par with
#					OWNER variable value, then execute the sqoop command to populate staging table 
#					MRDW_STG_MCOM_CTAC_ERROR_CODES_EX
#
#                5: Call Hive script to populate target table MRDW_DIM_MCOM_CTAC_ERR_CD_EX 
#
#  Modification Log
#  Name            Date         Log
#  -------------   ---------    ----------------------------------------------------------------------
#  Vidya    29/12/17     Created
##########################################################################################################

PROGRAM_NAME=`basename $0|cut -d"." -f1`
PROCESS_DATE=`date '+%Y%m%d%H%M'`

CURR_DIR=$LZSCRIPT
CTL_FILE="${PROGRAM_NAME}.ctl"

IATA_STG_TABLE="mrdw_stg_mcom_ctac_iata_ex"
ERR_CD_STG_TABLE="mrdw_stg_mcom_ctac_err_cd_ex"
IATA_TABLE_ROW_COUNT_LIST=$LOG_DIR/"iata_table_error_list.${PROCESS_DATE}.log"
ERR_CD_TABLE_ROW_COUNT_LIST=$LOG_DIR/"error_code_table_error_list.${PROCESS_DATE}.log"

MAIL_IDS='vidhya.vinayagam@marriott-sp.com'
MAIL_TEXT=$LOG_DIR/"${PROGRAM_NAME}.txt"

##############################################################################################
# STEP 1: Check whether or not the control file $CTL_FILE exist, create one
#         if it does not exist with a value of 1.
#
##############################################################################################

STATUS=0

if [ ! -f $CTL_FILE ]; then
  echo -e "\n Creating $CTL_FILE file"																| tee $LOG
  echo 1 > $CTL_FILE
fi

##############################################################################################
# STEP 2: 	Populate the parameter file $MRDW_PARAMFILES/CTAC_IATA/t_ctac_iata.par with
#			OWNER variable value, then execute the sqoop command to populate staging table 
#			MRDW_STG_MCOM_CTAC_IATA_EX
#
##############################################################################################

echo $LZETLHOME | awk -F"/" '{print $4}' | read owner_parm

if [ $owner_parm = 'dev' ] || [ $owner_parm = 'tst' ]; then
   OWNER=MMT
else
   OWNER=MMP
fi

#cd $MRDW_PARAMFILES/CTAC_IATA
#
#echo "[CTAC_IATA.WF:wf_MRDW_CTAC_IATA_REQUEST_LOAD]" 	> 	t_ctac_iata.par
#echo "\$\$OWNER=$OWNER" 								>> 	t_ctac_iata.par 

cd $CURR_DIR

if [ `cat $CTL_FILE` -eq 1 ]; then

	echo -e "\nStarting the SQOOP Command to import MMT_MP_MASTER"					| tee $LOG
export JAVA_OPTS=" $JAVA_OPTS -Djava.security.egd=file:///dev/urandom "
	
sqoop import \
--connect $ctac_jdbc_string \
--username $username \
--password $password \
--table MMT.MMT_MP_MASTER \
-m 1 \
--hive-import \
--hive-overwrite \
--hive-table $STG_SCHEMA.$IATA_STG_TABLE
	
	RESPONSE_CODE=$?
	
	if [ $RESPONSE_CODE -ne 0 ]; then
		RVAL=201
		echo -e "\nError executing sqoop import to ctac_err_cd stg table"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSTEP $RVAL: `date`                                                    	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nResponse from sqoop command:		                                     	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\n$RESPONSE_CODE                                                             	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nPlease check the log file \t$LOG \tunder \t$LOG_DIR directory			"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME failed" $MAIL_IDS
		rm -f $MAIL_TEXT
		exit $RVAL
	else
		echo -e "\n*************************************************************************" 		| tee -a $LOG
		echo -e "\n`date` successfully executed workflow sqoop command" 		| tee -a $LOG
		echo -e "\n*************************************************************************" 		| tee -a $LOG
		echo 2 > $CTL_FILE
	fi
fi

########################################################################################################
# STEP 3: Call daily_load_mrdw_dim_mcom_ctac_iata_ex.hql to populate target table MRDW_DIM_MCOM_CTAC_IATA_EX
#
########################################################################################################

if [ `cat $CTL_FILE` -eq 2 ]; then

	HQL_FILE_IATA=$LZSQL_TGT/daily_load_mrdw_dim_mcom_ctac_iata_ex.hql

	echo -e "\n Starting $HQL_FILE_IATA to populate target table MRDW_DIM_MCOM_IATA_CTAC_EX"  		| tee $LOG
	  
	# Calling the hive script to populate MRDW_DIM_MCOM_CTAC_IATA_EX_REPL and MRDW_DIM_MCOM_CTAC_IATA_EX
	RESPONSE=$(hive -v                            \
            -hiveconf STG_SCHEMA=$STG_SCHEMA     \
            -hiveconf TGT_SCHEMA=$TGT_SCHEMA     \
            -hiveconf ETL_SCHEMA=$ETL_SCHEMA     \
            -hiveconf ALIAS_SCHEMA=$ALIAS_SCHEMA \
            -f "$HQL_FILE_IATA" 2>&1) 
	RESPONSE_CODE=$?

	if [ $RESPONSE_CODE -ne 0 ];then
		RVAL=301
		echo -e "\nERROR executing $HQL_FILE_IATA											"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSTEP $RVAL: `date`                                                    	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nResponse from HIVE command:		                                     	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\n$RESPONSE                                                             	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nPlease check the log file \t$LOG \tunder \t$LOG_DIR directory			"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME failed" $MAIL_IDS
		rm -f $MAIL_TEXT
		exit $RVAL
	fi
	
	RESPONSE=$(sh fx_capture_table_output.sh "$RESPONSE" 2>&1)
	RESPONSE_CODE=$?
	
	if [ $RESPONSE_CODE -ne 0 ]; then
		RVAL=302
		echo -e "\nERROR CHECKING TABLE OUTPUT IN $PROGRAM        							"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSTEP $RVAL: `date`                                                    	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nResponse from Hive command:		                                     	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\n$RESPONSE                                                             	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nPlease check the log file \t$LOG \tunder \t$LOG_DIR directory			"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME failed" $MAIL_IDS
		rm -f $MAIL_TEXT
		exit $RVAL
	else
		# Write list of tables and row counts to $IATA_TABLE_ROW_COUNT_LIST to compare later
		printf "$RESPONSE" > $IATA_TABLE_ROW_COUNT_LIST
		if [ ! -s $IATA_TABLE_ROW_COUNT_LIST ]; then
		   # indicate that all unit tests passed
			echo -e "\n**********************************************************************	" 	| tee -a $LOG
			echo -e "\n`date` SCRIPT $HQL_FILE_IATA COMPLETED WITH NO ERRORS				" 	| tee -a $LOG
			echo -e "\nCTAC IATA table load is complete			                  				" 	| tee -a $LOG
			echo -e "\n**********************************************************************	" 	| tee -a $LOG
			echo 3 > $CTL_FILE		   
		else
		   # indicate that the unit tests revealed errors
			RVAL=303
			echo -e "\n SCRIPT $HQL_FILE_IATA ENCOUNTERD UNIT TEST ERRORS					"	| tee -a $LOG $MAIL_TEXT
			echo -e "\n**********************************************************************	" 	| tee -a $LOG $MAIL_TEXT
			echo -e "\nSTEP $RVAL: `date`                                                    	" 	| tee -a $LOG $MAIL_TEXT
			echo -e "\nCheck file $IATA_TABLE_ROW_COUNT_LIST for unit test errors           	" 	| tee -a $LOG $MAIL_TEXT
			echo -e "\n**********************************************************************	" 	| tee -a $LOG $MAIL_TEXT
			cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME FAILED" $MAIL_IDS
			rm -f $MAIL_TEXT
			exit $RVAL
		fi
	fi
fi

##############################################################################################
# STEP 4: 	Populate the parameter file $MRDW_PARAMFILES/CTAC_IATA/t_ctac_err_cd.par with
#			OWNER variable value, then execute the sqoop command to populate staging table 
#			MRDW_STG_MCOM_CTAC_ERROR_CODES_EX
##############################################################################################

#cd $MRDW_PARAMFILES/CTAC_IATA
#
#echo "[CTAC_IATA.WF:wf_MRDW_CTAC_ERROR_CODES_LOAD]" 			> 	t_ctac_err_cd.par
#echo "\$\$OWNER=$OWNER" 										>> 	t_ctac_err_cd.par 

cd $CURR_DIR

if [ `cat $CTL_FILE` -eq 3 ]; then

	echo -e "\n Starting the sqoop command"								| tee $LOG
 
sqoop import \
--connect $ctac_jdbc_string \
--username $username \
--password $password \
--table MMT.MMT_CTACODES_BATCH \
-m 1 \
--hive-import \
--hive-overwrite \
--hive-table $STG_SCHEMA.$ERR_CD_STG_TABLE

	RESPONSE_CODE=$?

	if [ $RESPONSE_CODE -ne 0 ];then
		RVAL=401
		echo -e "\nError executing Sqoop workflow                                       	"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSTEP $RVAL: `date`                                                    	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nResponse from Sqoop command:		                                     	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\n$RESPONSE                                                             	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nPlease check the log file \t$LOG \tunder \t$LOG_DIR directory			"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME FAILED" $MAIL_IDS
		rm -f $MAIL_TEXT
		exit $RVAL
	else
		echo -e "\n*************************************************************************" 		| tee -a $LOG
		echo -e "\n`date` Successfully executed Sqoop import for ctac_err_cd stage table		" 		| tee -a $LOG
		echo -e "\n*************************************************************************" 		| tee -a $LOG
		echo 4 > $CTL_FILE
	fi
fi

################################################################################
# STEP 5: Call daily_load_mrdw_dim_mcom_ctac_error_codes_ex.hql to populate target table MRDW_DIM_MCOM_CTAC_IATA_EX
#
################################################################################

if [ `cat $CTL_FILE` -eq 4 ]; then

	# export ENV_FILE="$NZ_ENV_STG"   -- may not need this, depending on the default Netezza env
	HQL_FILE_EC=$LZSQL_TGT/daily_load_mrdw_dim_mcom_ctac_error_codes_ex.hql

	echo -e "\n Starting $HQL_FILE_EC to populate target table MRDW_DIM_MCOM_CTAC_ERR_CD_EX" 		| tee -a $LOG

	# Calling the script to populate MRDW_DIM_MCOM_CTAC_IATA_EX_REPL and MRDW_DIM_MCOM_CTAC_IATA_EX
	RESPONSE=$(hive -v                            \
            -hiveconf STG_SCHEMA=$STG_SCHEMA     \
            -hiveconf TGT_SCHEMA=$TGT_SCHEMA     \
            -hiveconf ETL_SCHEMA=$ETL_SCHEMA     \
            -hiveconf ALIAS_SCHEMA=$ALIAS_SCHEMA \
            -f "$HQL_FILE_EC" 2>&1) 
	RESPONSE_CODE=$?

	if [ $RESPONSE_CODE -ne 0 ];then
		RVAL=501
		echo -e "\nError executing $HQL_FILE_EC												"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSTEP $RVAL: `date`                                                    	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nResponse from Hive command:		                                     	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\n$RESPONSE                                                             	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nPlease check the log file \t$LOG \tunder \t$LOG_DIR directory			"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME FAILED" $MAIL_IDS
		rm -f $MAIL_TEXT
		exit $RVAL
	fi

	RESPONSE=$(sh fx_capture_table_output.sh "$RESPONSE" 2>&1)
	RESPONSE_CODE=$?
	
	if [ $RESPONSE_CODE -ne 0 ]; then
		RVAL=502
		echo -e "\nERROR CHECKING TABLE OUTPUT IN $PROGRAM        							"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSTEP $RVAL: `date`                                                    	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nResponse from Shell command:		                                     	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\n$RESPONSE                                                             	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nPlease check the log file \t$LOG \tunder \t$LOG_DIR directory			"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME failed" $MAIL_IDS
		rm -f $MAIL_TEXT
		exit $RVAL
	else
		# Write list of tables and row counts to $ERR_CD_TABLE_ROW_COUNT_LIST to compare later
		printf "$RESPONSE" > $ERR_CD_TABLE_ROW_COUNT_LIST
		if [ ! -s $ERR_CD_TABLE_ROW_COUNT_LIST ]; then
		   # indicate that all unit tests passed
			echo -e "\n**********************************************************************	" 	| tee -a $LOG
			echo -e "\n`date` SCRIPT $HQL_FILE_EC COMPLETED WITH NO ERRORS					" 	| tee -a $LOG
			echo -e "\nCTAC ERROR CODE table load is complete	                  				" 	| tee -a $LOG
			echo -e "\n**********************************************************************	" 	| tee -a $LOG
			echo 5 > $CTL_FILE		   
		else
		   # indicate that the unit tests revealed errors
			RVAL=502
			echo -e "\nSCRIPT $HQL_FILE_EC ENCOUNTERD UNIT TEST ERRORS						"	| tee -a $LOG $MAIL_TEXT
			echo -e "\n**********************************************************************	" 	| tee -a $LOG $MAIL_TEXT
			echo -e "\nSTEP $RVAL: `date`                                                    	" 	| tee -a $LOG $MAIL_TEXT
			echo -e "\nCheck file $ERR_CD_TABLE_ROW_COUNT_LIST for unit test errors           	" 	| tee -a $LOG $MAIL_TEXT
			echo -e "\n**********************************************************************	" 	| tee -a $LOG $MAIL_TEXT
			cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME FAILED" $MAIL_IDS
			rm -f $MAIL_TEXT
			exit $RVAL
		fi
	fi
fi

# Delete raw data files
rm -f $IATA_TABLE_ROW_COUNT_LIST $ERR_CD_TABLE_ROW_COUNT_LIST

# Delete CTL file
rm -f $CTL_FILE

# exit the process with status as successful
exit $STATUS
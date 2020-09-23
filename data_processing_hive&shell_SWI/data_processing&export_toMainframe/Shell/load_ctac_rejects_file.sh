#!/bin/ksh
#########################################################################################################
#  File Name: load_ctac_rejects_file.sh
#
#  Description : 	This script executes an sftp command to retrieve CTAC error files from the mainframe and 
#					populate this data into Hadoop table. 
#
#                There are 5 major steps in the script 
#                1: Check whether or not the control file ctac_error_file_process_status.ctl exists, create 
#                   one if it does not exist with a value of 1 so that, the process always starts with 
#                   the first session unless previous run was not successful. 
#                2: Execute the SFTP procedure to bring in the error code file 
#                3: Execute LOAD Hadoop to populate staging table mrdw_stg_mcom_ctac_rejects, 
#					and verify table is populated
#                4: Call Hive script to populate target table MRDW_DIM_MCOM_CTAC_REJECTS_EX. 
#  Modification Log
#  Name            Date         Log
#  -------------   ---------    -----
#  Vidya    12/04/2017
##########################################################################################################
set -o xtrace
PROGRAM_NAME=`basename $0|cut -d"." -f1`
PROCESS_DATE=`date '+%Y%m%d%H%M'`

#BE CERTAIN TO CREATE THIS DIRECTORY BEFORE RUNNING
SQL_DIR=$NZ_SQL/CTAC_Atrb/load_tgt
LOG=$LOG_DIR/"${PROGRAM_NAME}.${PROCESS_DATE}.log"
CMD_FILE="${PROGRAM_NAME}.${PROCESS_DATE}.ftp"
CTL_FILE="${PROGRAM_NAME}.ctl"
FROM_FILE="//MMT.CT.MRDW.REJECTED\(0\)"
SFTP_CTAC_USER=MRDWFTP
SFTP_HOST=159.166.51.35

STG_TABLE_NAME="mrdw_stg_mcom_ctac_rejects_ex"
INPUT_FILE_NAME="reject.ctac01"
OUTPUT_FILE_NAME="reject_file_ascii.txt"
MAIL_TEXT=$LOG_DIR/"${PROGRAM_NAME}.txt"

##############################################################################################
# STEP 1: 	Check whether or not the control file ctac_error_file_process_status.ctl exists, 
#			create one if it does not exist with a value of 1.
#
##############################################################################################

STATUS=0

if [ ! -f $CTL_FILE ]; then
  
	echo -e "\n Creating $CTL_FILE file"           													| tee $LOG
	echo 1 > $CTL_FILE
fi

##############################################################################################
# STEP 2: Execute the SFTP procedure to bring in the error code file
#
##############################################################################################

echo $MRDW_ENV | awk -F"/" '{print $4}' | read owner_parm

# Determine the correct owner (MMT for test, MMP for production) to use in the filename
if [ $owner_parm = 'dev' ] || [ $owner_parm = 'tst' ]; then
   OWNER=MMT
else
   OWNER=MMP
fi


if [ `cat $CTL_FILE` -eq 1 ]; then

	echo -e "\n Starting SFTP procedure"															| tee $LOG
 
	echo get //$OWNER.CT.MRDW.REJECTED\(0\) $INPUT_FILE_NAME > $CMD_FILE
	sftp -o ConnectTimeout=60 -o ServerAliveInterval=60  -o port=2222 -o StrictHostKeyChecking=no -b $CMD_FILE $SFTP_CTAC_USER@$SFTP_HOST

	SFTP_STATUS=$?

	if [ $SFTP_STATUS -ne 0 ]; then
		RVAL=201
		echo -e "\nERROR sftping the $CMD_FILE					          					"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************"		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSTEP $RVAL: `date`                                                    	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSFTP_STATUS: $SFTP_STATUS	                 							"		| tee -a $LOG $MAIL_TEXT
		echo -e "\nPlease check the log file \t$LOG \tunder \t$LOG_DIR directory			"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************"		| tee -a $LOG $MAIL_TEXT
		cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME FAILED " $MAILIDS
		rm -f $MAIL_TEXT
		exit $RVAL
	else
		echo -e "\n*************************************************************************" 		| tee -a $LOG
		echo -e "\n`date` SUCCESSFULLY executed $CMD_FILE			                 	  	" 		| tee -a $LOG
		echo -e "\n*************************************************************************" 		| tee -a $LOG

		# Convert EBCDIC format to ASCII
		dd conv=ascii if=$INPUT_FILE_NAME of=reject_temp0.txt
		# File is one continuous line, so enter newline characters after the 171 char fixed line width
		sed -e "s/.\{171\}/&\n/g" <reject_temp0.txt > reject_temp1.txt
		# Insert delimiters between each data value so hive will work
		awk -v FIELDWIDTHS='3 10 10 5 8 8 5 8 8 3 10 8 5 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4 4' -v OFS='|' '{ $1=$1 ""; print }' <reject_temp1.txt > reject_temp2.txt
		# Finally, strip out the whitespace
		sed -e 's/[ \t]*//g' <reject_temp2.txt > $OUTPUT_FILE_NAME
		
		echo 2 > $CTL_FILE
	fi
fi

################################################################################
# STEP 3 - 	Execute hive to populate staging table mrdw_stg_mcom_ctac_rejects, 
#			and verify table is populated
#
################################################################################

if [ `cat $CTL_FILE` -eq 2 ]; then

	echo -e "\n Starting hive command to populate staging table $STG_TABLE_NAME"  				| tee $LOG

	# Truncate table 
	RESPONSE=$(hive -v                            \
            -hiveconf STG_SCHEMA=$STG_SCHEMA     \
            -hiveconf TGT_SCHEMA=$TGT_SCHEMA     \
            -hiveconf ETL_SCHEMA=$ETL_SCHEMA     \
            -hiveconf ALIAS_SCHEMA=$ALIAS_SCHEMA \
            -e "TRUNCATE TABLE $STG_SCHEMA.$STG_TABLE_NAME;" 2>&1) 
	RESPONSE_CODE=$?
	
	if [ $RESPONSE_CODE -ne 0 ]; then
		RVAL=301
		echo -e "\nERROR truncating table $STG_TABLE_NAME									"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSTEP $RVAL: `date`                                                    	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nResponse from hive command:		                                     	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\n$RESPONSE                                                             	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nPlease check the log file \t$LOG \tunder \t$LOG_DIR directory			"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME FAILED" $MAIL_IDS
		rm -f $MAIL_TEXT
		exit $RVAL
	else
		echo -e "\n*************************************************************************" 		| tee -a $LOG
		echo -e "\n`date` SUCCESSFULLY truncated table $STG_TABLE_NAME                 	  	" 		| tee -a $LOG
		echo -e "\n*************************************************************************" 		| tee -a $LOG
	fi	
	
	# Move local file to HDFS
	hadoop fs -put $LZSCRIPT $SRCFILES_PATH_HDFS
	
	# Load table
	/usr/ibmpacks/common-utils/current/jsqsh/bin/jsqsh bigsql -U $BIGSQL_USER -P $BIGSQL_PASSWORD  -i ${LZSCRIPT}/$load_ctac_rejects_table.sql
	
	RESPONSE_CODE=$?
	
	if [ $RESPONSE_CODE -eq 2 ]; then 
		RVAL=302
		echo -e "\nERROR loading table $STG_TABLE_NAME										"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************"		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSTEP $RVAL: `date`                                                    	"		| tee -a $LOG $MAIL_TEXT
		echo -e "\nResponse from hive command:	                                     	"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n$RESPONSE                                                             	"		| tee -a $LOG $MAIL_TEXT
		echo -e "\nPlease check the log file \t$LOG \tunder \t$LOG_DIR directory			"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************"		| tee -a $LOG $MAIL_TEXT
		cat $MAIL_TEXT | mailx -s "Script ${PROGRAM}.sh Failed" $MAIL_IDS
		rm -f $MAIL_TEXT
		exit $RVAL
	else
		echo -e "\n*************************************************************************" 		| tee -a $LOG
		echo -e "\n`date` SUCCESSFULLY loaded table $STG_TABLE_NAME	                 	  	" 		| tee -a $LOG
		echo -e "\n*************************************************************************" 		| tee -a $LOG
		echo 3 > $CTL_FILE
	fi
fi
  
##############################################################################################
# STEP 4: Insert REJECT records into LZ
#
##############################################################################################

##. ${NZ_ENV_TGT}

if [ `cat $CTL_FILE` -eq 3 ]; then
  
	TGT_HQL_FILE=$LZSQL_TGT/daily_load_mrdw_dim_mcom_ctac_rejects_ex.hql

	echo -e "\n Starting $TGT_HQL_FILE to populate target table MRDW_DIM_MCOM_REJECTS_CTAC_EX"  	| tee -a $LOG
  
	# Calling the SQL script to populate MRDW_DIM_MCOM_CTAC_REJECTS_EX_REPL and MRDW_DIM_MCOM_CTAC_REJECTS_EX
	RESPONSE=$(hive -v                            \
            -hiveconf STG_SCHEMA=$STG_SCHEMA     \
            -hiveconf TGT_SCHEMA=$TGT_SCHEMA     \
            -hiveconf ETL_SCHEMA=$ETL_SCHEMA     \
            -hiveconf ALIAS_SCHEMA=$ALIAS_SCHEMA \
            -f $TGT_HQL_FILE 2>&1) 
	RESPONSE_CODE=$?

	if [ $RESPONSE_CODE -ne 0 ];then
		RVAL=401
		echo -e "\nERROR executing $TGT_HQL_FILE"     												| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nSTEP $RVAL: `date`                                                    	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nResponse from hive command:		                                     	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\n$RESPONSE                                                             	" 		| tee -a $LOG $MAIL_TEXT
		echo -e "\nPlease check the log file \t$LOG \tunder \t$LOG_DIR directory			"		| tee -a $LOG $MAIL_TEXT
		echo -e "\n*************************************************************************" 		| tee -a $LOG $MAIL_TEXT
		cat $MAIL_TEXT | mailx -s "The script $PROGRAM_NAME FAILED" $MAIL_IDS
		rm -f $MAIL_TEXT
		exit $RVAL
	else
		echo -e "\n*************************************************************************" 		| tee -a $LOG
		echo -e "\n`date` SUCCESSFULLY executed $TGT_HQL_FILE		                 	  	" 		| tee -a $LOG
		echo -e "\n*************************************************************************" 		| tee -a $LOG
		echo 4 > $CTL_FILE
	fi
fi

# Delete data files
rm -f $INPUT_FILE_NAME
rm -f $OUTPUT_FILE_NAME
rm -f reject_temp*.txt
rm -f $CMD_FILE


# Delete CTL file
rm -f $CTL_FILE

# exit the process with status as successful
exit $STATUS
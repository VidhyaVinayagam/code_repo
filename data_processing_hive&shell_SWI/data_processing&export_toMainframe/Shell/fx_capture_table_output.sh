#!/bin/ksh
###################################################################################################
# Script Name: fx_capture_table_output.sh
#
# Description:  Used to capture output from a sql command wrap sql statement as follows.
#               select 'CAPTURE_TABLE_BEGIN'
#               select 1234
#               select 'CAPTURE_TABLE_END'
#               select 'CAPTURE_TABLE_BEGIN'
#               select 5678
#               select 5678
#               select 'CAPTURE_TABLE_END'
#               Response will return all captured matches
# Steps      :  Check that the input parameters are set.
# Modification LOG:
# Name             Date        Modification
# -----------      ----------  ---------------------------
# Vidya          04/02/2013  Initial version
###################################################################################################

trap 'ERR_LINE=$ERR_LINE"ERROR AT LINE $LINENO\n"' ERR
set -o pipefail

STATUS=0
PROGRAM=`basename $0 | cut -d"." -f1`
###################################################################################################
# Step 1 - test input parameters
###################################################################################################
unset FLAG
[[ -z "$INPUT_TO_PARSE" && ! -z $1 ]] && INPUT_TO_PARSE=$1
[ -z "$INPUT_TO_PARSE" ] && FLAG=$FLAG"\$INPUT_TO_PARSE or \$1: cannot be empty\n"
if [ ! -z $FLAG ]; then
   printf "\n***************************************************************************************************"
   printf "\n Cannot complete the process.  The calling parameters are not set."
   printf "\n Address any issues with the way this script is being called."
   printf "\nset the following parameters"
   printf "\n $FLAG"
   printf "\n***************************************************************************************************"
   exit 101
fi

RESPONSE=$(awk '/^CAPTURE_TABLE_BEGIN$/,/^CAPTURE_TABLE_END$/{print}' <<< $INPUT_TO_PARSE \
         | sed  -r '/^CAPTURE_TABLE_(BEGIN|END)/d' 2>&1)
RESPONSE_CODE=$?
if [ 0 -ne $RESPONSE_CODE ]; then
   printf "\n***************************************************************************************************"
   printf "\nthere was an issue with the capture table command.  Please review the output."
   printf "\n$RESPONSE\n"
   printf "\n$ERR_LINE"
   printf "\n***************************************************************************************************"
   exit 102
fi
printf "$RESPONSE"

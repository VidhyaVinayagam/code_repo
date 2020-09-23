#!/bin/bash

export job_nm=$1
#D
export odate=$2
export src_dir=$3
#/bidhdev/data/CSA/dev/shr/inbound/E09/ipnd/src/land
export nms_dir=$4
#/bidhdev/data/CSA/dev/shr/inbound/E09/nms/src/land
export file=$5
export srcbasedir=$6
#/bidhdev/data/CSA/dev/shr/inbound/E09/ipnd
export srcarchdir=$7
#/bidhdev/data/CSA/dev/shr/inbound/E09/ipnd/arch/
export nmsarchdir=$8
#/bidhdev/data/CSA/dev/shr/inbound/E09/nms/arch

export preprocessscript=ADWIpndPreProcessingFileIntegrity_FileArchiving.sh
# Assign Exit Codes
export EXIT_FAILURE=1;

export preprocessdir=$srcbasedir/src/preprocess

export processsttime=`date +"%H%M%S"`
echo "Process Start Time:"$processsttime

#Check all Parameters are Passed to the script
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
echo "####################Check all Parameters are Passed to the script##################" >&2
echo "===================================================================================================" >&2

  if [[ -n $JOB_NM ]]
    then
      echo "File Pattern Passed to the Script: $JOB_NM" >&2
    else
      echo "FAILURE: File Pattern Not Passed to Script" >&2
      exit $EXIT_FAILURE
    fi

  if [[ -n $ODATE ]]
    then
      echo "Run Date Passed to the Script: $ODATE" >&2
    else
      echo "FAILURE: Run Date Not Passed to Script" >&2
      exit $EXIT_FAILURE
    fi

  if [[ -n $SRC_DIR ]]
    then
      echo "Source land Directory Passed to the Script: $SRC_DIR" >&2
    else
      echo "FAILURE: Source land Directory Not Passed to Script" >&2
      exit $EXIT_FAILURE
    fi

  if [[ -n $fl_pttrn_without_csv ]]
    then
      echo "fl_pttrn_without_csv Passed to the Script: $fl_pttrn_without_csv" >&2
    else
      echo "FAILURE: fl_pttrn_without_csv Not Passed to Script" >&2
      exit $EXIT_FAILURE
    fi

  if [[ -n $srcbasedir ]]
    then
      echo "srcbasedir Passed to the Script: $srcbasedir" >&2
    else
      echo "FAILURE: srcbasedir Not Passed to Script" >&2
      exit $EXIT_FAILURE
    fi

# Validate if .profile file exists
  if [ ! -f ~/.profile ]
  then
    echo ".profile file does not found in home directory "
    exit $EXIT_FAILURE
  fi

# Execute .profile to export variables used in this script PRIVATEKEYFILENAME,CSAREMOTEHOSTNAME,CSAREMOTEHOSTUSERNAME
  . ~/.profile

  if [[ -e $SCRIPTPATH/$preprocessscript ]]
    then
      echo "Pre-Process Script Exists in the directory: $preprocessscript" >&2
    else
      echo "FAILURE: Pre-Process Script Exists in the directory" >&2
      exit $EXIT_FAILURE
    fi


# Test connection to Server 3 times
((count = 3))
while [[ $count -ne 0 ]] ; do
    # Test connection to Server
    ssh -q -o stricthostkeychecking=no -i ~/.ssh/$PRIVATEKEYFILENAME $CSAREMOTEHOSTUSERNAME@$CSAREMOTEHOSTNAME exit
    export rc=0  # Remove this once Rudresh fixes issues on remote server. Issue : After successful SSH echo $? returns 1
    rc=$?
    # Test Return code of Connection Status
         if [[ $rc -eq 0 ]] ; then
                echo "CONNECTION TO CSA SERVER SUCCESS"
                # Copy the Pre-process script from jof server to CSA server before execution
echo "put $SCRIPTPATH/$preprocessscript $preprocessdir"  | sftp -o stricthostkeychecking=no -q -b - -oIdentityFile=~/.ssh/$PRIVATEKEYFILENAME $CSAREMOTEHOSTUSERNAME@$CSAREMOTEHOSTNAME
                if [ "$?" -ne "0" ] ; then
                        echo "Error: Copy the Pre-process script from jof server to CSA server"
                        exit $EXIT_FAILURE
                else
                        echo "COPY PRE-PROCESS SCRIPT TO CSA SERVER COMPLETED"
                fi

                # Execute Pre-process script in CSA server
                echo "START EXECUTING THE PRE-PROCESS SCRIPT IN CSA SERVER...."
                ssh -q -o stricthostkeychecking=no -i ~/.ssh/$PRIVATEKEYFILENAME $CSAREMOTEHOSTUSERNAME@$CSAREMOTEHOSTNAME "sh $preprocessdir/$preprocessscript $JOB_NM $ODATE $SRC_DIR $fl_pttrn_without_csv $srcbasedir"
                if [ "$?" -ne "0" ] ; then
                        echo "Error: In Execution of Pre-process script in CSA server"
                        exit $EXIT_FAILURE
                else
                        echo "EXECUTION OF PRE-PROCESS SCRIPT IN CSA SERVER COMPLETED"
                fi

                # Delete the Pre-process script from jof server to CSA server after execution
                ssh -q -o stricthostkeychecking=no -i ~/.ssh/$PRIVATEKEYFILENAME $CSAREMOTEHOSTUSERNAME@$CSAREMOTEHOSTNAME "rm -f $preprocessdir/$preprocessscript " exit
                if [ "$?" -ne "0" ] ; then
                        echo "Error: In Execution of removing pre-process script in CSA server"
                        exit $EXIT_FAILURE
                else
                        echo "REMOVAL PRE-PROCESS SCRIPT IN CSA SERVER COMPLETED"
                fi

         ((count = 1))
         fi
     ((count = count - 1))
     sleep 5  #revisit to increase time
done

# Print a message if we failed
if [[ $rc -ne 0 ]] ; then
    echo "Could not connect to Server after 3 attempts - stopping."
    exit $EXIT_FAILURE
fi

export processettime=`date +"%H%M%S"`
echo "Process End Time:"$processettime


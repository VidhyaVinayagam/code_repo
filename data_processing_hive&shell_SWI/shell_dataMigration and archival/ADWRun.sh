#!/bin/bash

export EXIT_FAILURE=1;

scriptname=`basename $0`;
usagepttrn="Usage : ${scriptname} -m 'modification time for purgeing' -n 'Ent path' -o 'Shr path' -p 'Source System' -q 'modification time for zipping'"

while getopts m:n:o:p:q: opt 
do
case "$opt" in
m) modtimepurge=$OPTARG;;
n) entpath=$OPTARG;;
o) shrpath=$OPTARG;;
p) srcsystem=$OPTARG;;
q) modtimezip=$OPTARG;;
?) echo "${scriptname} : $OPTARG is an invalid option!"
echo $usagepttrn
exit $EXIT_FAILURE;
esac
done

export csascript=ADWRunCSA_File_Zip_Purge.sh


#Check all Parameters are Passed to the script
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
echo "####################Check all Parameters are Passed to the script##################" >&2
echo "===================================================================================================" >&2
#check all parameters are passed to the script#
#---------------------------------------------#
echo "####################Check all Parameters are Passed to the script##################" >&2
echo "===================================================================================================" >&2
	if [[ -n $modtimepurge ]]
	then
			echo "Retention Period is passed from Control-M to the Script:" $modtimepurge >&2
	else
			echo "FAILURE: Retention Period not passed from Control-M" >&2
			exit $EXIT_FAILURE
	fi

	if [[ -n $entpath ]]
	then
			echo "Enterprise Path is passed from Control-M to the Script:" $entpath >&2
	else
			echo "FAILURE: Enterprise Path not passed from Control-M" >&2
			exit $EXIT_FAILURE
	fi

	if [[ -n $shrpath ]]
	then
			echo "Shared Path is passed from Control-M to the Script:" $shrpath >&2
	else
			echo "FAILURE: Shared Path not passed from Control-M" >&2
			exit $EXIT_FAILURE
	fi

	if [[ -n $srcsystem ]]
	then
			echo "Source System is passed from Control-M to the Script:" $srcsystem >&2
	else
			echo "FAILURE: Source System not passed from Control-M" >&2
			exit $EXIT_FAILURE
	fi

	if [[ -n $modtimezip ]]
	then
			echo "Compression File time is passed from Control-M to the Script:" $modtimezip >&2
	else
			echo "FAILURE: Compression File time not passed from Control-M" >&2
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

  if [[ -e $SCRIPTPATH/$csascript ]]
    then
      echo "CSA Compression and Archival Purge Script Exists in the directory: $csascript" >&2
    else
      echo "FAILURE: CSA Compression and Archival Purge Script Exists in the directory" >&2
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
	        # Copy the CSA Purge script from jof server to CSA server before execution
	        echo "put $SCRIPTPATH/$csascript $entpath/$srcsystem"  | sftp -o stricthostkeychecking=no -q -b - -oIdentityFile=~/.ssh/$PRIVATEKEYFILENAME $CSAREMOTEHOSTUSERNAME@$CSAREMOTEHOSTNAME  	
	        if [ "$?" -ne "0" ] ; then
        		echo "Error: Copy the CSA Compression and Archival Purge script from jof server to CSA server"
		        exit $EXIT_FAILURE
		else
			echo "COPY CSA Compression and Archival Purge SCRIPT TO CSA SERVER COMPLETED"
	        fi

	        # Execute CSA Compression and Archival Purge script in CSA server
	        echo "START EXECUTING THE CSA Compression and Archival Purge SCRIPT IN CSA SERVER...." 
  		ssh -q -o stricthostkeychecking=no -i ~/.ssh/$PRIVATEKEYFILENAME $CSAREMOTEHOSTUSERNAME@$CSAREMOTEHOSTNAME "sh $entpath/$srcsystem/$csascript -m '$modtimepurge' -n '$entpath' -o '$shrpath' -p '$srcsystem' -q '$modtimezip'"
                if [ "$?" -ne "0" ] ; then
                        echo "Error: In Execution of CSA Compression and Archival Purge script in CSA server"
                        exit $EXIT_FAILURE
                else
                        echo "EXECUTION OF CSA compression and Archival Purge SCRIPT IN CSA SERVER COMPLETED"
                fi
 			
                # Delete the CSA Compression and Archival Purge script from jof server to CSA server after execution
		ssh -q -o stricthostkeychecking=no -i ~/.ssh/$PRIVATEKEYFILENAME $CSAREMOTEHOSTUSERNAME@$CSAREMOTEHOSTNAME "rm -f $entpath/$csascript " exit
                if [ "$?" -ne "0" ] ; then
                        echo "Error: In Execution of removing CSA Compression and Archival Purge script in CSA server"
                        exit $EXIT_FAILURE
                else
                        echo "REMOVAL CSA Compression and Archival Purge SCRIPT IN CSA SERVER COMPLETED"
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



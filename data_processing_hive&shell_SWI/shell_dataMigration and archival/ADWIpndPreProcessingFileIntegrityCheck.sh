#!/bin/bash
export JOB_NM=$1
export ODATE=$2
export SRC_DIR=$3
export fl_pttrn_without_csv=$4
export srcbasedir=$5

# Assign Exit Codes

export EXIT_FAILURE=1;

#Check all Parameters are Passed to the script 
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
echo "####################Check all Parameters are Passed to the script##################" >&2
echo "===================================================================================================" >&2

 if [[ -n $srcbasedir ]]
    then
      echo "Source System Base Directory Passed to the Script: $srcbasedir" >&2
    else
      echo "FAILURE: Source System Base Directory Not Passed to Script" >&2
      exit $EXIT_FAILURE
    fi
	
if [[ -n $fl_pttrn_without_csv ]]
    then
      echo "File Pattern Passed to the Script: $fl_pttrn_without_csv" >&2
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
	
  if [[ -d $SRC_DIR ]]
    then
      echo "Land Directory Exists: $SRC_DIR" >&2
    else
      echo "FAILURE: Land Directory Does not Exists" >&2
      exit $EXIT_FAILURE
    fi



cd $SRC_DIR

ls $fl_pttrn_without_csv > FileListToCheckFilePresence.txt
FileCount=`cat $SRC_DIR/FileListToCheckFilePresence.txt | wc -l`

  if [[ "$FileCount" -eq "0" ]]
    then
        echo "No files found for integrity check" >&2
        exit $EXIT_FAILURE
	else
		issues=""
		trailer_not_found=" Trailer not found "
		header_not_found=" Header not found "
		record_not_same=" Record Count is not same "
		huge_record=" Record count is greater than 100,000 "
		trailer_value=`tail -1 $fl_pttrn_without_csv | grep "TRL"`
		header_value=`head -1  $fl_pttrn_without_csv | grep "HDR"`
		
		length=`wc -l < $fl_pttrn_without_csv | awk '{print $1}'`
		count=`tail -1  $fl_pttrn_without_csv | cut -c 25-32 | awk '{sub(/^0*/,"");}1'`
		recordcount=`expr $length - 2`

		
		if [[ $trailer_value == "" ]]; then
			echo $issues$trailer_not_found
			issues=$issues$trailer_not_found
		else
			echo "Trailer found"
			if [ $recordcount == $count ]; then
				echo "Record Count is same"
				if [[ $recordcount -gt 1000 ]]; then
					echo "Huge Records"
					issues=$issues$huge_record
				fi
			else	
					issues=$issues$record_not_same
			fi
		fi


		if [[ $header_value == "" ]]; then
			issues=$issues$header_not_found
		else
			echo "Header found"
		fi
		
	if [[ $issues == "" ]]; then
		echo "No issues"
	else
		echo "Issues $issues" | mailx -s "Alert!!! Job Name = $JOB_NM || ODate = $ODATE || Issues in File $issues" sangam.wadhwa@team.telstra.com
	fi
  fi
  
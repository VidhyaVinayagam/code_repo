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


# Assign Exit Codes
export EXIT_FAILURE=1;

echo "####################Check if all the required Parameters are Passed to the script##################" >&2
echo "===================================================================================================" >&2

 if [[ -n $srcbasedir ]]
    then
      echo "Source System Base Directory Passed to the Script: $srcbasedir" >&2
    else
      echo "FAILURE: Source System Base Directory Not Passed to Script" >&2
      exit $exit_failure
    fi
	
if [[ -n $file ]]
    then
      echo "File Pattern Passed to the Script: $file" >&2
    else
      echo "FAILURE: File Pattern Not Passed to Script" >&2
      exit $exit_failure
    fi

  if [[ -n $odate ]]
    then
      echo -e "Run Date Passed to the Script: $odate \n" >&2
    else
      echo -e "FAILURE: Run Date Not Passed to Script \n" >&2
      exit $exit_failure
    fi

echo "####################Check if all the required directories passed to the script exist##################" >&2
echo "===================================================================================================" >&2
	
  if [[ -d $src_dir ]] 
    then
      echo "Land Directory Exists: $src_dir" >&2
    else
      echo "FAILURE: Land Directory Does not Exists" >&2
      exit $exit_failure
    fi

  if [[ -d $srcarchdir ]] 
    then
      echo "Actual Archival directory Exists: $srcarchdir" >&2
    else
      echo "FAILURE: Actual Archival directory Does not Exists" >&2
      exit $exit_failure
    fi
	
	  if [[ -d $nms_dir ]] 
    then
      echo "NMS land directory exists: $nms_dir" >&2
    else
      echo "FAILURE: NMS land directory Does not Exists" >&2
      exit $exit_failure
    fi
	
	  if [[ -d $nmsarchdir ]] 
    then
      echo -e "NMS Archival directory Exists: $nmsarchdir \n" >&2
    else
      echo -e "FAILURE: NMS Archival directory Does not Exists \n" >&2
      exit $exit_failure
    fi


# Decode directory based on order date
odate_yr=`expr $odate | cut -c 1-4`
odate_mn=`expr $odate | cut -c 5-6`
odate_d=`expr $odate | cut -c 7-8`
odate_dir=$odate_yr/$odate_mn/$odate_d

if [[ $file == *IPNDUP* ]]; then
echo -e "Given File is IPND Daily file , to be verified and custom archived under IPND \n"
cd $src_dir
ls $file > ipnd_daily_files_integrity_check.txt
ls $file > files_custom_archival.txt
else 
if [[ $file == NMS*error* ]]; then 
echo -e "Given File is NMS Error file , to be custom archived under NMS archival \n"
cd $src_dir
ls $nms_dir/$file > files_custom_archival.txt
fi
fi

echo -e "\n ####################Check if the input file required to be validated ##############################" >&2
echo "===================================================================================================" >&2
		
FileCount_for_Integrity_check=`cat $src_dir/ipnd_daily_files_integrity_check.txt | wc -l`

  if [[ "$FileCount_for_Integrity_check" -eq "0" ]]
    then
        echo "No files found for integrity check" >&2
	else
		echo "The incoming file $file needs to be verified"
		issues=""
		trailer_not_found=" Trailer record is not found in the given file"
		header_not_found=" Header record is not found in the given file"
		record_not_same=" Record Count is not same as mentioned in the Trailer "
		huge_record=" Record count is greater than 1000 "
		
		trailer_value=`tail -1 $file | grep "TRL"`
		header_value=`head -1  $file | grep "HDR"`
		
		length=`wc -l < $file | awk '{print $1}'`
		count=`tail -1  $file | cut -c 25-32 | awk '{sub(/^0*/,"");}1'`
		recordcount=`expr $length - 2`

		
		if [[ $trailer_value == "" ]]; then
			echo $issues$trailer_not_found
			issues=$issues$trailer_not_found
		else
			echo "Trailer record is found in the given file"
			if [ $recordcount == $count ]; then
				echo "Record Count is same as in the Trailer"
				if [[ $recordcount -gt 1000000 ]]; then
					echo "Record count is greater than 1000000"
					issues=$issues$huge_record
				fi
			else	
					issues=$issues$record_not_same
			fi
		fi


		if [[ $header_value == "" ]]; then
			issues=$issues$header_not_found
		else
			echo -e "Header record is found in the given file \n"
		fi
		
	if [[ $issues == "" ]]; then
		echo -e "No issues \n "
	else
		echo "Issues $issues" | mailx -s "Alert!!! Job Name = $JOB_NM || ODate = $ODATE || Issues in File $issues" sangam.wadhwa@team.telstra.com
	fi
	rm -f $src_dir/ipnd_daily_files_integrity_check.txt
  fi

echo -e "\n ####################Check if the input file need to be Archived ###################################" >&2
echo "===================================================================================================" >&2

FileCount_for_archival=`cat $src_dir/files_custom_archival.txt | wc -l`
 
 if [[ "$FileCount_for_archival" -eq "0" ]]
    then
        echo "No files found for Archival" >&2
        exit $EXIT_FAILURE
	else
		echo -e "File found for Archival: $file \n"  >&2
		odate_yr=`expr $odate | cut -c 1-4`
		odate_mn=`expr $odate | cut -c 5-6`
		odate_d=`expr $odate | cut -c 7-8`
		nms="nms/"
		
		export landed_file=$src_dir/$file odate_dir=$odate_yr/$odate_mn/$odate_d ipndarch=$srcarchdir/$odate_dir  ipndnmsarch=$nmsarchdir/$odate_dir
		
#-- moving IPND daily files	from landing directory to Archival directory
if [[ $file  == *IPNDUP* ]]; then
	echo "Incoming file is IPND daily file and needs to be archived"
	if [[ -d $ipndarch ]]; then
		 echo -e "The required archival directory exists \n"
		 mv $landed_file $ipndarch
		 echo -e "File ${landed_file} has been archived to ${ipndarch} \n"
	  else
		 echo -e "The required archival directory doesnt exist, so creating one \n"
		 mkdir -p $ipndarch
		 mv $landed_file $ipndarch
		 echo -e "File ${landed_file} has been  archived to ${ipndarch} \n"
	fi       

#-- moving nms_error file from landing directory to Archival directory	

elif [[ $file  == NMS*err* ]]; then
	echo "Incoming file is NMS Error file and needs to be archived"
	if [[ -d $ipndnmsarch ]]; then
		echo -e "The required NMS archival directory exists \n"
		mv $nms_dir/$file $ipndnmsarch
		echo -e "File $nms_dir/$file has been archived to ${ipndnmsarch} \n"
	else 
		echo -e "The required archival directory doesnt exist, so creating one \n"
		mkdir -p $ipndnmsarch
		mv $nms_dir/$file $ipndnmsarch
		echo -e "File $nms_dir/$file has been archived to ${ipndnmsarch} \n"
	fi
fi
	rm -f $src_dir/files_custom_archival.txt
fi

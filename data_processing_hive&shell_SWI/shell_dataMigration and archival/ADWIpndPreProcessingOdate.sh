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

ls $fl_pttrn_without_csv > FileListToAppendExtension.txt
FileCount=`cat $SRC_DIR/FileListToAppendExtension.txt | wc -l`

  if [[ "$FileCount" -eq "0" ]]
    then
        echo "No files found for appending extension" >&2
        exit $EXIT_FAILURE
  fi


for filename in `cat $SRC_DIR/FileListToAppendExtension.txt`; do
		#dos2unix $SRC_DIR/src/land/$filename
		extension=${filename##*.}
		name=${filename%.*}
		underscore="_"

case "$extension" in
   "txt") 
   mv $SRC_DIR/$filename $SRC_DIR/$name$underscore$ODATE.$extension 
   ;;
   "TXT") 
   mv $SRC_DIR/$filename $SRC_DIR/$name$underscore$ODATE.$extension
   ;;
   "dat") 
   mv $SRC_DIR/$filename $SRC_DIR/$name$underscore$ODATE.$extension
   ;;
   "DAT") 
   mv $SRC_DIR/$filename $SRC_DIR/$name$underscore$ODATE.$extension
   ;;
   "csv")
   mv $SRC_DIR/$filename $SRC_DIR/$name$underscore$ODATE.$extension
   ;;
   "CSV")
   mv $SRC_DIR/$filename $SRC_DIR/$name$underscore$ODATE.$extension
   ;;
    *)
   mv $SRC_DIR/$filename $SRC_DIR/$filename.$ODATE
   ;;
esac
		#val=`ls $SRC_DIR/$filename.$ODATE | wc -l`
		
		#if [[ "$val" -ne "1" ]]
		#then
		#	echo "Issue while generating the modified file $filename.$ODATE"
		#fi
done

rm -f $SRC_DIR/FileListToAppendExtension.txt
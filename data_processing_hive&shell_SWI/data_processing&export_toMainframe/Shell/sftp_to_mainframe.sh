#!/bin/bash
#########################################################################################################
#  File Name: sftp_to_mainframe.sh
#
#  Description :    This script executes an sftp command to place a file with the latest row of CTAC data
#                   from MRDW_FACT_MCOM_ATRB_CTAC_DAILY_EX_REPL onto the mainframe.
#  Modification Log
#  Name            Date         Log
#  -------------   ---------    ----------------------------------------------------------------------
#  Vidya     10/03/2017    Created
##########################################################################################################

set -o xtrace
FROM_FILE=$1
TO_FILE=//!$2
SOURCE_HOST=159.166.51.35
SOURCE_USER=MRDWFTP
ssh_opts="$ssh_opts -o ConnectTimeout=60"
ssh_opts="$ssh_opts -o ServerAliveInterval=60"
ssh_opts="$ssh_opts -o port=22"
ssh_opts="$ssh_opts -o StrictHostKeyChecking=no"
ssh_opts="$ssh_opts -o identityfile=~/.ssh/id_rsa"

# space in cyl. you need about 1.25 times the amount of cyl than you do size of the file
# cyl.200.20 is good for a file about 162 MB
# cyl.20.20 is good for a file about 16MB
# to get the size of the file you need to get the row size in bytes
# multiply it by it's rows to get a file size.
# use that number to get the cyl size.

sftp $ssh_opts -b- ${SOURCE_USER}@${SOURCE_HOST}<< EOB
ls /+mode=text,lrecl=91,recfm=FB,blksize=0,space=cyl.20.20
put  ${FROM_FILE} ${TO_FILE}
quit
EOB
if [[ $? -gt 0 ]]
then
                echo "failed to transfer ${FROM_FILE}..."
                exit 1
fi

exit 0
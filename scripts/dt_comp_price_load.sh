#!/sbin/sh
###########################################################################
#                                                                        #
#  THIS SOURCE CODE IS UNPUBLISHED, AND IS THE EXCLUSIVE PROPERTY OF NWC  #
#                                                                        #
###########################################################################
#
#   $Revision:   
#   $Workfile:   dt_comp_price_load.sh  $
#     $Author:   $
#       $Date:   $
#    $Modtime:   $
#
# Description:   This script is used to load Competitor Price Files into Oracle database
#
#       Usage:   dt_comp_price_load.sh 
#    Log File:   (Standard log file name)
# Output File:   (Standard output file name)
#
###################### Version-Control-Comment-Area #######################
export VC_Revision=$(echo '$ $:0.0$'|cut "-d:" -f2-|cut "-d$" -f1)

arguments="$*"

# set the batch environment
. ~/nwc_setenv.sh

# Initialize Variables
todays_date=$(date +%Y%m%d)
todays_date_time=$(date +%Y%m%d%H%M)
log_file=$DWHLOG/$(basename $0 .sh).log
tmp_file=$DWHTMP/$(basename $0 .sh).tmp
data_dir=$DWHDATA/demandtec/ids
work_data_dir=$data_dir/work
inbound_data_dir="$DATABRIDGE/CANADA/demandtec/competitor_price"
new_dir=$(date +%Y%m%d%H%M)
comp_inbound_file_list=$work_data_dir/comp_inbound_file_list

help()
{
cat << EOF
   Usage is: $0
   This script is used to load competitor price files into Oracle database
EOF
}
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
log()
{
    echo $* | tee -a $log_file
}
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
terminate()
{
    email_abend
    log "------------------------------------------------------------------------------"
    log " $0(V$VC_Revision) $arguments Terminated Abnormally on $(date +%Y-%m-%d) ($(date +%H:%M:%S))"
    log "------------------------------------------------------------------------------"
    clean_session
    exit 1
}
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
clean_session()
{
    # Check if new backup/error directories are empty. If so, remove it.
    if [ -d $data_dir/backup/$new_dir ] ; then
       if [ $(ls $data_dir/backup/$new_dir | wc -l) -eq 0 ] ; then
           rmdir $data_dir/backup/$new_dir
       fi
    fi

    if [ -d $data_dir/error/$new_dir ] ; then
       if [ $(ls $data_dir/error/$new_dir | wc -l) -eq 0 ] ; then
           rmdir $data_dir/error/$new_dir
       fi
    fi

    if [ -f $tmp_file ] ; then
       rm $tmp_file
    fi
}
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
function email_abend
{
  SUBJECT="Competior Price Load abended ("$HOSTNAME")"
  LINE1="\n\tPlease contact RMS support immediately ...\n"
  LINE2="\tRMS support\n\tPhone: 204 795 0923"
  MAILTO=`grep COMP_PRICE_FILE_LOAD $DWHDATA/mail.list | cut -f3- -d','`

  if [ ! -z "$MAILTO" ] ; then
    ( cat $log_file; echo -e $LINE1; echo -e $LINE2 ) | mailx -m -s "$SUBJECT" $MAILTO 
  fi
  echo "Abend log file emailed successfully. " >> $log_file
}

function email_success
{
  SUBJECT="Comp Price File Load completed successfully("$HOSTNAME")"
  LINE1="\n\t For your information ...\n"
  LINE2="\tRMS support\n\tPhone: 204 795 0923"
  MAILTO=`grep COMP_PRICE_FILE_LOAD $DWHDATA/mail.list | cut -f3- -d','`

  if [ ! -z "$MAILTO" ] ; then
    ( cat | tail -n 10 $log_file; echo -e $LINE1; echo -e $LINE2 ) | mailx -m -s "$SUBJECT" $MAILTO 
  fi  
}
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
comp_price_db_load()
{
sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on
set lines 200
-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK
--
DECLARE
  iFILE_NAME       VARCHAR2(100) := '$1';
  oERROR_MSG       VARCHAR2(1000);
BEGIN
  NWC_DW.NWC_TO_DT.COMP_PRICE_FILE_LOAD(iFILE_NAME, oERROR_MSG);
END;
/
HH

# If the above SQL finished unsuccessfully then error out the message and exit.
grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Error loading $1 into Database"
   cat $tmp_file >>$log_file
   mv *.log *.bad $data_dir/error/$new_dir
   terminate
fi

cat $tmp_file >>$log_file
}

#------------------------------------------------------------------------------
# Main script begins
#------------------------------------------------------------------------------

# Read the userid, password from the file '.secure.*'
read userid password <~/.secure.$ORACLE_SID.isoper

# Validate the number of arguments passed to the shell script
if [ $# -gt 0 ] ; then
    log "Error: $0(V$VC_Revision)   Bad number of arguments  ($@)"
    help $0
    exit 1
else
    NumParms=$#
fi

# Move previous runs log/output file
if [ -f $log_file ] ; then
    cat $log_file >>$log_file.$(last_modification $log_file).bak
    rm  $log_file
fi

# Create Backup and Error Directories
if [ ! -d $data_dir/backup/$new_dir ] ; then
   mkdir $data_dir/backup/$new_dir
fi

if [ ! -d $data_dir/error/$new_dir ] ; then
   mkdir $data_dir/error/$new_dir
fi

log "---------------------------------------------------------------------------------"
log "$0(V$VC_Revision) $arguments Starting : $(date +%Y-%m-%d) ($(date +%H:%M:%S))"
log "---------------------------------------------------------------------------------"

HOSTNAME=$(hostname)
ls -1tr $inbound_data_dir/*.csv > $comp_inbound_file_list
RC="$?"
if [ $RC -ne 0 ] ; then
  log "No files found to load on $(date +%Y/%m/%d) ($(date +%H:%M:%S)) !"
  log "----------------------------------------------------------------------------------------"
  log " $0(V$VC_Revision) $arguments Completed Successfully $(date +%Y/%m/%d) ($(date +%H:%M:%S))"
  log "----------------------------------------------------------------------------------------"
  exit 0
else
while read -r COMP_PRICE_FILE_NAME
do
  echo $COMP_PRICE_FILE_NAME
  FILE_BASE_NAME=$(basename $COMP_PRICE_FILE_NAME)
  log "Loading $COMP_PRICE_FILE_NAME file into database started on : $(date +%Y/%m/%d) ($(date +%H:%M:%S)) !"
  # rename incoming file to competitor_price.csv  and change file permissions. This is required for oracle external tables used to pars the fiel.
  cp -fp $COMP_PRICE_FILE_NAME competitor_price.csv 
  chmod 777 competitor_price.csv
  #dos2unix competitor_price.csv
    
  comp_price_db_load $FILE_BASE_NAME
  log "Loading $COMP_PRICE_FILE_NAME file into database finished on : $(date +%Y/%m/%d) ($(date +%H:%M:%S)) !"

  log "Removing $COMP_PRICE_FILE_NAME file from $hostname : $(date +%Y/%m/%d) ($(date +%H:%M:%S)) !"
  mv -f $COMP_PRICE_FILE_NAME $data_dir/backup/$new_dir

  log "Remove external table log files"
   mv -f $inbound_data_dir/dt_comp_price_et.log $data_dir/backup/$new_dir
  done < $comp_inbound_file_list

mv -f $comp_inbound_file_list $data_dir/backup/$new_dir

log "----------------------------------------------------------------------------------------"
log " $0(V$VC_Revision) $arguments Completed Successfully $(date +%Y/%m/%d) ($(date +%H:%M:%S))"
log "----------------------------------------------------------------------------------------"

#sending email to business users after processing the file
  email_success
fi

exit 0

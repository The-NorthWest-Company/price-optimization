#!/sbin/sh
###########################################################################
#                                                                         #
#  THIS SOURCE CODE IS UNPUBLISHED, AND IS THE EXCLUSIVE PROPERTY OF NWC  #
#                                                                         #
###########################################################################
#
#   $Revision:   $ 
#   $Workfile:   $ 
#     $Author:   $ 
#       $Date:   $
#    $Modtime:   $
#
# Description:   
#
#       Usage:  dt_ids_error_report.sh 
#
#  Parameters:   N/A
#    Log File:   (Standard log file name)
# Output File:   (Standard output file name)
#
###################### Version-Control-Comment-Area #######################
# $Log:   $
#
#
###########################################################################
export VC_Revision=`echo '$Revision: $:0.0$'|cut "-d:" -f2-|cut "-d$" -f1`

# set the batch environment
. ~/nwc_setenv.sh

help()
{
   cat << EOF

   Usage is: $0


EOF
}

log()
{
   echo $* | tee -a $log_file
}

email_error_report()
{
  LINE1="dt_ids_error_report.sh has been failed"
  LINE2="\nThanks,\n"
  LINE3="RMS Support Team"
  LINE4="apatel@northwest.ca"
  MAILTO=`grep DT_IDS_ERROR $DWHDATA/mail.list | cut -f 3- -d ','`
  SUBJECT=`grep DT_IDS_ERROR $DWHDATA/mail.list | cut -f 2 -d ','`

  if [[ ! -z "$MAILTO" ]]
  then
     (echo $LINE1; tail -50 $log_file; echo $LINE2; echo $LINE3; echo $LINE4) | mailx -m -s "$ORACLE_SID $SUBJECT" $MAILTO >> $log_file 2>&1
  else
     log "Mail list for DT_IDS_ERROR is not found on `date +%Y/%m/%d` (`date +%H:%M:%S`)"
  fi
}

terminate()
{
   log "------------------------------------------------------------------------------"
   log " $0(V$VC_Revision) $arguments Terminated Abnormally on `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   log "------------------------------------------------------------------------------"

   email_error_report

   exit 1
}

email_rejected_report()
{

log " Creating/Emailing report of demandtech IDS Rejected Records : `date +%Y/%m/%d` (`date +%H:%M:%S`)"

cd $DWHTMP

sqlplus -s <<HH >$tmp_file
$userid/$password

set termout off;
set colsep ',';
set underline off;
set feedback off;
set pagesize 0;
set linesize 2000;
set trimspool on;

SPOOL $spool_file

SELECT 'SKU' AS SKU,
       'UPC' AS UPC,
       'SUPPLIER' AS DESCRIPTION, 
       'LOCATION_KEY' AS STORE,
       'PRICE_TYPE' AS PRICE_TYPE,
       'START_DATE' AS START_DATE,
       'CREATED_DTM' AS CREATED_DATE,
       'ERROR_MSG' AS ERROR_DESCRIPTION,
       'BATCH_ID' AS BATCH_ID,
       'FILE_TYPE' AS FILE_TYPE
  FROM DUAL;
--
SELECT CF.SKU,
       CF.UPC,
       CF.SUPPLIER,
       CF.LOCATION_KEY,
       CF.PRICE_TYPE,
       CF.START_DATE,
       CF.CREATED_DTM,
       CF.ERROR_MSG,
       CF.BATCH_ID,
       'CCF' FILE_TYPE
  FROM NWC_DW.DT_COST_INTF CF
 WHERE CF.ERROR_RPT_DTM IS NULL
   AND CF.EXTRACTED_TO_DT_IND = 'E'
   AND CF.ERROR_MSG IS NOT NULL
 UNION ALL
SELECT PF.SKU,
       PF.UPC,
       PF.SUPPLIER,
       PF.LOCATION_KEY,
       PF.PRICE_TYPE,
       PF.START_DATE,
       PF.CREATED_DTM,
       PF.ERROR_MSG,
       PF.BATCH_ID,
       'PCF' FILE_TYPE
  FROM NWC_DW.DT_PRICE_INTF PF
 WHERE PF.ERROR_RPT_DTM IS NULL
   AND PF.EXTRACTED_TO_DT_IND = 'E'
   AND PF.ERROR_MSG IS NOT NULL
 UNION ALL
SELECT SP.SKU,
       SP.UPC,
       SP.SUPPLIER,
       SP.LOCATION_KEY,
       SP.PRICE_TYPE,
       TRUNC(SYSDATE) AS START_DATE,
       SP.CREATED_DTM,
       SP.ERROR_MSG,
       SP.BATCH_ID,
       'SPA' FILE_TYPE
  FROM NWC_DW.DT_LOCATION_ATT_INTF SP
 WHERE SP.ERROR_RPT_DTM IS NULL
   AND SP.EXTRACTED_TO_DT_IND = 'E'
   AND SP.ERROR_MSG IS NOT NULL;
--
SPOOL OFF
--
  UPDATE NWC_DW.DT_COST_INTF
     SET ERROR_RPT_DTM = SYSDATE 
   WHERE ERROR_RPT_DTM IS NULL
     AND ERROR_MSG IS NOT NULL;
--
  UPDATE NWC_DW.DT_PRICE_INTF
     SET ERROR_RPT_DTM = SYSDATE 
   WHERE ERROR_RPT_DTM IS NULL
     AND ERROR_MSG IS NOT NULL;
--
  UPDATE NWC_DW.DT_LOCATION_ATT_INTF
     SET ERROR_RPT_DTM = SYSDATE 
   WHERE ERROR_RPT_DTM IS NULL
     AND ERROR_MSG IS NOT NULL;
--
HH
# If for any reason the above sql statement fails, it is going to put the
# Oracle Error message prefixed with the "ORA" word. Search for the word "ORA"
grep 'ORA-' $tmp_file > /dev/null
RC="$?"
if [ $RC -eq 0 ] ; then
   cat $tmp_file >> $log_file
   terminate
fi
cat $spool_file
if [[ -s $spool_file ]] ; then
   if [ `cat $spool_file | wc -l` -gt 1 ] ; then
      MAIL_ADDR=`grep "DT_IDS_ERROR" $DWHDATA/mail.list|cut -f 3- -d','`
      SUBJECT="Demandtech IDS Rejected Records Report ("`hostname`")"
      MSG_BODY="Please review attached rejected IDS files records report."
      #(echo $MSG_BODY; uuencode $spool_file $spool_file;) | mailx -m -s "$SUBJECT" $MAIL_ADDR >> $log_file 2>&1
      (echo $MSG_BODY;)|/bin/mailx -s "$SUBJECT" -a "$spool_file" $MAIL_ADDR >> $log_file 2>&1

      log " Email sent to $MAIL_ADDR on : `date +%Y/%m/%d` (`date +%H:%M:%S`)"

      rm $spool_file

   else
      log " No rejected records found. : `date +%Y/%m/%d` (`date +%H:%M:%S`)"
   fi
fi

} # End of email_error_report function

#------------------------------------------------------------------------------
# Main script begins
#------------------------------------------------------------------------------

arguments="$*"

todays_date=`date +%Y%m%d`
todays_date_time=`date +%Y%m%d%H%M`
new_dir=$todays_date
log_file=$DWHLOG/`basename $0 .sh`.log
tmp_file=$DWHTMP/`basename $0 .sh`.tmp
spool_file=$DWHTMP/$todays_date_time.dt_ids_rejected_records.csv

# Roll over log file
if [ -f $log_file ] ; then
   cat $log_file >> $log_file.`last_modification $log_file`.bak
   rm  $log_file
fi

#------------------------------------------------------------------------------
# Set command line options
#------------------------------------------------------------------------------

log "---------------------------------------------------------------------------------"
log "$0(V$VC_Revision) $arguments Starting : `date +%Y-%m-%d` (`date +%H:%M:%S`)"
log "---------------------------------------------------------------------------------\n"

NumParms=$#

# Validate the number of arguments passed to the shell script
if [ $NumParms -gt 0 ] ; then
   log "Error: $0(V$VC_Revision) Bad number of arguments ($@)"
   help
   exit 1
fi

# Read the userid, password from the file '.secure.*'
read userid password < ~/.secure.$ORACLE_SID.isoper

email_rejected_report

log "---------------------------------------------------------------------------------"
log " $0(V$VC_Revision) $arguments Completed Successfully `date +%Y/%m/%d` (`date +%H:%M:%S`)"
log "---------------------------------------------------------------------------------\n"

exit 0


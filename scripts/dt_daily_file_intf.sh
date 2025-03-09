#!/sbin/sh
###########################################################################
#                                                                         #
#  THIS SOURCE CODE IS UNPUBLISHED, AND IS THE EXCLUSIVE PROPERTY OF NWC  #
#                                                                         #
###########################################################################
#
#   $Revision:   $
#   $Workfile:   dt_daily_file_intf.sh  $
#     $Author:   $
#       $Date:   $
#    $Modtime:   $
#
# Description:   This script is for creating and sending Full Load Files to DT. 
#                1. Product Hierarchy Noder Master
#                2. Product Hierarchy Tree
#                3. Location
#                4. Sellable Item

#       Usage:   $
#
#    Log File:   (Standard log file name)
# Output File:   (Standard output file name)
#                                   
###################### Version-Control-Comment-Area #######################
export VC_Revision=`echo '$Revision:   1.0  $:0.0$'|cut "-d:" -f2-|cut "-d$" -f1`

arguments="$*"

# set the batch environment
. ~/nwc_setenv.sh

typeset -u param=$1
todays_date=`date +%Y%m%d`
todays_date_time=`date +%Y%m%d%H%M%S`

log_file=$DWHLOG/`basename $0 .sh`_${param}.log
tmp_file=$DWHTMP/`basename $0 .sh`_${param}.tmp
temp_file=$DWHTMP/`basename $0 .sh`.temp
sql_file=$DWHTMP/`basename $0 .sh`.sql
data_dir=$DWHDATA/demandtec/ids/work/
bkp_dir=$DWHDATA/demandtec/ids/backup/
banner="NCR"

host_env="("`hostname`" - $ORACLE_SID)"

if [ "$ORACLE_SID" = "dwdev1" ]
then
   ENV="DEV1"
elif [ "$ORACLE_SID" = "dwtst1" ]
then
   ENV="TST1"
elif [ "$ORACLE_SID" = "rendevu" ]
then
   ENV="PRD"
else
   log "Invalid Environment or job isn't setup to run in this ( $ORACLE_SID ) environment"
   #terminate
   ENV="DEV1"
fi


# Read the userid, password from the file '.secure.*'
read userid password < ~/.secure.$ORACLE_SID.isoper

function Help
{ 
   echo "Usage is: $1 "
}

log()
{
   echo $* | tee -a $log_file
}

terminate()
{
   log "------------------------------------------------------------------------------"
   log " $0(V$VC_Revision) $arguments Terminated Abnormally on `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   log "------------------------------------------------------------------------------"
   exit 1
}
cleanup()
{
   log "\n Clean Up Started \n"
   # Remove log files older than 7 days
   find $DWH -maxdepth 2 -type f -name `basename $0 .sh`.log* -mtime +7 -exec rm {} \;
}
#Generate Files in tmp file

Create_File_PHM()
{
sqlplus -s << HH >$tmp_file
$userid/$password

set pagesize 0;
set heading off;
set feedback off;
set linesize 25000;
set trimspool on;
set trimout on;
set tab off;
set colsep ,;
set wrap off;

spool $tmp_file;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

   SELECT LVL
     FROM (
   SELECT PHNM_LEVEL1 LVL, 1 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   UNION
   SELECT PHNM_LEVEL2 LVL, 2 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   UNION
   SELECT PHNM_LEVEL3 LVL, 3 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   UNION
   SELECT PHNM_LEVEL4 LVL, 4 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   UNION
   SELECT PHNM_LEVEL5 LVL, 5 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   UNION
   SELECT PHNM_LEVEL6 LVL, 6 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   )
    ORDER BY SORT_BY;
           
spool off;
HH

grep -E 'ORA-' $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
 cat $tmp_file >> $log_file
 log "Oracle to SQL error encountered while run PL/SQL block at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
 terminate
fi

if [ -f $tmp_file ]
then 
    FileType="PHM"
   if [ $? -ne 0 ]
   then
     log "Error: Unable to create file"
     terminate
   else
     file_count=$(wc -l < $tmp_file)
     if [ $file_count -gt 1 ]
     then
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        # Delete previous run full load file if it exists in work folder then delete it
        if [ `ls -1 $data_dir/$FileType* | wc -l` -ne 0 ] ; then
           ls -1 $data_dir/$FileType*  >> $log_file
           rm $data_dir/$FileType*
        fi
        mv $Filename $data_dir
        send_file_to_dt 
     else
       log "Empty import file created for $FileType"
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
      # terminate
     fi
   fi
else
  log "Query returned zero rows. No csv file produced."
  terminate
fi

}

Create_File_PHT()
{
sqlplus -s << HH >$temp_file
$userid/$password

set linesize 25000;
set serveroutput on size 1000000;
declare
  mv_count number(5):=0;
begin
  select count(*) into mv_count
    from nwc_dw.product_hrchy_vw;
  dbms_output.put_line('Number of records in view :' || mv_count);
end;
/
HH
grep -E 'ORA-' $temp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
 cat $temp_file >> $log_file
 log "Oracle to SQL error encountered while run PL/SQL block at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
fi
cat $temp_file >> $log_file

sqlplus -s << HH >$tmp_file
$userid/$password

set pagesize 0;
set heading off;
set feedback off;
set linesize 25000;
set trimspool on;
set trimout on;
set tab off;
set colsep ,;
set wrap off;

spool $tmp_file;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

   SELECT LVL
     FROM (
   SELECT PHT_LEVEL1 LVL, 1 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   UNION
   SELECT PHT_LEVEL2 LVL, 2 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   UNION
   SELECT PHT_LEVEL3 LVL, 3 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   UNION
   SELECT PHT_LEVEL4 LVL, 4 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   UNION
   SELECT PHT_LEVEL5 LVL, 5 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   UNION
   SELECT PHT_LEVEL6 LVL, 6 SORT_BY
     FROM NWC_DW.PRODUCT_HRCHY_VW
   )
    ORDER BY SORT_BY;
           
spool off;
HH

grep -E 'ORA-' $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
 cat $tmp_file >> $log_file
 log "Oracle to SQL error encountered while run PL/SQL block at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
 terminate
fi

if [ -f $tmp_file ]
then 
    FileType="PHT"
   if [ $? -ne 0 ]
   then
     log "Error: Unable to create file"
     terminate
   else
     file_count=$(wc -l < $tmp_file)
     if [ $file_count -gt 1 ]
     then
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        # Delete previous run full load file if it exists in work folder then delete it
        if [ `ls -1 $data_dir/$FileType* | wc -l` -ne 0 ] ; then
           ls -1 $data_dir/$FileType*  >> $log_file
           rm $data_dir/$FileType*
        fi
        mv $Filename $data_dir
        send_file_to_dt 
     else
        log "Empty import file created for $FileType"
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
      # terminate
     fi
   fi
else
  log "Query returned zero rows. No csv file produced."
  terminate
fi

}

Create_File_LOC()
{
sqlplus -s << HH >$tmp_file
$userid/$password

set pagesize 0;
set heading off;
set feedback off;
set linesize 25000;
set trimspool on;
set trimout on;
set tab off;
set colsep ,;
set wrap off;

spool $tmp_file;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

   SELECT LOC_DETAIL
     FROM NWC_DW.DT_LOC_DETAIL_VW;
           
spool off;
HH

grep -E 'ORA-' $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
 cat $tmp_file >> $log_file
 log "Oracle to SQL error encountered while run PL/SQL block at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
 terminate
fi

if [ -f $tmp_file ]
then 
   FileType="LOC"
   if [ $? -ne 0 ]
   then
     log "Error: Unable to create file"
     terminate
   else
     file_count=$(wc -l < $tmp_file)
     if [ $file_count -gt 1 ]
     then
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        # Delete previous run full load file if it exists in work folder then delete it
        if [ `ls -1 $data_dir/$FileType* | wc -l` -ne 0 ] ; then
           ls -1 $data_dir/$FileType*  >> $log_file
           rm $data_dir/$FileType*
        fi
        mv $Filename $data_dir
        send_file_to_dt 
     else
        log "Empty import file created for $FileType"
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
      # terminate
     fi
   fi
else
  log "Query returned zero rows. No csv file produced."
  terminate
fi

}

Create_File_SIF()
{
sqlplus -s << HH >$tmp_file
$userid/$password

set pagesize 0;
set heading off;
set feedback off;
set linesize 25000;
set trimspool on;
set trimout on;
set tab off;
set colsep ,;
set wrap off;

spool $tmp_file;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

   SELECT SIF_ITEM_DETAIL
     FROM NWC_DW.DT_SIF_DETAIL_VW;
           
spool off;
HH

grep -E 'ORA-' $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
 cat $tmp_file >> $log_file
 log "Oracle to SQL error encountered while run PL/SQL block at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
 terminate
fi

if [ -f $tmp_file ]
then 
   FileType="SIF"
   if [ $? -ne 0 ]
   then
     log "Error: Unable to create file"
     terminate
   else
     file_count=$(wc -l < $tmp_file)
     if [ $file_count -gt 1 ]
     then
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        # Delete previous run full load file if it exists in work folder then delete it
        if [ `ls -1 $data_dir/$FileType* | wc -l` -ne 0 ] ; then
           ls -1 $data_dir/$FileType*  >> $log_file
           rm $data_dir/$FileType*
        fi
        mv $Filename $data_dir
        send_file_to_dt 
     else
        log "Empty import file created for $FileType"
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
      # terminate
     fi
   fi
else
  log "Query returned zero rows. No csv file produced."
  terminate
fi

}
Create_File_SPS()
{
sqlplus -s << HH >$tmp_file
$userid/$password

set pagesize 0;
set heading off;
set feedback off;
set linesize 25000;
set trimspool on;
set trimout on;
set tab off;
set colsep ,;
set wrap off;

spool $tmp_file;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

   SELECT ITEM_KEY ||'|'||
          LOCATION_KEY ||'|'||
          STATUS ||'|'||
          '|'||'|'||'|'
     FROM NWC_DW.DT_LOCATION_STATUS_INTF
   WHERE EXTRACTED_TO_DT_IND = 'N'
     ORDER BY SKU, LOCATION_KEY;
           
spool off;
--
  UPDATE NWC_DW.DT_LOCATION_STATUS_INTF
     SET EXTRACTED_TO_DT_IND = 'Y',
         EXTRACTED_TO_DT_DTM = SYSDATE
   WHERE EXTRACTED_TO_DT_IND = 'N';
--
COMMIT;
--
HH

grep -E 'ORA-' $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
 cat $tmp_file >> $log_file
 log "Oracle to SQL error encountered while run PL/SQL block at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
 terminate
fi

if [ -f $tmp_file ]
then 
   FileType="SPS"
   if [ $? -ne 0 ]
   then
     log "Error: Unable to create file"
     terminate
   else
     file_count=$(wc -l < $tmp_file)
     if [ $file_count -gt 1 ]
     then
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
        send_file_to_dt 
     else
       log "Empty import file created for $FileType"
        Filename=$FileType"_"$banner"_"$todays_date.txt
        touch $Filename
        mv $tmp_file $Filename
        mv $Filename $data_dir
       #terminate
     fi
   fi
else
  log "Query returned zero rows. No csv file produced."
  terminate
fi
}

Create_File_PCF()
{
sqlplus -s << HH >$tmp_file
$userid/$password

set pagesize 0;
set heading off;
set feedback off;
set linesize 25000;
set trimspool on;
set trimout on;
set tab off;
set colsep ,;
set wrap off;

spool $tmp_file;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

   SELECT ITEM_KEY ||'|'||
          LOCATION_KEY ||'|'||
          TO_CHAR(START_DATE, 'MM/DD/YYYY') ||'|'||
          UNIT_RETAIL ||'|'||
          UNIT_RETAIL_MULTIPLE ||'|'||
          '|'||'|'||'|'||'|'||'|'||'|'||'|'
     FROM (SELECT DISTINCT ITEM_KEY, LOCATION_KEY, START_DATE, UNIT_RETAIL, UNIT_RETAIL_MULTIPLE
             FROM NWC_DW.DT_PRICE_INTF
            WHERE EXTRACTED_TO_DT_IND = 'N'
            ORDER BY ITEM_KEY, LOCATION_KEY);
           
spool off;
--
  UPDATE NWC_DW.DT_PRICE_INTF
     SET EXTRACTED_TO_DT_IND = 'Y',
         EXTRACTED_TO_DT_DTM = SYSDATE
   WHERE EXTRACTED_TO_DT_IND = 'N';
--
COMMIT;
--
HH

grep -E 'ORA-' $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
 cat $tmp_file >> $log_file
 log "Oracle to SQL error encountered while run PL/SQL block at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
 terminate
fi

if [ -f $tmp_file ]
then 
   FileType="PCF"
   if [ $? -ne 0 ]
   then
     log "Error: Unable to create file"
     terminate
   else
     file_count=$(wc -l < $tmp_file)
     if [ $file_count -gt 1 ]
     then
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
        send_file_to_dt 
     else
        log "Empty import file created for $FileType"
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
      # terminate
     fi
   fi
else
  log "Query returned zero rows. No csv file produced."
  terminate
fi
}

Create_File_SPA()
{
sqlplus -s << HH >$tmp_file
$userid/$password

set pagesize 0;
set heading off;
set feedback off;
set linesize 25000;
set trimspool on;
set trimout on;
set tab off;
set colsep ,;
set wrap off;

spool $tmp_file;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

   SELECT A.ITEM_KEY ||'|'||
          A.LOCATION_KEY ||'|'||
          A.NNC_SAVING_AMT ||'|'||
          A.AFRM_SAVING_AMT ||'|'||
          (NVL(A.COL_SAVING_PCT, 0) + NVL(A.HFP_SAVING_PCT, 0)) 
     FROM NWC_DW.DT_LOCATION_ATT_INTF A,
          NWC_DW.DT_SIF_STG B,
          NWC_DW.DT_LOC_STG L
   WHERE A.EXTRACTED_TO_DT_IND = 'N'
     AND A.ITEM_KEY = B.ITEM_KEY
     AND A.SKU = B.SKU
     AND A.LOCATION_KEY = L.LOCATIONKEY
     ORDER BY A.SKU, A.LOCATION_KEY;
           
spool off;
--
  UPDATE NWC_DW.DT_LOCATION_ATT_INTF
     SET EXTRACTED_TO_DT_IND = 'Y',
         EXTRACTED_TO_DT_DTM = SYSDATE
   WHERE EXTRACTED_TO_DT_IND = 'N';
--
COMMIT;
--
HH

grep -E 'ORA-' $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
 cat $tmp_file >> $log_file
 log "Oracle to SQL error encountered while run PL/SQL block at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
 terminate
fi

if [ -f $tmp_file ]
then 
   FileType="SPA"
   if [ $? -ne 0 ]
   then
     log "Error: Unable to create file"
     terminate
   else
     file_count=$(wc -l < $tmp_file)
     if [ $file_count -gt 1 ]
     then
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
        send_file_to_dt 
     else
        log "Empty import file created for $FileType"
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
      # terminate
     fi
   fi
else
  log "Query returned zero rows. No csv file produced."
  terminate
fi
}

Create_File_CCF()
{
sqlplus -s << HH >$tmp_file
$userid/$password

set pagesize 0;
set heading off;
set feedback off;
set linesize 25000;
set trimspool on;
set trimout on;
set tab off;
set colsep ,;
set wrap off;

spool $tmp_file;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

   SELECT A.ITEM_KEY ||'|'||
          A.LOCATION_KEY ||'|'||'|'||
          A.SUPPLIER ||'|'||
          1 ||'|'||'|'||
          to_char(A.START_DATE, 'MM/DD/YYYY') ||'|'||
          A.UNIT_COST ||'|'||
          A.UNIT_OTHER_COST1 ||'|'||
          A.TOTAL_FREIGHT_AMT ||'|'||'|'||
          A.UNIT_OTHER_COST2 ||'|'||'|'
     FROM NWC_DW.DT_COST_INTF A,
          NWC_DW.DT_SIF_STG B,
          NWC_DW.DT_LOC_STG L
   WHERE A.EXTRACTED_TO_DT_IND = 'N'
     AND A.ITEM_KEY = B.ITEM_KEY
     AND A.SKU = B.SKU
     AND A.LOCATION_KEY = L.LOCATIONKEY
     ORDER BY A.SKU, A.LOCATION_KEY;
           
spool off;
--
  UPDATE NWC_DW.DT_COST_INTF
     SET EXTRACTED_TO_DT_IND = 'Y',
         EXTRACTED_TO_DT_DTM = SYSDATE
   WHERE EXTRACTED_TO_DT_IND = 'N';
--
COMMIT;
--
HH

grep -E 'ORA-' $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
 cat $tmp_file >> $log_file
 log "Oracle to SQL error encountered while run PL/SQL block at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
 terminate
fi

if [ -f $tmp_file ]
then 
   FileType="CCF"
   if [ $? -ne 0 ]
   then
     log "Error: Unable to create file"
     terminate
   else
     file_count=$(wc -l < $tmp_file)
     if [ $file_count -gt 1 ]
     then
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
        send_file_to_dt 
     else
        log "Empty import file created for $FileType"
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
      # terminate
     fi
   fi
else
  log "Query returned zero rows. No csv file produced."
  terminate
fi
}

Create_File_CPL()
{
sqlplus -s << HH >$tmp_file
$userid/$password

set pagesize 0;
set heading off;
set feedback off;
set linesize 25000;
set trimspool on;
set trimout on;
set tab off;
set colsep ,;
set wrap off;

spool $tmp_file;

WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

   SELECT A.ITEM_KEY ||'|'||
          A.LOCATION_KEY ||'|'||
          A.COMPETITOR_NAME ||'|'||
          A.UNIT_PRICE ||'|'||
          A.UNIT_MULTIPLE ||'|'||
          A.PRICE_TYPE ||'|'||
          to_char(A.EFFECTIVE_DATE, 'MM/DD/YYYY')
     FROM NWC_DW.DT_COMP_PRICE_INTF A,
          NWC_DW.DT_SIF_STG B,
          NWC_DW.DT_LOC_STG L
   WHERE A.EXTRACTED_TO_DT_IND = 'N'
     AND A.ITEM_KEY = B.ITEM_KEY
     AND A.SKU = B.SKU
     AND A.LOCATION_KEY = L.LOCATIONKEY
     AND TRUNC(A.EFFECTIVE_DATE) <= TRUNC(SYSDATE)
     ORDER BY A.SKU, A.LOCATION_KEY;
           
spool off;
--
  UPDATE NWC_DW.DT_COMP_PRICE_INTF
     SET EXTRACTED_TO_DT_IND = 'Y',
         EXTRACTED_TO_DT_DTM = SYSDATE
   WHERE EXTRACTED_TO_DT_IND = 'N'
     AND TRUNC(EFFECTIVE_DATE) <= TRUNC(SYSDATE);
--
COMMIT;
--
HH

grep -E 'ORA-' $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
 cat $tmp_file >> $log_file
 log "Oracle to SQL error encountered while run PL/SQL block at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
 terminate
fi
if [ -f $tmp_file ]
then 
   FileType="CPL"
   if [ $? -ne 0 ]
   then
     log "Error: Unable to create file"
     terminate
   else
     file_count=$(wc -l < $tmp_file)
     if [ $file_count -gt 0 ]
     then
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
        send_file_to_dt 
     else
        log "Empty import file created for $FileType"
        Filename=$FileType"_"$banner"_"$todays_date.txt
        mv $tmp_file $Filename
        mv $Filename $data_dir
      # terminate
     fi
   fi
else
  log "Query returned zero rows. No csv file produced."
  terminate
fi
}
send_file_to_dt()
{
if [ `ls -1 $data_dir | wc -l` -ne 0 ] ; then
    cd $data_dir
    sftp -oPort=160 -oIdentityFile=~/.ssh/demandtec.prv dti8031@FileVault.demandtec.com <<TTT
    cd upload
    ls -l
    mput *
    quit
TTT

   RC="$?"
   if [[ $RC -ne 0 ]] ; then
      log "SFTP session failed. Error code $RC"
      terminate
   fi
   mv $data_dir/*.txt $bkp_dir
fi
}
#------------------------------------------------------------------------------
# Set command line options
#------------------------------------------------------------------------------

# Roll over log file
if [[ -f $log_file ]]
then
   cat $log_file >> $log_file.`last_modification $log_file`.bak
   rm  $log_file
fi

log "---------------------------------------------------------------------------------"
log "$0(V$VC_Revision) $arguments Starting : `date +%Y-%m-%d` (`date +%H:%M:%S`)"
log "---------------------------------------------------------------------------------"

NumParms=$#

# Validate the number of arguments passed to the shell script
#------------------------------------------------------------------------------
# Main script begins
#------------------------------------------------------------------------------
log  "Database Instance: $ORACLE_SID"
log  "Number of Parameters: $NumParms"


if [ $NumParms -eq 0 ];  then
   log "Creating and Sending all the Files"
   Create_File_PHM
   Create_File_PHT
   Create_File_LOC
   Create_File_SIF
   Create_File_SPS
   Create_File_PCF
   Create_File_SPA
   Create_File_CCF
   Create_File_CPL
   
elif [ $NumParms -eq 1 ] ; then

#Create Files
    case "$1" in

      [1]) echo  "Creating file for PHM"
           Create_File_PHM
         ;;
      [2]) echo  "Creating file for PHT"
           Create_File_PHT
         ;;
      [3]) echo  "Creating file for LOC"
           Create_File_LOC
         ;;
      [4]) echo  "Creating file for SIF"
           Create_File_SIF
         ;;
      [5]) echo  "Creating file for SPS"
           Create_File_SPS
         ;;
      [6]) echo  "Creating file for PCF"
           Create_File_PCF
         ;;
      [7]) echo  "Creating file for SPA"
           Create_File_SPA
         ;;
      [8]) echo  "Creating file for CCF"
           Create_File_CCF
         ;;
      [9]) echo  "Creating file for CPL"
           Create_File_CPL
         ;;
      *) echo "Invalid FILE NUMBER"
         ;;
    esac
   
else
   log "Error: $0(V$VC_Revision) Bad number of arguments ($@)"
   help
   terminate

fi

cleanup

log "--------------------------------------------------------------------------"
log " $0(V$VC_Revision) $arguments Completed Successfully `date +%Y/%m/%d` (`date +%H:%M:%S`)"
log "--------------------------------------------------------------------------"

exit 0

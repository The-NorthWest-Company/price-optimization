#!/sbin/sh
###########################################################################
#                                                                         #
#  THIS SOURCE CODE IS UNPUBLISHED, AND IS THE EXCLUSIVE PROPERTY OF NWC  #
#                                                                         #
###########################################################################
#
#   $Revision:   1.0  $
#   $Workfile:   dt_full_load_file_feed.sh  $
#     $Author:   ISAPAT  $
#   $Revision:   1.0  $
#       $Date:   01 Nov 2022 11:24:04  $
#    $Modtime:   01 Nov 2022 11:22:12  $#
# Description:   This script is used to load data into Demandtech Location and SIF files. 
#
#       Usage:   dt_full_load_file_feed.sh
#
#    Log File:   (Standard log file name)
# Output File:   (Standard output file name)
#                                   
###################### Version-Control-Comment-Area #######################
export VC_Revision=`echo '$Revision:   1.0  $:0.0$'|cut "-d:" -f2-|cut "-d$" -f1`

# set the batch environment
. ~/nwc_setenv.sh

todays_date=`date +%Y%m%d`
todays_date_time=`date +%Y%m%d%H%M`

log_file=$DWHLOG/`basename $0 .sh`.log
tmp_file=$DWHTMP/`basename $0 .sh`.tmp

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
   log " $0(V$VC_Revision) $NumParms Terminated Abnormally on `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   log "------------------------------------------------------------------------------"
   exit 1
} 
Cleanup()
{
   log "\n Clean Up Started \n"
   # Remove log files older than 7 days
   find $DWH -maxdepth 2 -type f -name `basename $0 .sh`.log* -mtime +7 -exec rm {} \;
}
Load_data_into_loc_tb()
{
log "\n`date +%Y/%m/%d-%H:%M:%S` Starting function \"Load_data_into_loc_tb\"\n"

sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on size 1000000
set linesize 999
         
-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK
--
DECLARE
  vSysDate           VARCHAR2(50)   := NULL;
  oERROR_MSG         VARCHAR2(1000);
         
BEGIN
  --
  SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
    INTO vSysDate
    FROM DUAL;
  --  
    NWC_DW.NWC_TO_DT.POPULATE_DT_LOC_STG_TB(oERROR_MSG );    
  -- 
  SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
    INTO vSysDate
    FROM DUAL;
   --
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE('Location File Data loaded into Staging tables at '||vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   --
   COMMIT;
   --
   EXCEPTION
   WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,1,255));
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,256,255));
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,511,255));
   END;
   --
/
HH
grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while Loading data into Demantech Location table AT `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   cat $tmp_file
else
   log "---------------------------------------------------------------------------------"
   log "Data has been Successfully Loaded into DT Location staging table AT `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   log "---------------------------------------------------------------------------------\n" 
fi
}

Load_data_into_sif_tb()
{
log "\n`date +%Y/%m/%d-%H:%M:%S` Starting function \"Load_data_into_sif_tb\"\n"

sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on size 1000000
set linesize 999
         
-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK
--
DECLARE
  vSysDate           VARCHAR2(50)   := NULL;
  oERROR_MSG         VARCHAR2(1000);
         
BEGIN
  --
  SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
    INTO vSysDate
    FROM DUAL;
  --  
    NWC_DW.NWC_TO_DT.POPULATE_DT_SIF_STG_TB(oERROR_MSG );    
  -- 
  SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
    INTO vSysDate
    FROM DUAL;
   --
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE('Sellable Items File Data loaded into Staging tables at '||vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   --
   COMMIT;
   --
   EXCEPTION
   WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,1,255));
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,256,255));
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,511,255));
   END;
   --
/
HH
grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while Loading data into Demantech Items table AT `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   cat $tmp_file
else
   log "---------------------------------------------------------------------------------"
   log "Data has been Successfully Loaded into DT Items staging table AT `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   log "---------------------------------------------------------------------------------\n" 
fi
}

Load_data_into_sps_tb()
{
log "\n`date +%Y/%m/%d-%H:%M:%S` Starting function \"Load_data_into_sps_tb\"\n"

sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on size 1000000
set linesize 999
         
-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK
--
DECLARE
  vSysDate           VARCHAR2(50)   := NULL;
  oERROR_MSG         VARCHAR2(1000);
         
BEGIN
  --
  SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
    INTO vSysDate
    FROM DUAL;
  --  
    NWC_DW.NWC_TO_DT.POPULATE_DT_SPS_TB(oERROR_MSG );    
  -- 
  SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
    INTO vSysDate
    FROM DUAL;
   --
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE('Item Location Status File Data loaded into Staging tables at '||vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   --
   COMMIT;
   --
   EXCEPTION
   WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,1,255));
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,256,255));
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,511,255));
   END;
   --
/
HH
grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while Loading data into Demantech Item Location Status table AT `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   cat $tmp_file
else
   log "---------------------------------------------------------------------------------"
   log "Data has been Successfully Loaded into DT Item Location Status staging table AT `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   log "---------------------------------------------------------------------------------"
fi
}
#------------------------------------------------------------------------------
Reject_Error_Records()
{
log "\n`date +%Y/%m/%d-%H:%M:%S` Starting function \"Reject_Error_Records\"\n"

sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on size 1000000
set linesize 999
         
-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK
--
DECLARE
  vSysDate           VARCHAR2(50)   := NULL;
  oERROR_MSG         VARCHAR2(1000);
         
BEGIN
  --
  SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
    INTO vSysDate
    FROM DUAL;
  --  
    NWC_DW.NWC_TO_DT.UPDATE_DT_COST_INTF_ERROR_REC(oERROR_MSG );    
  -- 
    NWC_DW.NWC_TO_DT.UPDATE_DT_PRICE_INTF_ERROR_REC(oERROR_MSG );    
  -- 
    NWC_DW.NWC_TO_DT.UPDATE_DT_SPA_INTF_ERROR_REC(oERROR_MSG );    
  -- 
  SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
    INTO vSysDate
    FROM DUAL;
   --
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE('Reject Error Records completed at '||vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   --
   COMMIT;
   --
   EXCEPTION
   WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,1,255));
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,256,255));
        DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,511,255));
   END;
   --
/
HH
grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while updating Error Records AT `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   cat $tmp_file
else
   log "---------------------------------------------------------------------------------"
   log "Error Records have  been Successfully updated into DT tables AT `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   log "---------------------------------------------------------------------------------"
fi
}
#------------------------------------------------------------------------------
# Main script begins
#------------------------------------------------------------------------------
NumParms=$#
echo $NumParms

# Read the userid, password from the file '.secure.*'
read userid password < ~/.secure.$ORACLE_SID.isoper

# Roll over log file
if [[ -f $log_file ]]
then
   cat $log_file >> $log_file.`last_modification $log_file`.bak
   rm  $log_file
fi

log "------------------------------------------------------------------------------\n"
log "$0(V$VC_Revision) $0 Starting : `date +%Y-%m-%d` (`date +%H:%M:%S`)"
log "------------------------------------------------------------------------------\n"

if [ $NumParms -eq 0 ];  then
   log "Loading data into all the DW Demantech tables"
   Load_data_into_loc_tb
   Load_data_into_sif_tb
   Load_data_into_sps_tb

elif [ $NumParms -eq 1 ] ; then

#Load Data
    case "$1" in
      [1]) echo  "Load Data into Location Table"
           Load_data_into_loc_tb
         ;;
      [2]) echo  "Load Data into Sellable Item(SIF) Table"
           Load_data_into_sif_tb
         ;;
      [3]) echo  "Load Data into Store Product Status Table"
           Load_data_into_sps_tb
          #Reject Error Records Routine
           Reject_Error_Records
         ;;
      *) echo "Invalid Option Selected"
         ;;
    esac

else
   log "Error: $0(V$VC_Revision) Bad number of arguments ($@)"
   help
   terminate

fi

Cleanup

log "--------------------------------------------------------------------------"
log " $0(V$VC_Revision) $NumParms Completed Successfully `date +%Y/%m/%d` (`date +%H:%M:%S`)"
log "--------------------------------------------------------------------------"

exit 0

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
# Description:   The purpose of this script is to call an Oracle package to
#                move data from Integration to DW for DT interface
#                
#
#       Usage:   hq2dw_data_feed.sh
#
#  Parameters:   
#
#    Log File:   (Standard log file name)
# Output File:   (Standard output file name)
#
###################### Version-Control-Comment-Area #######################
# $Log:   $
#
###########################################################################
export VC_Revision=`echo '$Revision:   1.7  $:0.0$'|cut "-d:" -f2-|cut "-d$" -f1`

# set the batch environment
. ~/nwc_setenv.sh ; Scriptname_var=$0

todays_date=`date +%Y%m%d`
todays_date_time=`date +%Y%m%d%H%M`
log_file=$RMSLOG/`basename $Scriptname_var .sh`.log
tmp_file=$RMSTMP/`basename $Scriptname_var .sh`.tmp

help()
{
   cat << EOF

   Usage is: $1
    The purpose of this script is to call an Oracle package to bring data from PowerHQ to DW 
 
EOF
}

log()
{
   echo -e $* | tee -a $log_file
}

terminate()
{
   log "------------------------------------------------------------------------------"
   log " $Scriptname_var(V$VC_Revision) $NumParms Terminated Abnormally on `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   log "------------------------------------------------------------------------------"

   exit 1
}
Load_data_into_sps_tb()
{
sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on size 1000000
set linesize 999

-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

DECLARE
   oERROR_MSG         VARCHAR2(4000);
   vSysDate           VARCHAR2(50);
BEGIN
   --
   SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
     INTO vSysDate
     FROM DUAL;

   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE(' POWERHQ_TO_DW.POPULATE_DT_LOC_INTF_STG_TB called at: '|| vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');

   POWERHQ_TO_DW.POPULATE_DT_LOC_INTF_STG_TB (oERROR_MSG ); 

   SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
     INTO vSysDate
     FROM DUAL;

   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE(' POWERHQ_TO_DW.POPULATE_DT_LOC_INTF_STG_TB completed at: '|| vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');

EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,1,255));
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,256,255));
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,511,255));
END;
/
COMMIT;
HH

grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while loading SPS data into Demandtech: `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   terminate
else
   log "------------------------------------------------------------------------------"
   log " Data has been successfully Loaded into Store Status table At  `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   log "------------------------------------------------------------------------------"
fi 
}

Load_data_into_spa_tb()
{
sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on size 1000000
set linesize 999

-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

DECLARE
   oERROR_MSG         VARCHAR2(4000);
   vSysDate           VARCHAR2(50);
BEGIN
   --
   SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
     INTO vSysDate
     FROM DUAL;

   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE(' POWERHQ_TO_DW.POPULATE_DT_LOC_ATT_INTF_TB called at: '|| vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');

   POWERHQ_TO_DW.POPULATE_DT_LOC_ATT_INTF_TB (oERROR_MSG ); 

   SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
     INTO vSysDate
     FROM DUAL;

   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE(' POWERHQ_TO_DW.POPULATE_DT_LOC_ATT_INTF_TB completed at: '|| vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');

EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,1,255));
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,256,255));
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,511,255));
END;
/
COMMIT;
HH

grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while loading SPA data into Demandtech: `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   terminate
else
   log "------------------------------------------------------------------------------"
   log " Data has been successfully Loaded into Store location Attributes table At  `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   log "------------------------------------------------------------------------------"
fi 
}

Load_data_into_ccf_tb()
{
sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on size 1000000
set linesize 999

-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

DECLARE
   oERROR_MSG         VARCHAR2(4000);
   vSysDate           VARCHAR2(50);
BEGIN
   --
   SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
     INTO vSysDate
     FROM DUAL;

   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE(' POWERHQ_TO_DW.POPULATE_DT_COST_INTF_TB called at: '|| vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');

   POWERHQ_TO_DW.POPULATE_DT_COST_INTF_TB (oERROR_MSG ); 

   SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
     INTO vSysDate
     FROM DUAL;

   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE(' POWERHQ_TO_DW.POPULATE_DT_COST_INTF_TB completed at: '|| vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');

EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,1,255));
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,256,255));
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,511,255));
END;
/
COMMIT;
HH

grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while loading CCF data into Demandtech: `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   terminate
else
   log "------------------------------------------------------------------------------"
   log " Data has been successfully Loaded into DW Cost table At  `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   log "------------------------------------------------------------------------------"
fi
}

#------------------------------------------------------------------------------
# Main script begins
#------------------------------------------------------------------------------
NumParms=$#
echo $NumParms

# Read the userid, password from the file '.secure.*'
read userid password < ~/.secure.$ORACLE_SID.nwc_oper

log "---------------------------------------------------------------------------------"
log "$Scriptname_var(V$VC_Revision) $NumParms Starting : `date +%Y-%m-%d` (`date +%H:%M:%S`)"
log "---------------------------------------------------------------------------------\n"

if [ $NumParms -eq 0 ];  then
     Load_data_into_sps_tb
     Load_data_into_spa_tb
     Load_data_into_ccf_tb
elif [ $NumParms -eq 1 ] ; then
#Load Data
    case "$1" in
    [1]) echo  "Load Data into Location Status(SPS) Table"
	 Load_data_into_sps_tb
       ;;
    [2]) echo  "Load Data into Item Store Attributes(SPA) Table"
         Load_data_into_spa_tb
       ;;
    [3]) echo  "Load Data into Cost Table"
         Load_data_into_ccf_tb
      ;;
     *) echo "Invalid Option Selected"
     ;;
    esac

else
     log "Error: $Scriptname_var(V$VC_Revision) Bad number of arguments ($@)"
     help
     terminate
fi

# Roll over log file
if [ -f $log_file ] ; then
   cat $log_file >> $log_file.`last_modification $log_file`.bak
   rm  $log_file
fi

log "----------------------------------------------------------------------------------------"
log " $Scriptname_var(V$VC_Revision) $NumParms Completed Successfully `date +%Y/%m/%d` (`date +%H:%M:%S`)"
log "----------------------------------------------------------------------------------------"

exit 0


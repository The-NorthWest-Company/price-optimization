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
#                bring price changes from PowerHQ to DT(DW).
#                
#
#       Usage:   hq2dw_dt_price_intf.sh
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

help()
{
   cat << EOF

   Usage is: $Scriptname_var
    The purpose of this script is to call an Oracle package to bring price changes from PowerHQ to DW 
 
EOF
}

log()
{
   echo -e $* | tee -a $log_file
}

terminate()
{
   log "------------------------------------------------------------------------------"
   log " $Scriptname_var(V$VC_Revision) $arguments Terminated Abnormally on `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   log "------------------------------------------------------------------------------"

   exit 1
}


check_if_run()
{
log ". . . . . . . . . . . . . . . . . . ."
log "Check if another similar process is running."
log "my PID $$ ($PPID)"
log "RMS var: $RMS"

# identify my session
mySession="none"
for inst1 in "mmsdevc1" "mmststc1" "mmststc2" "mmsuatc1" "mmsprd"
do
    if echo "$RMS" | grep -i -q "$inst1"; then
       mySession=${inst1}
       break
    fi
done
log "my session is \"$mySession\""

if [ "$mySession" = "none" ] ; then
   log "Couldn't identify process environment:"
   log "RMS=$RMS"
   PCNT=3
   return
fi


# ################################# #
# identify sessions in the memory
# ################################# #

ps -efx|grep "$Scriptname_var" > $RMSTMP/${short_file_name}.pid1
grep -v -e "grep " -e "vi " -e "more " -e "pg " -e "page " -e "tws" -e "crisp" -e "v " -e "v.sh " \
-e "/bin/sh -c /data_links" -e "$$"  -e "$PPID"  $RMSTMP/${short_file_name}.pid1 > $RMSTMP/${short_file_name}.pid2

echo pid1 >> $log_file
cat  $RMSTMP/${short_file_name}.pid1 >> $log_file

while read line1; do 
    log "- - - - -"
    log "verifying session :"
    log "line1=$line1"

    line2=""
    FoundSession="none"
    for inst1 in "mmsdevc1" "mmststc1" "mmststc2" "mmsuatc1" "mmsprd" "MMHOME"
    do
        if echo "$line1" | grep -i -q "$inst1"; then
           if [ "$inst1" = "MMHOME" ] ; then
              if echo "$RMS" | grep -i -q "mmsprd"; then
                 FoundSession="mmsprd"
              else
                 FoundSession="mmsuatc1"
              fi
           else
              FoundSession=${inst1}
           fi
           break
        fi
    done
    
    if [ "$FoundSession" = "none" ] ; then
       log "Couldn't identify process:"
       log "$line1"
       PCNT=4
       return
    else
       if [ "$FoundSession" = "$mySession" ] ; then
          PCNT=2
          log "One more session is runnning in the current environment"
          #email_report
          break
       fi
    fi
done < $RMSTMP/${short_file_name}.pid2

log ". . . . . . . . . . . . . . . . . . ."
}



#------------------------------------------------------------------------------
# Main script begins
#------------------------------------------------------------------------------

arguments="$*"

# set the batch environment
. ~/nwc_setenv.sh ; Scriptname_var=$0

todays_date=`date +%Y%m%d`
todays_date_time=`date +%Y%m%d%H%M`
new_dir=$todays_date
log_file=$RMSLOG/`basename $Scriptname_var .sh`.log
tmp_file=$RMSTMP/`basename $Scriptname_var .sh`.tmp
short_file_name=`basename $Scriptname_var .sh`

# Roll over log file
if [ -f $log_file ] ; then
   cat $log_file >> $log_file.`last_modification $log_file`.bak
   rm  $log_file
fi

#------------------------------------------------------------------------------
# Set command line options
#------------------------------------------------------------------------------

log "---------------------------------------------------------------------------------"
log "$Scriptname_var(V$VC_Revision) $arguments Starting : `date +%Y-%m-%d` (`date +%H:%M:%S`)"
log "---------------------------------------------------------------------------------\n"

NumParms=$#

# Validate the number of arguments passed to the shell script
if [ $NumParms -gt 0 ] ; then
   log "Error: $Scriptname_var(V$VC_Revision) Bad number of arguments ($@)"
   help
   exit 1
fi

# Read the userid, password from the file '.secure.*'
read userid password < ~/.secure.$ORACLE_SID.nwc_oper

# Check if another similar process is running
PCNT=0
check_if_run
log "PCNT=$PCNT"

if [ $PCNT -gt 1 ] ; then
   log "Another running instance of hq2dw_dt_price_intf.sh has been detected."
   if [ $PCNT -eq 2 ] ; then
      cat $RMSTMP/${short_file_name}.pid2 >> $log_file
   fi
   terminate
fi


log "---------------------------------------------------------------------------------"       
log "processing starts : `date +%Y-%m-%d` (`date +%H:%M:%S`)"             
log "---------------------------------------------------------------------------------\n"     


sqlplus -s <<HH >$tmp_file
$userid/$password
set pagesize 0
set heading off
set feedback off
set linesize 2500
set long 2000000000
set trimspool on
set trimout on
set echo off
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK
update HQ_RETEK_CONTROL
   set ERROR_DTM = null
 WHERE RETEK_SUBSCRIBED_DTM IS NOT NULL 
   AND RETEK_PROCESSED_DTM IS NULL
   AND INTERFACE_ID = 'DT_PRICE_INTF'
   AND ERROR_DTM IS NOT NULL
;
commit;
HH

grep -E 'ERROR|ORA-' $tmp_file> /dev/null
RC=$?

if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while script tried to clean ERROR_DTM for last BATCH ID."
   log "POWERHQ_TO_DW.POPULATE_DT_PRICE_INTF_TB is terminated abnormally."
   cat $tmp_file >> $log_file
   terminate
fi




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
   v_i                NUMBER;
BEGIN
   --
   SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
     INTO vSysDate
     FROM DUAL;

   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE(' POWERHQ_TO_DW.POPULATE_DT_PRICE_INTF_TB called at: '|| vSysDate);
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');

   POWERHQ_TO_DW.POPULATE_DT_PRICE_INTF_TB (oERROR_MSG ); 

   SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS')
     INTO vSysDate
     FROM DUAL;

   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE(' POWERHQ_TO_DW.POPULATE_DT_PRICE_INTF_TB completed at: '|| vSysDate);
   DBMS_OUTPUT.PUT_LINE('   with output message:');
   if oERROR_MSG is not null then
      for v_i in 0..(length(oERROR_MSG)/255+1) loop
          DBMS_OUTPUT.PUT_LINE(substr(oERROR_MSG, v_i*255+1, 255));
          exit when (v_i+1)*255+1>length(oERROR_MSG);
      end loop;
   end if;
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

cat $tmp_file >> $log_file
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while staging Power HQ price changes for Retek processing at: `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   terminate
fi 

log "----------------------------------------------------------------------------------------"
log " $Scriptname_var(V$VC_Revision) $arguments Completed Successfully `date +%Y/%m/%d` (`date +%H:%M:%S`)"
log "----------------------------------------------------------------------------------------"

exit 0


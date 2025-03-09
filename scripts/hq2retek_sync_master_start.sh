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
# Description:   This script calls Oracle Package for generating queries for new and updated data from
#                power hq to integration database for further processing to Retek.    
#
#       Usage:   hq2retek_sync_master_start.sh 
#
#  Parameters:   <GroupNo> - Optional Group Number to use for processing
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

   Usage is: $Scriptname_var [-g<GroupNo>]
   This script calls Oracle Package for calling NWCsp_HQ_Retek_Sync_Master procedure on SQL server.

   OPTIONS:
     [-g GroupNo] - call NWCsp_HQ_Retek_Sync_Master with defined "Group Number".
                    If not defined then GroupNo=0

     [-d LastChangeDate] - Last Change Date. If not defined then LastChangeDate=''
     [-r RunNTimes] - Run procedure N times. (default value = 3 000 000)
EOF
}

log()
{
   echo $* | tee -a $log_file
}


email_report()
{
  LINE1="NWCsp_HQ_Retek_Sync_Master has been failed"
  LINE2="\nThanks,\n"
  LINE3="RMS Support Team"
  LINE4="rms@northwest.ca"
  MAILTO=`grep POWERHQ_JOBS_ERROR $RMSDATA/mail.list | cut -f 3- -d ','`
  SUBJECT=`grep POWERHQ_JOBS_ERROR $RMSDATA/mail.list | cut -f 2 -d ','`

  if [[ ! -z "$MAILTO" ]]
  then
     (echo $LINE1; tail -50 $log_file; echo $LINE2; echo $LINE3; echo $LINE4) | mailx -s "$ORACLE_SID $SUBJECT" $MAILTO >> $log_file 2>&1
  else
     log "Mail list for POWERHQ_JOBS_ERROR is not found on `date +%Y/%m/%d` (`date +%H:%M:%S`)"
  fi
}



terminate()
{
   log "------------------------------------------------------------------------------"
   log " $Scriptname_var(V$VC_Revision) $arguments Terminated Abnormally on `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   log "------------------------------------------------------------------------------"

   email_report

   if [ -f $RMSTMP/${pid_file}.pid1 ] ; then
      rm  $RMSTMP/${pid_file}.pid1
   fi

   if [ -f $RMSTMP/${pid_file}.pid2 ] ; then
      rm  $RMSTMP/${pid_file}.pid2
   fi

   exit 1
}


init_sleep()
{
sqlplus -s <<HH >$tmp_file
$userid/$password
set heading off;
set feedback off;
set pagesize 0;
set line 999;
SELECT SUM(NVL(A.CODE_NUMERIC_VALUE,0))
  FROM RTKC_NWC.NWC_CODE_DETAIL A
 WHERE A.CODE_TYPE = 'POWER_HQ_TO_INTDB'
   and A.CODE = 'SYNCMASTER_LOOP24_INTERVAL'
 GROUP BY A.CODE_TYPE;
HH

   # If the above sql finished unsuccessfully then error out the message and exit.
   grep ORA- $tmp_file> /dev/null
   RC="$?"
   if [ $RC -eq 0 ] ; then
      log "Error encountered while retrieving Sleep Times."
      cat $tmp_file >> $log_file
      terminate
   else
      read POWER_HQ_TO_INTDB_SYNCMASTER_INTERVAL < $tmp_file
   fi

   if [ -z "$POWER_HQ_TO_INTDB_SYNCMASTER_INTERVAL" -o "$POWER_HQ_TO_INTDB_SYNCMASTER_INTERVAL" -eq 0 ] ; then
      POWER_HQ_TO_INTDB_SYNCMASTER_INTERVAL=600
   fi

   log "The following Sleep Settings have been defined for this execution of hq2retek_sync_master_start.sh:"
   log " - POWER_HQ_TO_INTDB_SYNCMASTER_INTERVAL=$POWER_HQ_TO_INTDB_SYNCMASTER_INTERVAL"
}



check_if_run()
{
log ". . . . . . . . . . . . . . . . . . ."
log "Check if another similar process is running."
log "my PID $$ ($PPID)"
log "RMS var: $RMS"

# identify my session
mySession="none"
for inst1 in "mmsdevc1" "mmststc1" "mmsuatc1" "mmsprd"
do
    if echo "$RMS" | grep -i -q "$inst1"; then
       mySession=${inst1}_g${GroupNo}
       break
    fi
done
log "my session is \"$mySession\""

if [ "$mySession" = "none" ] ; then
   log "Couldn't identify process environment:"
   log "RMS=$RMS"
   PCNT=2
   return
fi

# ################################# #
# identify sessions in the memory
# ################################# #

ps -efx|grep "hq2retek_sync_master_start.sh" > $RMSTMP/${pid_file}.pid1
grep -v -e "grep " -e "vi " -e "more " -e "pg " -e "page " -e "tws" -e "crisp" -e "v " -e "v.sh " \
-e "/bin/sh -c /data_links" -e "$$"  -e "$PPID"  $RMSTMP/${pid_file}.pid1 > $RMSTMP/${pid_file}.pid2

#-e "/sbin/sh /data_links" -e "$$" 

echo pid1 >> $log_file
cat  $RMSTMP/${pid_file}.pid1 >> $log_file


while read line1; do 
    log "- - - - -"
    log "verifying session :"
    log "line1=$line1"

    line2=""
    FoundSession="none"
    for inst1 in "mmsdevc1" "mmststc1" "mmsuatc1" "mmsprd" "MMHOME"
    do
        if echo "$line1" | grep -i -q "$inst1"; then
           if [ "$inst1" = "MMHOME" ] ; then
              if echo "$RMS" | grep -i -q "mmsprd"; then
                 FoundSession="mmsprd"
              else
                 FoundSession="mmsdevc1"
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
       PCNT=2
       return
    else
       line2=`echo $line1 | sed -n 's/.*\(hq2retek_.*\).*/\1/p'`
       echo $line2 | read -r p1 p2 p3 p4 p5

       #echo "line2=$line2"
       #echo "paresd vals = $p1 / $p2 / $p3 / $p4 / $p5"
       #log "verifying session :"
       #log "$line2"

       reqsubstr="-g"
       for param1 in $p1 $p2 $p3 $p4 $p5
       do
           if [[ ! -z "$param1" ]] then
              if [[ -z "${param1##$reqsubstr*}" ]] ; then
                 FoundSession=`echo ${FoundSession}_$param1`
                 break
              fi
           fi
       done

       FoundSession=`echo $FoundSession | sed -e "s/-//g"`
       log "FoundSession=$FoundSession"

       if [ "$FoundSession" = "$mySession" ] ; then
          PCNT=2
          log "One more session is runnning for the same group and environment"
          #email_report
          break
       fi
    fi
done < $RMSTMP/${pid_file}.pid2

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
pid_file=`basename $Scriptname_var .sh`_$$
mySession=""

#------------------------------------------------------------------------------
# Set command line options
#------------------------------------------------------------------------------

while getopts "hg:d:r:" option
do 
    case $option in
      (h)
         help $Scriptname_var
         exit 1
         ;;
      (g)
	     GroupNo=$OPTARG
         ;;
      (d)
	     LastChangeDate=$OPTARG
         ;;
      (r)
	     RunNTimes=$OPTARG
         ;;
      (?)
         print -u2 "$Scriptname_var: unknown option $OPTARG"
         help $Scriptname_var
         exit 1
         ;;
      (*)
         print -u2 "$Scriptname_var: unknown option $OPTARG"
         help $Scriptname_var
         exit 1
         ;;
    esac
done
shift OPTIND-1

if [ -z "$GroupNo" ] ; then
   GroupNo=0
fi

if [ -z "$LastChangeDate" ] ; then
   LastChangeDate=''
fi

if [ -z "$RunNTimes" ] ; then
   RunNTimes=3000000
fi


log_file=$RMSLOG/`basename $Scriptname_var .sh`_g${GroupNo}_$$.log
tmp_file=$RMSTMP/`basename $Scriptname_var .sh`_g${GroupNo}_$$.tmp


log "---------------------------------------------------------------------------------"
log "$Scriptname_var(V$VC_Revision) $arguments Starting : `date +%Y-%m-%d` (`date +%H:%M:%S`)"
log "---------------------------------------------------------------------------------\n"
log "Script has been started in `echo $TWO_TASK` environment"
log "the following parameters have been specified for this run:"
log "GroupNo=$GroupNo"
log "LastChangeDate=$LastChangeDate"
log "RunNTimes=$RunNTimes"

NumParms=$#

# Validate the number of arguments passed to the shell script
if [ $NumParms -gt 3 ] ; then
   log "Error: $Scriptname_var(V$VC_Revision) Bad number of arguments ($@)"
   help
   email_report
   exit 1
fi


# Check if another similar process is running. Run this function only if GroupNo is set
PCNT=0
check_if_run
#echo PCNT=$PCNT

if [ $PCNT -gt 1 ] ; then
   log "Another running instance of hq2retek_sync_master_start.sh has been detected."
   cat $RMSTMP/${pid_file}.pid2 >> $log_file
   terminate
fi


# define signal file name
#sig_file=`basename $Scriptname_var .sh`_stop_$mySession.sig
sig_file=hq2retek_sync_master_stop_$mySession.sig

if [ -f $RMSCONFIG/${sig_file} ] ; then
   log "$Scriptname_var (V$VC_Revision) $RMSCONFIG/${sig_file} verified `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   rm $RMSCONFIG/${sig_file} 2>> $log_file
fi


# Read the userid, password from the file '.secure.*'
read userid password < ~/.secure.$ORACLE_SID.nwc_oper


# Initialize Sleep Variables
init_sleep

# Loop until processing interval has elapsed
while [ RunNTimes -gt 0 ] # Begin interval loop
do

  # Check for stop file and break outter loop if found
  if [ -f $RMSCONFIG/${sig_file} ] ; then
     log "$Scriptname_var (V$VC_Revision) $RMSCONFIG/${sig_file} verified `date +%Y-%m-%d` (`date +%H:%M:%S`)"
     break
  fi

  log "---------------------------------------------------------------------------------"       
  log "Start running prc_call_sql_procedure('NWCsp_HQ_Retek_Sync_Master') : `date +%Y-%m-%d` (`date +%H:%M:%S`)"             
                                                                                         
                                                                                          
sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on size 1000000
set linesize 999

-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK

DECLARE
   in_procedure_name   varchar2(64);
   in_procedure_params varchar2(512);
   out_msg             varchar2(8000);
   vSysDate            VARCHAR2(50);
   RetCode             NUMBER;
   v_i                 NUMBER;
BEGIN

   SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS') INTO vSysDate FROM DUAL;

   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');
   DBMS_OUTPUT.PUT_LINE('NWCsp_HQ_Retek_Sync_Master called at: '|| vSysDate);
   DBMS_OUTPUT.PUT_LINE('...');

   in_procedure_name := 'dbo.NWCsp_HQ_Retek_Sync_Master';
   in_procedure_params := '@lastChangeDate_in=''$LastChangeDate''~@group_in=$GroupNo~@out_ret_msg=';

   NWC_OPER.prc_call_sql_procedure(in_procedure_name, in_procedure_params, out_msg);

   SELECT TO_CHAR(SYSDATE,'DD-MON-YYYY HH24:MI:SS') INTO vSysDate FROM DUAL;

   DBMS_OUTPUT.PUT_LINE('NWCsp_HQ_Retek_Sync_Master completed at: '|| vSysDate);
   DBMS_OUTPUT.PUT_LINE('   with message > ');
   if out_msg is not null then
      for v_i in 0..(length(out_msg)/255+1) loop
          DBMS_OUTPUT.PUT_LINE(substr(out_msg, v_i*255+1, 255));
          exit when (v_i+1)*255+1>length(out_msg);
      end loop;
   end if;
   DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------------------------------------');

COMMIT;
EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      DBMS_OUTPUT.PUT_LINE('ERROR:');
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,1,255));
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,256,255));
      DBMS_OUTPUT.PUT_LINE(substr('Error encountered running SQLPLUS Script:'||SQLCODE||' - '||SQLERRM,511,255));
END;
/
HH


  grep -E 'ERROR:|ORA-|Error Message=' $tmp_file> /dev/null
  RC=$?
  
  cat $tmp_file >> $log_file
  if [ $RC -eq 0 ] ; then
     log "Oracle error encountered while running prc_call_sql_procedure('NWCsp_HQ_Retek_Sync_Master') at `date +%Y-%m-%d` (`date +%H:%M:%S`)."
     terminate
  fi 

  
  log "... finished at `date +%Y-%m-%d` (`date +%H:%M:%S`)"             
  log "---------------------------------------------------------------------------------\n"     
  
  
  # Check for stop file and break outter loop if found
  if [ -f $RMSCONFIG/${sig_file} ] ; then
      log "$Scriptname_var (V$VC_Revision) $RMSCONFIG/${sig_file} verified `date +%Y-%m-%d` (`date +%H:%M:%S`)"
      break
  fi

  RunNTimes=`expr $RunNTimes - 1`
  echo RunNTimes=$RunNTimes
  if [ $RunNTimes -eq 0 ] ; then
     break
  fi 

  sleep $POWER_HQ_TO_INTDB_SYNCMASTER_INTERVAL
done


if [ -f $RMSCONFIG/${sig_file} ] ; then
   log "delete $RMSCONFIG/${sig_file} signal file"
   rm $RMSCONFIG/${sig_file} 2>> $log_file
fi


if [ -f $tmp_file ] ; then
   log "delete $tmp_file temporary file"
   rm $tmp_file 2>> $log_file
fi


if [ -f $RMSTMP/${pid_file}.pid1 ] ; then
   rm  $RMSTMP/${pid_file}.pid1
fi


if [ -f $RMSTMP/${pid_file}.pid2 ] ; then
   rm  $RMSTMP/${pid_file}.pid2
fi




log "----------------------------------------------------------------------------------------"
log " $Scriptname_var(V$VC_Revision) $arguments Completed Successfully `date +%Y/%m/%d` (`date +%H:%M:%S`)"
log "----------------------------------------------------------------------------------------"

exit 0


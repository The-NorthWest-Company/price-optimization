#!/sbin/sh
###########################################################################
#                                                                         #
#  THIS SOURCE CODE IS UNPUBLISHED, AND IS THE EXCLUSIVE PROPERTY OF NWC  #
#                                                                         #
###########################################################################
#
#   $Revision:
#   $Workfile:
#     $Author:
#   $Revision:
#       $Date:
#    $Modtime:
# Description:   This script is used to get sale data to Demandtech.
#
#       Usage:   dt_sale.sh
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

data_dir=$DWHDATA/demandtec/sale
new_dir=`date +%Y%m%d`

# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------

function Help
{
   echo "Usage is: $1 "
}

log()
{
   echo $* | tee -a $log_file
}

# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------

terminate()
{
   log "------------------------------------------------------------------------------"
   log " $0(V$VC_Revision) $NumParms Terminated Abnormally on `date +%Y-%m-%d` (`date +%H:%M:%S`)"
   log "------------------------------------------------------------------------------"
   exit 1
}

# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------

Get_HQ_SKUs()
{

log "---------------------------------------------------------------------------------"
log "Get_HQ_SKUs function start :  `date +%Y-%m-%d` (`date +%H:%M:%S`)."
log "---------------------------------------------------------------------------------"


sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on size 1000000
set linesize 999

-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK
--
DELETE FROM NWC_DW.HQ_SKUS;
--
INSERT INTO NWC_DW.HQ_SKUS
( VENDOR_ITEM, SKU, ITEM_ID )
  SELECT DISTINCT
         VENDOR_ITEM,
         VENDOR_ITEM,
         ITEM_ID
    FROM NWC_HQPM.VENDOR_ITEM;
--
COMMIT;
--
HH
grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while getting HQ SKUs on : `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   cat $tmp_file
else
   log "---------------------------------------------------------------------------------"
   log "Get_HQ_SKUs function completed :  `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   log "---------------------------------------------------------------------------------"
fi

cat $tmp_file

}

# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------

Get_Sales()
{

log "---------------------------------------------------------------------------------"
log "Get_Sales function start :  `date +%Y-%m-%d` (`date +%H:%M:%S`)."
log "---------------------------------------------------------------------------------"


sqlplus -s <<HH >$tmp_file
$userid/$password
set serveroutput on size 1000000
set linesize 999

-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK
--
DECLARE
  iSTORE        NUMBER(4):= $pSTORE;
  ioERROR_MSG   VARCHAR2(1000);
BEGIN
  --
  NWC_DW.DT_SALE.GET_SALES( iSTORE, ioERROR_MSG);
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
   log "Oracle error encountered while getting sale on : `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   cat $tmp_file
else
   log "---------------------------------------------------------------------------------"
   log "Get_Sales function completed :  `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   log "---------------------------------------------------------------------------------"
fi

}

# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------

Generate_DT_TLOG_File()
{

log "---------------------------------------------------------------------------------"
log "Generate_DT_TLOG_File function start :  `date +%Y-%m-%d` (`date +%H:%M:%S`)."
log "Generating file $DT_TLOG_NCR"
log "---------------------------------------------------------------------------------"

sqlplus -s <<HH >$tmp_file
$userid/$password
SET TERMOUT OFF;
SET UNDERLINE OFF;
SET COLSEP ','
SET FEEDBACK OFF;
SET PAGESIZE 0;
SET LINESIZE 2000;
SET TRIMSPOOL ON;

-- Rollback changes , return error code if SQL error or OS error occurs, exit session
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
WHENEVER OSERROR EXIT FAILURE ROLLBACK
--
SPOOL $DT_TLOG_NCR
--
SELECT  TRANSACTION_SEQUENCE_NUM                    ||'|'||        -- SequenceNum
        REGISTER_ID                                 ||'|'||        -- Register ID
        LOCATION_ID                                 ||'|'||        -- Location Key
        TRANSACTION_ID                              ||'|'||        -- Transaction ID (PK)
        TO_CHAR(TRANSACTION_DATE,'YYYYMMDD')        ||'|'||        -- Transaction Date
        '12:00:00'                                  ||'|'||        -- Transaction Time
        SCAN_TYPE                                   ||'|'||        -- Scan Type
        ITEM_KEY                                    ||'|'||        -- Item Key
        QUANTITY                                    ||'|'||        -- Quantity
        SCAN_UOM                                    ||'|'||        -- Scan UOM
        TOTAL_SCAN_PRICE                            ||'|'||        -- Total Scan Price
        UNIT_REGULAR_PRICE                          ||'|'||        -- Unit Regular Price
        REFERENCE_ID                                ||'|'||        -- Reference ID
        UNIT_LIST_COST                              ||'|'||        -- Unit List Cost
        NULL                                        ||'|'||        -- Not used
        NULL                                        ||'|'||        -- Original Transaction Id
        NULL                                        ||'|'||        -- Not used
        NULL                                        ||'|'||        -- Reserved 1
        NULL                                        ||'|'||        -- Not used
        NULL                                        ||'|'||        -- Reserved 2
        SCANNED_UPC                                 ||'|'||        -- UserField1
        PRIMARY_UPC                                 ||'|'||        -- UserField2
        SKU                                                        -- UserField3
  FROM NWC_DW.DT_SALE_STG
 WHERE RECORD_STATUS = 'C'
   AND SENT_TO_DT_IND = 'N'
ORDER BY TRANSACTION_DATE, LOCATION_ID, SKU, SCAN_TYPE;
--
SPOOL OFF
--
UPDATE NWC_DW.DT_SALE_STG
   SET SENT_TO_DT_IND = 'Y',
       SENT_TO_DT_DTM = SYSDATE
 WHERE SENT_TO_DT_IND = 'N';
--
HH
grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while getting sale on : `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   cat $tmp_file
else
   log "---------------------------------------------------------------------------------"
   log "Generate_DT_TLOG_File function completed :  `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   log "---------------------------------------------------------------------------------"
fi

chmod 777 $DT_TLOG_NCR

}

# ----------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------

Post_DT_TLOG_File()
{

log "---------------------------------------------------------------------------------"
log "Post_DT_TLOG_File function start :  `date +%Y-%m-%d` (`date +%H:%M:%S`)."
log "Upload $DT_TLOG_NCR file into DemandTec SFTP."
log "---------------------------------------------------------------------------------"

echo "FTP Logic Here!"
if [ `ls -1 $DT_TLOG_NCR | wc -l` -ne 0 ] ; then
    sftp -oPort=160 -oIdentityFile=~/.ssh/demandtec.prv dti8031@FileVault.demandtec.com <<TTT
    cd upload
    ls -l
    mput *
    quit
TTT

RC="$?"
if [[ $RC -ne 0 ]] ; then
   log "$DT_TLOG_NCR file upload into DemandTec SFTP failed!"
   terminate
else
   log "---------------------------------------------------------------------------------"
   log "$DT_TLOG_NCR file was uploaded into DemandTec SFTP sucessfully!"
   log "Post_DT_TLOG_File function completed :  `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   log "---------------------------------------------------------------------------------"
   mv $DT_TLOG_NCR $data_dir/backup/$new_dir
fi
fi
}

# ----------------------------------------------------------------------------------------
# Main script begins
# ----------------------------------------------------------------------------------------
# Read the userid, password from the file '.secure.*'
read userid password < ~/.secure.$ORACLE_SID.isoper

#Validate the number of arguments passed to the shell script
if [ $# -gt 1 ] ; then
    log "Error: $0(V$VC_Revision)   Bad number of arguments  ($@)"
    Help $0
    exit 1
fi
NumParms=$#
if [ $NumParms -eq "0" ] ; then 
    pSTORE=NULL
fi
if [ $NumParms -eq "1" ] ; then 
    pSTORE=$1
fi
echo $pSTORE
# Move previous runs log/output file
if [ -f $log_file ] ; then
    cat $log_file >>$log_file.`last_modification $log_file`.bak
    rm  $log_file
fi

# Create Backup and Error Directories
if [ ! -d $data_dir/backup/$new_dir ] ; then
   mkdir $data_dir/backup/$new_dir
fi

if [ ! -d $data_dir/error/$new_dir ] ; then
   mkdir $data_dir/error/$new_dir
fi

log "------------------------------------------------------------------------------\n"
log "$0(V$VC_Revision) $0 Starting : `date +%Y-%m-%d` (`date +%H:%M:%S`)"
log "------------------------------------------------------------------------------\n"

# Get Sale Date from nwc_code_detail

sqlplus -s <<HH >$tmp_file
$userid/$password
set heading off;
set feedback off;
set pagesize 0;
set linesize 999
select to_char((trunc(sysdate) - code_numeric_value), 'YYYYMMDD')
 from nwc_code_detail
where code_type = 'DT_SALE_PICKUP_DAYS';
HH
grep ORA- $tmp_file> /dev/null
RC=$?
if [ $RC -eq 0 ] ; then
   log "Oracle error encountered while getting SALE DATE  on : `date +%Y-%m-%d` (`date +%H:%M:%S`)."
   cat $tmp_file >> $log_file
   cat $tmp_file
else
  read SALE_DATE < $tmp_file
fi

DT_TLOG_NCR=TLOG_NCR_"$SALE_DATE".txt

cd $data_dir

Get_HQ_SKUs

Get_Sales

Generate_DT_TLOG_File

#Post_DT_TLOG_File

log "--------------------------------------------------------------------------"
log " $0(V$VC_Revision) $NumParms Completed Successfully `date +%Y/%m/%d` (`date +%H:%M:%S`)"
log "--------------------------------------------------------------------------"

exit 0

v_dat=`date +"20%y%m%d"`
v_datm=`date +"20%y%m%d%H%M%S"`
filename=`basename "$0"`

ArgNum=$#
ArgOneValue=$1

echo "*********************************************************************\n"

INFORM()
{
echo "INFORM----$filename:$LINENO:$v_datm >" $1
}

ERROR()
{
echo "ERROR-----$filename:$LINENO:$v_datm >" $1
exit
}

DeleteImportedData()
{

INFORM "Deleting Data from TOT_USAGE_DISTRIBUTION_DETAIL for File Name : '$1'"

sqlstrbatchFL=`sqlplus -s $DATABASE<<EOF
DELETE FROM RB_CUSTOM.TOT_USAGE_DISTRIBUTION_DETAIL
WHERE EVENT_FILE_NAME='$1' ;
commit;
exit;
EOF`

}

RemoveControlM()
{

INFORM "Removing Control M Character from File : $1"

dirLog=`dirname $1`

LogFile="$dirLog""/Log.txt"

Command="dos2unix $1 $1 2>$LogFile"

eval $Command

rm $LogFile

}

INFORM "Process Started ..."

INFORM "Database : $DATABASE | Date : $v_datm"

INFORM "Number of Arguments Passed : '$ArgNum' | ArgOneValue : '$ArgOneValue' "

checkDirectory()
{
INFORM "Checking ENV Variable : $1"

dir=$(eval echo \${$1})
len=${#dir} #length of the String checking Variable

if [ $len == 0 ]; then
    ERROR "$1 is not set"
else
    if [ ! -d $dir ]; then
        ERROR "$1 directory does not exist"
    else
        INFORM "$1 has the value: '$dir'"
    fi
fi

}

ListDirectory()
{
INFORM "Listing Directory : '$1'"

        FileList=`find $1/ -type f `

        DirListCount=${#FileList}

        iFile=0

        if [ $DirListCount -ne 0 ]; then

                for FileFullPath in $FileList
                do
                iFile=$((1+$iFile))
                FileName=`basename $FileFullPath`
                INFORM "$iFile | $FileName"
                done
        fi
}


#Checking ENV Variables

checkDirectory DL_INPUT_PATH

checkDirectory DL_CONFIG_PATH

checkDirectory DL_CTRL_PATH

checkDirectory DL_WORK_PATH

checkDirectory DL_BAD_PATH

checkDirectory DL_LOG_PATH

checkDirectory DL_REPORT_PATH

checkDirectory DL_COMPLETE_PATH

configfile="$DL_CONFIG_PATH/dataloader_config.csv"

INFORM "Loading Config Data : $configfile"

if [ ! -f "$configfile" ]; then
    ERROR "$configfile does not exist."
fi

#Work Input and Control Folder

InputWorkDir="$DL_WORK_PATH/InputWork"

if [ ! -d "$InputWorkDir" ]; then
    INFORM "$InputWorkDir does not exist. Creating InputWork folder."
    mkdir -p $InputWorkDir
fi

ctrlWorkDir="$DL_WORK_PATH/ctrlWork"

if [ ! -d "$ctrlWorkDir" ]; then
    INFORM "$ctrlWorkDir does not exist. Creating ctrlWork folder."
    mkdir -p $ctrlWorkDir
fi

ORG_IFS=$IFS

IFS=","


MSNo=1
MainTotalRecordCount=0
MainTotalFileCount=0
MainRejectedFiles=0
StatusText=""

RejectedFiles=0

randomDateTime=`date +"20%y%m%d%H%M%S"`
MainReportFile="$DL_REPORT_PATH/""MainReport_""$randomDateTime"".txt"
v_DateTime=`date +"%d/%m/20%y %H:%M:%S"`

echo -e "-----------------------------------------------------------------------------------------------------------------------\n" >>$MainReportFile
echo -e "Main Report\t\t\t Report Process Date : $v_DateTime \n" >>$MainReportFile
echo -e "-----------------------------------------------------------------------------------------------------------------------\n" >>$MainReportFile
echo -e "SNo\tSource Directory\tFile Name\t\t\t\t\t\t\t\t\tStatus\tRecord Count\tProcess Date\n">>$MainReportFile

INFORM "Main Report File Name : '$MainReportFile'"

#Reading the dataloader_config.csv file

while read ArgValue ListValue HeaderSize FooterSize ctrlFile
do

#If Argument is passed only a specific file Input file will be loaded
#else, All Input files will be loaded
#Argument to be passed is configured in Config file

if [[ $ArgNum -eq 0 ]] || [[ $ArgOneValue == $ArgValue ]]
then


        echo "====================================================================="

        InputWorkFile="$InputWorkDir/""$InputFile"".input"

        INFORM "$ArgValue | $ListValue | $HeaderSize | $FooterSize | $ctrlFile"

        #Checking if Contrl file exists in the path

        ctrlFileFullPath="$DL_CTRL_PATH/$ctrlFile"

        INFORM "CtrlFile Full Path : $ctrlFileFullPath"

        if [ ! -f "$ctrlFileFullPath" ]; then
            ERROR "$ctrlFileFullPath does not exist."
        fi

        INFORM "Listing : $DL_INPUT_PATH | $ListValue"

        InputFileList=`find $DL_INPUT_PATH/ -type f -name $ListValue*`

        InputDirListCount=${#InputFileList}

        IFS=$ORG_IFS

        SNumber=1
        TotalRecordCount=0
        TotalFileCount=0

        SourceDir=""

        HeaderBoo=0

        ReportFile=""

        if [ $InputDirListCount -ne 0 ]; then

                for InputFileFullPath in $InputFileList
                do
                        echo "---------------------------------------------------------------------"

                        dirName=`dirname $InputFileFullPath`

                        mainDir=${dirName##*/}

                        if [ "$SourceDir" != "$mainDir" ]; then

                                INFORM "DIRECTORY : '$dirName' | Main Dir : '$mainDir'"

                        echo "---------------------------------------------------------------------"

                                SourceDir=$mainDir
                                HeaderBoo=1
                                randomDateTime=`date +"20%y%m%d%H%M%S"`
                                ReportFile="$DL_REPORT_PATH/""$ArgValue""_""$SourceDir""_""$ListValue""_""$randomDateTime"".txt"

                        fi

                        if [ $HeaderBoo == 1 ]; then

                                echo -e "-----------------------------------------------------------------------------------------------------------------------\n" >>$ReportFile
                                echo -e "SNo\t\tFile Name\t\t\t\t\tStatus\tRecord Count\tProcess Date\n">>$ReportFile

                                SNumber=1
                                RejectedFiles=0
                                HeaderBoo=0
                                TotalRecordCount=0
                                TotalFileCount=0

                        fi


                        InputFile=`basename $InputFileFullPath`

                        INFORM "Processing FILE : '$InputFile'"

                        INFORM "Report File Name : '$ReportFile'"

                        lineCount=`perl -lne 'END { print $. }' $InputFileFullPath `

                        lineCount=`echo $lineCount | sed -e 's/^[ ]*//'`

                        startLine=$((1+$HeaderSize))

                        endLine=$(($lineCount-$FooterSize))

                        #Removing Header and Footer

                        InputWorkFile="$InputWorkDir/""$InputFile"".input"

                        INFORM "Getting Actual Records startLine : $startLine | endLine : $endLine | lineCount : $lineCount "

                        sedCommand="sed -n -e '$startLine,$endLine""p' $InputFileFullPath > $InputWorkFile"

                        eval $sedCommand

                        ctrlWorkFile="$ctrlWorkDir/$ctrlFile"".$ArgValue"

                        cp $ctrlFileFullPath $ctrlWorkFile

                        #Naming of EventFile for the Table

                        sedCommand="perl -pi -e 's/EVENTFILENAME/$InputFile/g' $ctrlWorkFile"

                        eval $sedCommand

                        #Checking Column Count

                        columnCount=$(awk -F"," '{print NF}' $InputWorkFile | sort | uniq | wc -l | xargs)

                        if [ $columnCount == 1 ]; then

                                INFORM "InputFile : '$InputFile' NOT Corrupt"

                                DeleteImportedData $InputFile

                                #BadFile | LogFile in working folder

                                badFileName="$DL_BAD_PATH/""$InputFile"".bad"

                                logFileName="$DL_LOG_PATH/""$InputFile"".log"

                                #Removing Event from the begining of the file

                                sedCommand="perl -pi -e 's/^Event://g' $InputWorkFile"

                                eval $sedCommand

                                # Counting Actual number of Records in InputFile

                                lineCount=`perl -lne 'END { print $. }' $InputWorkFile `

                                lineCount=`echo $lineCount | sed -e 's/^[ ]*//'`

                                INFORM "Input File No : $TotalFileCount : Name : '$InputFile' Record Count : '$lineCount'"

                                INFORM "Running SQLLDR Process for File : '$InputFile'"

                                sqlldrLog="$DL_LOG_PATH/""sqlldr"".$ArgValue.log.$v_datm"

                                INFORM "Log File : $sqlldrLog"

                                sqlldrCommand="sqlldr $DATABASE control=$ctrlWorkFile log=$logFileName bad=$badFileName data=$InputWorkFile >>$sqlldrLog"

                                echo "$sqlldrCommand\n" >>$sqlldrLog

                                eval $sqlldrCommand

                                StatusText="Imported"

                                movingFile="mv $InputFileFullPath $DL_COMPLETE_PATH/$InputFile"

                                eval $movingFile

                        else

                                StatusText="Rejected"

                                RejectedFiles=$(($RejectedFiles+1))

                                MainRejectedFiles=$(($MainRejectedFiles+1))

                                lineCount=0

                                INFORM "Input File: '$InputFile' is Corrupt "

                                InputCorruptFile="$InputFileFullPath"".corrupt"

                                movingFile="mv $InputFileFullPath $InputCorruptFile"

                                eval $movingFile

                        fi

                        v_DateTime=`date +"%d/%m/20%y %H:%M:%S"`

                        INFORM "$SNumber | $InputFile | $StatusText |$lineCount | $v_DateTime "

                        echo -e "$SNumber\t\t$InputFile\t\t$StatusText\t$lineCount\t\t$v_DateTime\n">>$ReportFile
                        echo -e "$MSNo\t$SourceDir\t\t$InputFile\t\t\t\t\t$StatusText\t$lineCount\t\t$v_DateTime\n">>$MainReportFile

                        SNumber=$(($SNumber+1))
                        MSNo=$(($MSNo+1))
                        TotalFileCount=$((1+$TotalFileCount))
                        MainTotalFileCount=$((1+$MainTotalFileCount))
                        TotalRecordCount=$(($TotalRecordCount+$lineCount))
                        MainTotalRecordCount=$(($lineCount+$MainTotalRecordCount))

                        echo "---------------------------------------------------------------------"

                done

                SuccessFiles=$(($TotalFileCount-$RejectedFiles))

                echo -e "-----------------------------------------------------------------------------------------------------------------------\n" >>$ReportFile

                echo -e "Total Files Processed : $TotalFileCount \t Total Files Successfully Imported : $SuccessFiles \t Total Files Rejected : $RejectedFiles \n">>$ReportFile
                
                echo -e "Total Number of Records : $TotalRecordCount \n">>$ReportFile

                echo -e "-----------------------------------------------------------------------------------------------------------------------\n" >>$ReportFile

                INFORM "Total No of File : $TotalFileCount | Imported : $SuccessFiles | Rejected : $RejectedFiles |Total No of Records : $TotalRecordCount"

                echo "====================================================================="

                removeFileListCount=0

                removeFileList=`ls $InputWorkDir/*`

                removeFileListCount=${#removeFileList}

                INFORM "Input Work Dir :'$InputWorkDir' Total Count : '$removeFileListCount'"

                if [ $removeFileListCount -ne 0 ]; then
                    removeFile="rm ""$InputWorkDir/*"
                    eval $removeFile
                fi

                removeFileListCount=0

                removeFileList=`ls $ctrlWorkDir/*`

                removeFileListCount=${#removeFileList}

                INFORM "Ctrl Work Dir :'$ctrlWorkDir' Total Count : '$removeFileListCount'"

                if [ $removeFileListCount -ne 0 ]; then
                    removeFile="rm ""$ctrlWorkDir/*"
                    eval $removeFile
                fi


        fi

        IFS=","

fi

done < $configfile  # Done Configuration File

echo "====================================================================="

ListDirectory $DL_BAD_PATH

MainSuccessFiles=$(($MainTotalFileCount-$MainRejectedFiles))

echo -e "-----------------------------------------------------------------------------------------------------------------------\n" >>$MainReportFile

echo -e "Total Files Processed : $MainTotalFileCount \t Total Files Successfully Imported : $MainSuccessFiles \t Total Files Rejected : $MainRejectedFiles \n">>$MainReportFile

echo -e "Total Number of Records : $MainTotalRecordCount \n">>$MainReportFile

echo -e "-----------------------------------------------------------------------------------------------------------------------\n" >>$MainReportFile

INFORM "Total Files Processed : $MainTotalFileCount | Successfully : $MainSuccessFiles | Rejected : $MainRejectedFiles"
INFORM "Total Records Processed : $MainTotalRecordCount"

INFORM "Process Ended"

echo "====================================================================="

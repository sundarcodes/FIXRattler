#Set the environment variables

export LT_INPUT_FILES_PATH=$HOME/FIXRattler/inputfiles
export LT_SOURCE_FILES_PATH=$HOME/FIXRattler/sourcecode
export LT_OUTPUT_FILES_PATH=$HOME/FIXRattler/outputfiles/EqFIX
export LT_FIX_LOG_FILES_PATH=$HOME/FIXRattler/logs


if [ $# -ne 1 ]; then
echo "Generate_Load.sh <HostNames from where the load to be run separated by colon>"
echo "Example : Generate_EqLoad.sh sa01xefixpe01:sa01xeoprpe02:sa01xnagiospe01"
exit 1
fi;

hostNamesList=$1

#Clear of all Files in the output Directory

rm -rf $LT_OUTPUT_FILES_PATH/*

#Generate the Load Patterns Files to be used by FIX Rattler
$LT_SOURCE_FILES_PATH/generateEqFIXRattlerConfig.py $LT_INPUT_FILES_PATH/EqTimeLine.txt $LT_INPUT_FILES_PATH/OrderbookInput.txt $LT_INPUT_FILES_PATH/AccountInput.txt $LT_INPUT_FILES_PATH/EqMMR.txt $LT_INPUT_FILES_PATH/NINList.txt $LT_INPUT_FILES_PATH/NINAccMapping.txt $LT_INPUT_FILES_PATH/MurabhaAccFile.txt



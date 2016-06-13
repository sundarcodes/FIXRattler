#Set the environment variables

export LT_INPUT_FILES_PATH=$HOME/FIXRattler/inputfiles
export LT_SOURCE_FILES_PATH=$HOME/FIXRattler/sourcecode
export LT_OUTPUT_FILES_PATH=$HOME/FIXRattler/outputfiles
export LT_FIX_LOG_FILES_PATH=$HOME/FIXRattler/logs


if [ $# -ne 1 ]; then
echo "StartFIXLoad.sh <Start Time>"
echo "Example : StartFIXLoad.sh 16:30:00"
exit 1
fi;

startTime=$1

#Clear of all Files in the output Directory

rm -rf $LT_FIX_LOG_FILES_PATH/*

cd $LT_SOURCE_FILES_PATH

nohup $LT_SOURCE_FILES_PATH/FIXRattler.py $LT_OUTPUT_FILES_PATH/set1/FIXRattlerStart.txt $startTime &


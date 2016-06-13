#!/usr/bin/python

import sys
import os
import datetime
import Utilities

#inputFileDirPath=os.getenv("LT_INPUT_FILES_PATH");
#ProgramsFileDirPath=os.getenv("LT_PROGRAMS_FILES_PATH");
#scriptsFilePath=os.getenv("LT_SCRIPTS_FILES_PATH");


def addEventstoFile(user,eventName,Timing,Board,Priority):
	fp.write(user+",,,"+Timing+",,"+Priority+","+eventName+",,"+Board+",,\n");

#Main starts here

if (len(sys.argv) != 3):
	print("Usage:python SetMarketTiming.py <LoadPatternFile> <Market Preopen Time in HH:MM:SS");
	exit(1);
	
	
preOpenDuration=0;
openDuration=0;
userName="su3"
fp=0;
marketInputFileName=sys.argv[1];
preopenStartTimeStr=sys.argv[2];
fpin=open(marketInputFileName,"r");
for line in fpin:
	strLine=line.strip();
	strLine=strLine.strip('"');
	arr=strLine.split(",");
	marketState=arr[0];
	marketTiming=arr[1];
	if (marketState == "PREOPEN"):
		preOpenDuration=preOpenDuration+int(marketTiming);
	elif (marketState == "OPEN"):
		openDuration=openDuration+int(marketTiming);

inputFileDir=os.getenv("LT_INPUT_FILES_PATH","/home/tesysop/FIXRattler/inputfiles");

outputFileNameAddEvents=inputFileDir+"/"+"AddTrdEvents.csv"
#outputFileNameRemoveEvents=inputFileDir+"/"+"RemoveTrdEvents.csv"
fp=open(outputFileNameAddEvents,"w");
#Schedule the market open to be 2 mins after the start of the scriptsA
timevar=datetime.datetime.strptime(preopenStartTimeStr,"%H:%M:%S");
timediffdelta=timevar-datetime.datetime.now();
preopenDelay=timediffdelta.seconds+1;
timevar=datetime.datetime.now() + datetime.timedelta(0,preopenDelay);
preOpenTime=timevar.strftime("%H%M%S");
timevar=datetime.datetime.now() + datetime.timedelta(0,preOpenDuration+preopenDelay);
openTime=timevar.strftime("%H%M%S");
timevar=datetime.datetime.now() + datetime.timedelta(0,preOpenDuration+preopenDelay+900);
preOpenTimeFI=timevar.strftime("%H%M%S");
timevar=datetime.datetime.now() + datetime.timedelta(0,preOpenDuration+preopenDelay+1800);
openTimeFI=timevar.strftime("%H%M%S");
timevar=datetime.datetime.now() + datetime.timedelta(0,preOpenDuration+openDuration+preopenDelay-1800);
precloseTimeFI=timevar.strftime("%H%M%S");
timevar=datetime.datetime.now() + datetime.timedelta(0,preOpenDuration+openDuration+preopenDelay-900);
closeTimeFI=timevar.strftime("%H%M%S");
timevar=datetime.datetime.now() + datetime.timedelta(0,preOpenDuration+openDuration+preopenDelay);
closeTime=timevar.strftime("%H%M%S");
postTradeDuration=3600
timevar=datetime.datetime.now() + datetime.timedelta(0,preOpenDuration+openDuration+preopenDelay+postTradeDuration);
EndOfDayTime=timevar.strftime("%H%M%S");
EODTimeDuration=300
timevar=datetime.datetime.now() + datetime.timedelta(0,preOpenDuration+openDuration+preopenDelay+postTradeDuration+EODTimeDuration);
EODTime=timevar.strftime("%H%M%S");
EOMTimeDuration=300
timevar=datetime.datetime.now() + datetime.timedelta(0,preOpenDuration+openDuration+preopenDelay+postTradeDuration+EODTimeDuration+EOMTimeDuration);
EOMTime=timevar.strftime("%H%M%S");
EndSessionTimeDuration=300
timevar=datetime.datetime.now() + datetime.timedelta(0,preOpenDuration+openDuration+preopenDelay+postTradeDuration+EODTimeDuration+EOMTimeDuration+EndSessionTimeDuration);
EndSessionTime=timevar.strftime("%H%M%S");

#FI Market Timing

fp.write("Transaction: trdEventAdd\n");
fp.write("user,onbehalfof,eventTime.date,eventTime.time,randomisedInterval,priority,transitionId,boardGroupId,boardId,instrId,secCode\n");

addEventstoFile(userName,"PREOPEN",preOpenTime,"SAEQ","1")
addEventstoFile(userName,"OPEN",openTime,"SAEQ","1")
addEventstoFile(userName,"START_INDEX",openTime,"SAEQ","3")
addEventstoFile(userName,"START_INDEX",openTime,"INDEX","4")
addEventstoFile(userName,"START_INDEX",openTime,"PRIVATE INDEX","5")
addEventstoFile(userName,"PERIODINDIC",openTime,"SAEQ","6")
addEventstoFile(userName,"PREOPEN_DEBT",preOpenTimeFI,"SAFI","2")
addEventstoFile(userName,"OPEN_DEBT",openTimeFI,"SAFI","2")
addEventstoFile(userName,"PERINDCLOSE",closeTime,"SAEQ","1")
addEventstoFile(userName,"CLOSE_INDEX",closeTime,"INDEX","2")
addEventstoFile(userName,"CLOSE_INDEX",closeTime,"PRIVATE INDEX","3")
addEventstoFile(userName,"ENDOFTRADES",closeTime,"","4")
addEventstoFile(userName,"POSTTRADE",closeTime,"","5")
addEventstoFile(userName,"POSTTRADE",closeTime,"SAEQ","7")
addEventstoFile(userName,"POSTTRD_DEBT",closeTime,"SAFI","8")
addEventstoFile(userName,"ENDOFDAY",EndOfDayTime,"SAEQ","2")
addEventstoFile(userName,"ENDOFDAY",EndOfDayTime,"SAFI","3")
addEventstoFile(userName,"ENDOFDAY",EndOfDayTime,"INDEX","4")
addEventstoFile(userName,"ENDOFDAY",EndOfDayTime,"PRIVATE INDEX","5")
addEventstoFile(userName,"ENDOFDAY",EndOfDayTime,"","6")
addEventstoFile(userName,"IPXS-EOD",EODTime,"","1")
addEventstoFile(userName,"IPXS-EOM",EOMTime,"","1")
addEventstoFile(userName,"IPXS-ENDSESS",EndSessionTime,"","1")
		
fp.close()


#Run the qtestcsv command to Remove the Trade Events


#Run the qtestcsv command to Add the Trade Events

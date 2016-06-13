#!/usr/bin/python

import sys
import os
import Utilities
import random

#main starts here
if (len(sys.argv)!=8):
        print "Usage:generateEqFIXRattlerConfig.py <InputConfigFile> <OrderbookFile> <AccFile> <MemberOrderRatioFile> <NINFile> <NINAccFile> <MurabhaNINFile>"
        exit(1);

#Get the File Name
infileName=sys.argv[1]
#fixinfoFile=sys.argv[2]
orderBookInFile=sys.argv[2]
accInFile=sys.argv[3]
mbrOrdRatioFile=sys.argv[4]
ninFile=sys.argv[5];
ninAccFile=sys.argv[6];
murabhaNinFile=sys.argv[7];
#fixHostname=os.popen("hostname").read().strip();

#Read the environment variables to get the path
inputDirPath=Utilities.getEnvValue("LT_INPUT_FILES_PATH");
outputDirPath=Utilities.getEnvValue("LT_OUTPUT_FILES_PATH");
programDirPath=Utilities.getEnvValue("LT_PROGRAMS_FILES_PATH");
#maxDriversPerFIXSlammerConfigFile=10

port="";
mbrCode="";
fixtraderId=""
traderId=""
mbrArr={}
arrIndex=0;
superUser="su4"
superUser1="su5"
#superUser="perf1"
#superUser1="perf2"
smallMembersCount=0;
heavyMemberOrderRatioTotal=0;

# Read from this FIX INfo file and store partipant trader id,port number,member code
fp=open(mbrOrdRatioFile,"r");
for line in fp:
        strLine=line.strip('"\n');
        if (line.startswith("#") or line.find("userid") != -1 or line.find("-+") != -1 or line.isspace() ):
                continue
        arr=strLine.split(",")
        mbrCode=arr[0].strip();
        fixtraderId=arr[1].strip();
        #Change it to the Trader ID
        traderId=fixtraderId.replace("F","T");
        port=arr[2].strip();
        fixserverIP=arr[3].strip();
        mbrOrderRatio=arr[4].strip();
        if (mbrOrderRatio == "0"):
                continue;
        if (mbrOrderRatio == "EQ"):
                smallMembersCount=smallMembersCount+1;
                mbrOrderRatio="";
        else:
                heavyMemberOrderRatioTotal=heavyMemberOrderRatioTotal+int(mbrOrderRatio);

        #Get one buy and 1 sell Acc from Account Input file for this member
        cmd="grep \""+mbrCode+",\" " + accInFile+" | head -200 | cut -d ',' -f2";
        #print cmd
        accArr=os.popen(cmd).read().strip("\n").split("\n");
        #print "Done with storing fix related info for member :"+str(arrIndex);
        #buyAcc=out[0].strip();
        #sellAcc=out[1].strip();
        #accArr=out[0:];
        fixtuple=tuple([mbrCode,traderId,port,fixserverIP,accArr,mbrOrderRatio]);
        mbrArr[arrIndex]=fixtuple
        arrIndex=arrIndex+1;

totMembersAvailable=arrIndex
#print "Done with Getting FIX Info"
#for element in mbrarr:
#       print mbrarr[element][2];

#Read all the available order books along with its reference price and store it
orderBook=""
refPrice=""
ordArr={};
orderBookCount=0;

fp=open(orderBookInFile,"r");
for line in fp:
        strLine=line.strip();
        strLine=strLine.strip('"');
        if (line.startswith("#") or line.isspace() ):
                continue
        arr=strLine.split(":")
        orderBook=arr[0].strip();
        refPrice=arr[1].strip();
        orderRatio=arr[2].strip();
        ordtuple=tuple([orderBook,refPrice,orderRatio]);
        ordArr[orderBookCount]=ordtuple
        orderBookCount=orderBookCount+1;

#print "Done with Getting Orderbooks"
#Open a File for adding the time and and respective FIXSlammerConfig File
FIXSlammerStartFile=outputDirPath+"/"+"FIXRattlerstart.txt"
fixslammerFP=open(FIXSlammerStartFile,"w");

#This member belongs to the others category
#Get the order rate for the "Other" Member category from the Member Order Ratio File
#Distribute the Load % evenly across these small members
smallMbrsOrderRatio=100-heavyMemberOrderRatioTotal;
if (smallMembersCount != 0):
        orderPercentOthers=float(smallMbrsOrderRatio)/smallMembersCount;
else:
        orderPercentOthers=0;

#Load the Active NIN from the NIN file
ninList=[];
fpNIN=open(ninFile,"r");
for nin in fpNIN:
	ninList.append(nin.strip('"\n'));
totalNINCount=len(ninList);

#Read through the Main Parameter File to generate FIXSlammerConfig File for each run
timeToRun=0;
totMembers=0;
orderRatePerMem=0.0;
totOrdersForThisRun=0
percentU10=0.0;
percentU12=0.0;
percentU14=0.0;
percentU16=0.0;
percentU18=0.0;
percentU20=0.0;
percentU22=0.0;
#percentOrderbook=0.0;
actualTotalOrdersForthisRun=0;
totalOrdersForEntireRun=0;
totalTimeForEntireRun=0;

#Time in minutes,Total Members,Order Rate Per Member,Trade %,Cancel %,Order book %,NumberofMarketOrders
count=0;
fp=open(infileName,"r");
for line in fp:
        strLine=line.strip();
        strLine=strLine.strip('"');
        if (line.startswith("#")):
                continue
        count=count+1;
        arr=strLine.split(",")
        timeToRun=int(arr[1]);
        totOrdersForThisRun=int(arr[2]);
        percentU10=float(arr[3]);
        percentU12=float(arr[4]);
        percentU14=float(arr[5]);
        percentU16=float(arr[6]);
        percentU18=float(arr[7]);
        percentU20=float(arr[8]);
        percentU22=float(arr[9]);

        #Prepare the input file required for FIXSlammerConfigGeneration.py
        index=0;
        fixslammerinputfilesarr={}
        fixslammerinputpatternfile=outputDirPath+"/"+"fixrattlerinput.txt";
        fp=open(fixslammerinputpatternfile,"w");
        for i in mbrArr:
                mbrCode=mbrArr[i][0];
                traderID=mbrArr[i][1];
                fixPort=mbrArr[i][2];
                fixHostname=mbrArr[i][3];
                accArr=mbrArr[i][4];
                orderPercent=mbrArr[i][5];
                if (orderPercent == ""):
                        orderPercent=orderPercentOthers;

                #Computer the order rate for this member
                totalOrderforthisMember=float(orderPercent)*totOrdersForThisRun*0.01;
                #print totalOrderforthisMember
                orderRatePerMem=round(totalOrderforthisMember/timeToRun,2);
                #print mbrCode,orderPercent,totalOrderforthisMember,timeToRun,orderRatePerMem
                #print mbrCode,int(orderRatePerMem)
                actualOrdersgenbythismem=orderRatePerMem*timeToRun;
                actualTotalOrdersForthisRun=actualTotalOrdersForthisRun+actualOrdersgenbythismem;
                total_num_elements=len(accArr);
                buyAccIndex=int(total_num_elements/2);
                buyAcc=accArr[0].strip('"\n');
                for i in list(range(1,buyAccIndex)):
                        buyAcc=buyAcc+","+accArr[i].strip('"\n');
                sellAccIndex=buyAccIndex+1;
                sellAcc=accArr[sellAccIndex].strip('"\n');
                for i in list(range(sellAccIndex+1,total_num_elements-1)):
                        sellAcc=sellAcc+","+accArr[i].strip('"\n');

                #Get random 5 NINs from NIN List
		ninStr="";
		for i in list(range(0,5)):
			randomNin=ninList[random.randint(0,totalNINCount-1)];
			if (ninStr == ""):
				ninStr=randomNin;
			else:
				ninStr=ninStr+","+randomNin;	

                #Get the NINAcc List
                cmdStr="grep \""+mbrCode+";\" "+ninAccFile;
                tempList=os.popen(cmdStr).read().strip('"').split('\n');
		ninAccStr="";
		for line in tempList:
			strLine=line.strip('"');
			if (strLine == ''):
				continue;
                	ninAccList=strLine.split(';');
			if (ninAccStr == ""):
				ninAccStr=ninAccList[1];
			else:
                		ninAccStr=ninAccStr+","+ninAccList[1];

                #Get the MurabhaAcc List
                cmdStr="grep \""+mbrCode+";\" "+murabhaNinFile;
                murabhaNinAccList=os.popen(cmdStr).read().strip('\n"').split(';');
		if (murabhaNinAccList[0]==''):
			continue;
                murabhaAccStr=murabhaNinAccList[1];

                fp.write(fixHostname+":"+fixPort+":"+mbrCode+":"+traderID+":"+buyAcc+":"+sellAcc+":"+str(orderRatePerMem)+":"+str(percentU10)+":"+str(percentU12)+":"+str(percentU14)+":"+str(percentU16)+":"+str(percentU18)+":"+str(percentU20)+":"+str(percentU22)+":"+ninStr+":"+murabhaAccStr+":"+ninAccStr+":true\n");

        fp.close()

        #print "Done with preparing fixslammer input file for Run:"+str(count)
        #Now generate the FIXSlammerConfig Files
        fixslammerFileName=outputDirPath+"/"+"FIXRattlerEqRun"+str(count)+".xml";
        cmdName=programDirPath+"/EqFIXRattlerConfigGeneration.py --InputFile "+ fixslammerinputpatternfile+" --InputOrderbookFile "+orderBookInFile+" --OutFile "+fixslammerFileName;
        os.system(cmdName);
        fixslammerFileName=os.path.abspath(fixslammerFileName);


        actualTotalOrdersForthisRun=actualTotalOrdersForthisRun;
        print "\nTotal Messages for Run "+str(count)+" could be:"+str(actualTotalOrdersForthisRun);
        print "Probable Message Rate for Run "+str(count)+" could be:"+str(round(actualTotalOrdersForthisRun/timeToRun,2));
        totalOrdersForEntireRun=totalOrdersForEntireRun+actualTotalOrdersForthisRun;
        totalTimeForEntireRun=totalTimeForEntireRun+timeToRun
        actualTotalOrdersForthisRun=0
        strToWritetoFile=str(timeToRun);
        strToWritetoFile=strToWritetoFile+","+fixslammerFileName;
        fixslammerFP.write(strToWritetoFile+"\n");

print "\nTotal Messages For the Entire Run could be :"+str(totalOrdersForEntireRun)
print "Probable Average Message Rate for Entire Run could be:"+str(round(totalOrdersForEntireRun/totalTimeForEntireRun,2));

#Close all Files
fp.close()
fixslammerFP.close()


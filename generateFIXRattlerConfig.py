#!/usr/bin/python
import sys
import os
import Utilities
import random

#main starts here
if (len(sys.argv)!=6):
        print "generateFIXRattlerConfig.py <InputConfigFile> <OrderbookFile> <AccFile> <MemberInfoFile> <hostNameList separated by colon>"
        exit(1);

#Get the File Name
infileName=sys.argv[1]
#fixinfoFile=sys.argv[2]
orderBookInFile=sys.argv[2]
accInFile=sys.argv[3]
mbrOrdRatioFile=sys.argv[4]
hostNameStr=sys.argv[5];
hostNameList=hostNameStr.split(':');
numberOfSets=len(hostNameList);
if (numberOfSets==0):
   hostNameList.append(hostNameStr);
   numberOfSets=1
hostname=os.uname()[1];
#fixHostname=os.popen("hostname").read().strip();
#fixHostname=sys.argv[5]

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
        fixhostname=arr[3].strip();
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
        fixtuple=tuple([mbrCode,traderId,port,accArr,mbrOrderRatio,fixhostname]);
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
#Open a File for adding the time and and respective FIXRattlerConfig File
FIXRattlerStartFileList=[];
for i in list(range(int(numberOfSets))):
        #Create directory if it doest not exist
        try:
          dirName=outputDirPath+"/set"+str(i+1);
          os.makedirs(dirName);
        except OSError:
          pass;
        FIXRattlerStartFile=dirName+"/FIXRattlerStart.txt";
        fixRattlerFP=open(FIXRattlerStartFile,"w");
        FIXRattlerStartFileList.append(fixRattlerFP);

#This member belongs to the others category
#Get the order rate for the "Other" Member category from the Member Order Ratio File
#Distribute the Load % evenly across these small members
smallMbrsOrderRatio=100-heavyMemberOrderRatioTotal;
if (smallMembersCount != 0):
        orderPercentOthers=float(smallMbrsOrderRatio)/smallMembersCount;
else:
        orderPercentOthers=0;

#Read through the Main Parameter File to generate FIXSlammerConfig File for each run
timeToRun=0;
totMembers=0;
orderRatePerMem=0.0;
totOrdersForThisRun=0
percentTrade=0.0;
percentCancel=0.0;
percentMarket=0.0;
percentHidden=0.0;
percentModify=0.0;
percentGTD=0.0;
percentExpiry=0.0;
percentReject=0.0;
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
        percentTrade=arr[3];
        percentCancel=arr[4];
        percentMarket=arr[5];
        percentHidden=arr[6];
        percentModify=arr[7];
        percentGTD=arr[8];
        percentExpiry=arr[9];
        percentReject=arr[10];

        #Prepare the input file required for FIXSlammerConfigGeneration.py
        index=0;
        FIXRattlerInputFileList=[];
        FIXRattlerInputFileNameList=[];
        for i in list(range(int(numberOfSets))):
          dirName=outputDirPath+"/set"+str(i+1);
          FIXRattlerInputFile=dirName+"/FIXRattlerConfigInput.txt"
          fixRattlerFP=open(FIXRattlerInputFile,"w");
          FIXRattlerInputFileList.append(fixRattlerFP);
          FIXRattlerInputFileNameList.append(FIXRattlerInputFile);
        for index in mbrArr:
                mbrCode=mbrArr[index][0];
                traderID=mbrArr[index][1];
                fixPort=mbrArr[index][2];
                accArr=mbrArr[index][3];
                orderPercent=mbrArr[index][4];
		fixHostname=mbrArr[index][5];
                if (orderPercent == ""):
                        orderPercent=orderPercentOthers;

                #Computer the order rate for this member
                totalOrderforthisMember=float(orderPercent)*totOrdersForThisRun*0.01;
                #print totalOrderforthisMember
                orderRatePerMem=round(totalOrderforthisMember/timeToRun,2);
                #print mbrCode,orderPercent,totalOrderforthisMember,timeToRun,orderRatePerMem
                #print mbrCode,int(orderRatePerMem)
                actualOrdersgenbythismem=int(orderRatePerMem*timeToRun);
                actualTotalOrdersForthisRun=actualTotalOrdersForthisRun+actualOrdersgenbythismem;
                batchrate=orderRatePerMem;
                #quantityMultiplier=random.choice([10,10,10,20,50]);
                total_num_elements=len(accArr);
                buyAccIndex=int(total_num_elements/2);
                buyAcc=accArr[0].strip('"\n');
                for i in list(range(1,buyAccIndex)):
                        buyAcc=buyAcc+","+accArr[i].strip('"\n');
                sellAccIndex=buyAccIndex+1;
                sellAcc=accArr[sellAccIndex].strip('"\n');
                for i in list(range(sellAccIndex+1,total_num_elements-1)):
                        sellAcc=sellAcc+","+accArr[i].strip('"\n');

                #fp.write(fixHostname+","+fixPort+","+mbrCode+","+traderID+","+buyAcc+","+sellAcc+","+percentCancel+","+str(int(orderRatePerMem))+","+str(batchrate)+","+percentTrade+","+timeInForce+",false,true,"+str(quantityMultiplier)+"\n");
                fileIndex=index%numberOfSets;
                fp=FIXRattlerInputFileList[fileIndex];
                fp.write(fixHostname+":"+fixPort+":"+mbrCode+":"+traderID+":"+buyAcc+":"+sellAcc+":"+percentCancel+":"+str(orderRatePerMem)+":"+str(batchrate)+":"+percentTrade+":"+percentMarket+":"+percentHidden+":"+percentModify+":"+percentGTD+":"+percentExpiry+":"+percentReject+":false:true:\n");

        for file in FIXRattlerInputFileList:
           file.close()

        #print "Done with preparing fixslammer input file for Run:"+str(count)
        #Now generate the FIXSlammerConfig Files
        for i in list(range(numberOfSets)):
           fixRattlerXMLFileName=outputDirPath+"/set"+str(i+1)+"/FIXRattlerRun"+str(count)+".xml";
           cmdName=programDirPath+"/FIXRattlerConfigGeneration.py --InputFile "+ FIXRattlerInputFileNameList[i]+" --InputOrderbookFile "+orderBookInFile+" --OutFile "+fixRattlerXMLFileName;
           os.system(cmdName);
           fixRattlerXMLFileName=os.path.abspath(fixRattlerXMLFileName);
           strToWritetoFile=str(timeToRun);
           strToWritetoFile=strToWritetoFile+","+fixRattlerXMLFileName;
           FIXRattlerStartFileList[i].write(strToWritetoFile+"\n");


        actualTotalOrdersForthisRun=actualTotalOrdersForthisRun;
        print "\nTotal Orders for Run "+str(count)+" could be:"+str(actualTotalOrdersForthisRun);
        print "Probable Order Rate for Run "+str(count)+" could be:"+str(round(actualTotalOrdersForthisRun/timeToRun,2));
        totalOrdersForEntireRun=totalOrdersForEntireRun+actualTotalOrdersForthisRun;
        totalTimeForEntireRun=totalTimeForEntireRun+timeToRun
        actualTotalOrdersForthisRun=0

print "\nTotal Orders For the Entire Run could be :"+str(totalOrdersForEntireRun)
print "Probable Average Order Rate for Entire Run could be:"+str(round(totalOrdersForEntireRun/totalTimeForEntireRun,2));

#Close all Files
for file in FIXRattlerStartFileList:
       file.close()


#Ship all the files to the destination servers

#for i in list(range(numberOfSets)):
#   if (hostname == hostNameList[i]):
#      continue;
#   print("Shipping files to "+hostNameList[i]);
#   cmd="ssh -q "+hostNameList[i]+" mkdir -p "+outputDirPath+"/set"+str(i+1);
#   os.system(cmd);
#   cmd="scp -q "+outputDirPath+"/set"+str(i+1)+"/* "+hostNameList[i]+":"+outputDirPath+"/set"+str(i+1)+"/.";
#   #cmd="scp -q "+outputDirPath+"/set"+str(i+1)+"/* "+hostNameList[i]+":"+outputDirPath+"/.";
#   os.system(cmd);

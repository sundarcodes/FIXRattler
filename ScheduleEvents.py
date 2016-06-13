#!/usr/bin/python
import sys
import sched
import time
import os;



def jobOpenOrders(outFile):
   cmd=". $HOME/.setenviron ; $ASTSDIR/bin/astsrun smdump -u opr2 -orders | grep \" P \" >> "+outFile;
   os.system(cmd);

def jobPerf(outFile):
   cmd=". $HOME/.setenviron ; $ASTSDIR/bin/astsrun Performance -u opr2 >> "+outFile;
   os.system(cmd);

def jobTables(outFile):
   cmd=". $HOME/.setenviron ; $ASTSDIR/bin/astsrun Tables -u opr2 >> "+outFile;
   os.system(cmd);

def jobOrders(outFile):
   cmd=". $HOME/.setenviron ; $ASTSDIR/bin/astsrun smdump -u opr2 -orders | grep \" O \" >> "+outFile;
   os.system(cmd);

def jobBiggestOrderbook(outFile):
   cmd=". $HOME/.setenviron ; $ASTSDIR/bin/astsrun smdump -u opr2 -orders | grep \" O \" | grep \"SAEQ                1010\" >> " +outFile;
   os.system(cmd);

def jobBiggestParticpant(outFile):
   cmd=". $HOME/.setenviron ; $ASTSDIR/bin/astsrun smdump -u opr2 -orders | grep \" O \" | grep \"T001\" >> "+outFile;
   os.system(cmd);

def jobKillPerformance():
   cmd="pkill -u tesysop -f \"Performance\"";
   os.system(cmd);

def jobCommon(cmdLine):
   cmd=cmdLine;
   os.system(cmd);

def runPerf(hostname,engineName,outFile):
   cmd="ssh -q "+hostname+" \"Performance -u "+engineName+"\">> "+outFile;
   os.system(cmd);


#Main Starts here
if __name__ == "__main__":

 if (len(sys.argv)!=2):
   print("Usage:ScheduleEvents.py <preopen time in HH:MM:SS>");
   sys.exit(1);

 serverMap={};
 serverMap["sa01xefixpe01"]="ipxsfix1"; 
 serverMap["sa01xefixpe02"]="ipxsfix2"; 
 serverMap["sa01xewspe01"]="tsmr1"; 
 serverMap["sa01xewspe02"]="tsmr2"; 
 serverMap["sa01xetippe01"]="ipxstip1"; 
 serverMap["sa01xetippe02"]="ipxstip2"; 
 serverMap["sa01xeoprpe01"]="opr1"; 
 serverMap["sa01xeoprpe01"]="ipxssmarts1"; 
 serverMap["sa01xeoprpe02"]="ipxssmarts2"; 
 serverMap["sa01xefixpe01"]="ipxsrdb1"; 
 serverMap["sa01xefixpe02"]="ipxsrdb2"; 
 serverMap["sa01xetippe01"]="ipxsequator1"; 
 serverMap["sa01xetippe02"]="ipxsequator2"; 

 #Get the Home Directory
 homedir=os.environ['HOME'];

 #Create a directory in the measurement Area
 dateStr=time.strftime("%Y%m%d",time.localtime());
 outputDirName=homedir+"/measurement/"+dateStr;
 cmd="mkdir -p "+outputDirName;
 os.system(cmd);

 timeArg=sys.argv[1];
 timeToPreOpen=dateStr + " " +timeArg;
 s=sched.scheduler(time.time,time.sleep);
 preopenTime=time.mktime(time.strptime(timeToPreOpen,"%Y%m%d %H:%M:%S"));
 #preopenTime=time.strptime(timeToPreOpen,"%Y%m%d %H:%M:%S");
 #print preopenTime
 #print time.strftime("%H:%M:%S",time.localtime(preopenTime));
 openTime=preopenTime+3600;
 #print time.strftime("%Y%m%d %H:%M:%S",time.localtime(openTime));
 postTradeTime=openTime+16200;
 outFile=outputDirName+"/openOrders.txt"
 s.enterabs(preopenTime-(1*60),1,jobOpenOrders,(outFile,));
 outFile=outputDirName+"/preopenPerformance.txt"
 s.enterabs(preopenTime,1,jobPerf,(outFile,));

 #Scheduling for Open

 s.enterabs(openTime-(2*60),1,jobKillPerformance,());
 outFile=outputDirName+"/openPerformance.txt"
 s.enterabs(openTime-(1*60),1,jobPerf,(outFile,));

 #Run the performance utility across all servers
 priority=2;
 for key in list(serverMap.keys()):
   hostname=key;
   engineName=serverMap[key];
   outFile=outputDirName+"/"+engineName+".txt";
   s.enterabs(openTime-(1*60),priority,runPerf,(hostname,engineName,outFile,));
   priority=priority+1;
    

 outFile=outputDirName+"/preopenTables.txt"
 s.enterabs(openTime,1,jobTables,(outFile,));
 outFile=outputDirName+"/preopenOpenOrders.txt"
 s.enterabs(openTime,2,jobOrders,(outFile,));
 outFile=outputDirName+"/preopenOpenBookOrders1.txt"
 s.enterabs(openTime,3,jobBiggestOrderbook,(outFile,));
 outFile=outputDirName+"/preopenOpenPartOrders1.txt"
 s.enterabs(openTime,4,jobBiggestParticpant,(outFile,));
 outFile=outputDirName+"/openingTables.txt"
 s.enterabs(openTime+60,1,jobTables,(outFile,));
 cmdLine=". $HOME/.setenviron ; "+outputDirName+"/price.sh";
 s.enterabs(openTime+60,2,jobCommon,(cmdLine,));

 #Scheduling for Post Trade

 s.enterabs(postTradeTime,1,jobKillPerformance,());
 cmd=". $HOME/.setenviron ; "+outputDirName+"/price.sh";
 s.enterabs(postTradeTime,2,jobCommon,(cmd,));
 outFile=outputDirName+"/openTables.txt"
 s.enterabs(postTradeTime,3,jobTables,(outFile,));
 outFile=outputDirName+"/openOpenOrders.txt"
 s.enterabs(postTradeTime,4,jobOrders,(outFile,));
 outFile=outputDirName+"/openOpenBookOrders1.txt"
 s.enterabs(postTradeTime,5,jobBiggestOrderbook,(outFile,));
 outFile=outputDirName+"/openOpenPartOrders1.txt"
 s.enterabs(postTradeTime,6,jobBiggestParticpant,(outFile,));

 #Scheduling for EndOfDay
 endOfDayTime=postTradeTime+4000;

 s.run();
 time.sleep(3600+16200+4500);


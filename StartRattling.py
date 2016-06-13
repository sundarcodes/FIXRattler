#!/usr/bin/env python

import os;
import sys;
import subprocess;
import time;
import datetime;

if (len(sys.argv)!=3):
        print "Usage:StartRattling.py <InputFile> <StartTime in HH:MM:SS>"
        exit(1)

#Open the input File
fileIn=sys.argv[1];
startTimeStr=sys.argv[2];

# If "now", start the FIX load immediately

if (startTimeStr.lower() != "now"):
        # Just sleep till the start time has come
        timevar=datetime.datetime.strptime(startTimeStr,"%H:%M:%S");
        timediff=timevar-datetime.datetime.now();
        timetoSleep=timediff.seconds;
        print("Sleeping till "+startTimeStr);
        time.sleep(timetoSleep);


# Now start working..
fp=open(fileIn);

fpstdout=open("FIXRattler.log","w");
timetoRun=0;
for line in fp:
        str1=line.strip();
        arr=str1.split(",")
        fixRattlerConfigFile=arr[1];
        timetoRun=int(arr[0]);
        #Start the FIXRattler Process
        proc=subprocess.Popen(['./FIXRattler.py',fixRattlerConfigFile],stdout=fpstdout,stderr=fpstdout)
        id=proc.pid
        #strpid=str(id)
        if (proc.returncode != None):
                print"Starting FIX Rattler failed for run:"+fixSlammerConfigFile;
                exit(1)
        print "FIX Rattler for Run :"+fixRattlerConfigFile+" started at "+time.strftime("%H:%M:%S")
        #Get the process id of fix rattler java process
        pid=os.popen("ps xu | grep python | grep FIXRattler\.py | grep -v grep | awk BEGIN'{FD=\"\"}{print$2}'").read().strip('\n');
	print(pid)

        time.sleep(timetoRun);
        #proc.terminate()
        #proc.kill();
        #Send SIGINT to Rattler pid
        os.kill(int(pid),2);
        print "FIX Rattler for Run :"+fixRattlerConfigFile+" stopped at "+time.strftime("%H:%M:%S")
        time.sleep(3);
        #print"FIX Slammer with process id:"+strpid+" killed"


#End of the run, run the FIX Stats
dt=datetime.datetime.now();
dirname=dt.strftime("%Y%m%d_%H_%M")
cmd="mkdir -p oldlogs/"+dirname;
os.system(cmd);
FIXSlammerStatfileName="./oldlogs/"+dirname+"/FIXRattlerStats.txt";
fpout=open(FIXSlammerStatfileName,"w");
cmd="grep 35=D FIX*.log | wc -l";
TotalOrders=os.popen(cmd).read().strip();
str="Total Orders Sent by FIXSlammer for all members:"+TotalOrders;
fpout.write(str+"\n");
cmd="grep 39=8 FIX*.log | wc -l";
TotalOrdersRejected=os.popen(cmd).read().strip();
str="Total Orders Rejected for all members:"+TotalOrdersRejected;
fpout.write(str+"\n");

cmd="grep 39=8 FIX*.log | cut -d '|' -f 38 | cut -d ':' -f2 | sort -u > RejectedReasons.txt";
os.system(cmd);

cmd="mv FIX*.log RejectedReasons.txt oldlogs/"+dirname;
os.system(cmd);

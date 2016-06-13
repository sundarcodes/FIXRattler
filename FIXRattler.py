#!/usr/bin/env python

import sys;
from XMLReader import XMLReader
from Driver import Driver
from EquatorDriver import EqDriver
import threading
import signal
import time
import datetime
import logging
import os


def getLogObject():
    dirName=os.getenv("LT_FIX_LOG_FILES_PATH","/home/tesysop/FIXRattler/logs");
    LogFileName=dirName+"/FIXRattler.log";
    logger=logging.getLogger("FIXRattler");
    logger.setLevel(logging.DEBUG);
    fh = logging.FileHandler(LogFileName);
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter);
    logger.addHandler(fh);
    return logger; 


#Main Starts here
def main(filename):

        #Global Thread List
        threads=[];
        signalReceived=False;
        symList=[];
        driversList=[];
        driverObjects=[];
        threadName="Main"

        logObj=getLogObject();

        def printMsg(msg):
           #fmtString=str(datetime.datetime.now())+"\t"+threadName+"\t"+msg;
           logObj.info(msg);

        def signalhandler(signum,frame):
           printMsg("Signal Received:"+str(signum));
           signalReceived=True;
           for driver in driverObjects:
              driver.stopRunning();
           del driverObjects[:];


        #Add signal Handler
        signal.signal(signal.SIGINT,signalhandler);
        

        fileList=[]; 
        fileFp=open(filename,"r");
        for line in fileFp:
          fileList.append(line);

        # Read from the XML File
        tmpList=fileList[0].split(',');
        filename=tmpList[1].strip('\n');
        xmlReaderobj = XMLReader(filename);
        symList=xmlReaderobj.getSymbolList();
        driversList=xmlReaderobj.getDriversList();
        driverType=xmlReaderobj.getDriverType();

        for driver in driversList:
            if (driverType == "XStream"):
                driverObj=Driver(symList,driver,fileList);
            elif (driverType == "EqFIX"):
                driverObj=EqDriver(symList,driver,fileList);
            #driverObj.printProperties();
            driverObjects.append(driverObj);

        threads=[]

        for driver in driverObjects:
                t=threading.Thread(target=driver.run)
                threads.append(t);
                t.start();
        while(True):
                time.sleep(2);
                #Check if all threads are alive. Othersise shutdown
                numberOfThreadsDown=0;
                for thread in threads:
                 if(not thread.is_alive()):
                    numberOfThreadsDown=numberOfThreadsDown+1
                if (numberOfThreadsDown == len(threads)):
                    printMsg("All threads have shut down...Shutting down now. Bye !!");
                    return;
                #Check signal received status
                if (len(driverObjects) == 0):
                        printMsg("Received Shutdown Signal in Main. Shutting down now...");
                        for thread in threads:
                          thread.join();
                        print("All threads joined..Exiting..Bye !!");
                        return;

if __name__ == "__main__":
        try:
                if (len(sys.argv) != 3):
                        print("Usage:FIXRattler.py <List of ConfigFiles> <Start Time in HH:MM:SS>");
                        sys.exit(1)
                startTimeStr=sys.argv[2];

                # If "now", start the FIX load immediately

                if (startTimeStr.lower() != "now"):
                # Just sleep till the start time has come
                   timevar=datetime.datetime.strptime(startTimeStr,"%H:%M:%S");
                   timediff=timevar-datetime.datetime.now();
                   timetoSleep=timediff.seconds;
                   print("Sleeping till "+startTimeStr);
                   time.sleep(timetoSleep);

                main(sys.argv[1])
                #main("EqFix.xml")
        except KeyboardInterrupt:
                print("Received Keyboard event");
                sys.exit(1);
        #except Exception:
        #        print("Exception occured during startup...");
        #        sys.exit(1);


#!/usr/bin/python

import os;
import PrintColor;
import subprocess;
import sys;

class bcolors:
    HEADER = '\033[95m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

serverList=[];

def setupEnvironVariables():
  #PrintColor.print_bold("Setting up Environmental Variables");
  homeDir=os.environ["HOME"];
  os.environ["LT_INPUT_FILES_PATH"]=homeDir+"/FIXRattler/inputfiles";
  os.environ["LT_SOURCE_FILES_PATH"]=homeDir+"/FIXRattler/sourcecode";
  os.environ["LT_OUTPUT_FILES_PATH"]=homeDir+"/FIXRattler/outputfiles";
  os.environ["LT_FIX_LOG_FILES_PATH"]=homeDir+"/FIXRattler/logs";


def GenerateLoad():
  PrintColor.print_bold("Generating Load...");
  sourceFilesPath=os.environ["LT_SOURCE_FILES_PATH"];
  inputFilesPath=os.environ["LT_INPUT_FILES_PATH"];
  outputFilesPath=os.environ["LT_OUTPUT_FILES_PATH"];
  serverListString="";
  shippingFileName=inputFilesPath+"/ShippingFilesList.txt";
  cmdToWrite="scp -q "+outputFilesPath+"/set";
  fp=open(shippingFileName,"w");
  numberOfServers=int(raw_input("Enter the number of Servers From where you want to run X-Stream FIX Load:"));
  for i in list(range(numberOfServers)):
      serverName=raw_input("Enter server # "+str(i+1)+" : ");
      serverList.append(serverName.strip('\n'));
      if (i != 0):
        serverListString=serverListString+":"+serverName;
      else:
        serverListString=serverName;
      cmdText="ssh -q "+serverName+" mkdir -p "+outputFilesPath+"/set"+str(i+1);
      fp.write(cmdText+"\n");
      cmdText=cmdToWrite+str(i+1)+"/* "+serverName+":"+outputFilesPath+"/set"+str(i+1);
      fp.write(cmdText+"\n");

  fp.close();
  cmdToExecute=sourceFilesPath+"/generateFIXRattlerConfig.py "+inputFilesPath+"/LoadPattern.txt "+inputFilesPath+"/OrderbookInput.txt "+inputFilesPath+"/AccountInput.txt "+inputFilesPath+"/MemberOrderRatio.txt "+serverListString;
  os.system(cmdToExecute);
  option=raw_input("Do you want to ship the files now ?(y/n)");
  if (option.upper()=='Y'):
     fp=open(shippingFileName,"r");
     for line in fp:
        cmdToExecute=line.strip('\n');
	os.system(cmdToExecute);
     fp.close();

  PrintColor.print_bold("Load Generation Done.. Press enter to continue to main menu...");
  response=raw_input();
  MainMenu();


def SetReferencePrice():
  PrintColor.print_bold("Going to create Reference Price Files");
  sourceFilesPath=os.environ["LT_SOURCE_FILES_PATH"];
  inputFilesPath=os.environ["LT_INPUT_FILES_PATH"];
  cmdToExecute=sourceFilesPath+"/qtestcsv_set_ref_price_cmd_creation.sh "+inputFilesPath+"/OrderbookInput.txt > "+inputFilesPath+"/Temp1.csv";
  os.system(cmdToExecute);
  cmdToExecute="cat "+inputFilesPath+"/Temp1.csv | sed s/\\\"//g > "+inputFilesPath+"/SetRefPrice.csv";
  os.system(cmdToExecute);
  cmdToExecute="rm "+inputFilesPath+"/Temp1.csv";
  os.system(cmdToExecute);
  PrintColor.print_info("Reference Price input file created.Going to set Reference Price");
  cmdToExecute="ssh sa01xewspe01 ~/.setenviron;qtestcsv -c "+inputFilesPath+"/SetRefPrice.csv";
  os.system(cmdToExecute);
  PrintColor.print_bold("Reference Price has been set.. Press enter to continue to main menu...");
  response=raw_input();
  MainMenu();

def SetMarketStates(preOpenTime):
  PrintColor.print_bold("Going to generate Market states");
  sourceFilesPath=os.environ["LT_SOURCE_FILES_PATH"];
  inputFilesPath=os.environ["LT_INPUT_FILES_PATH"];
  outputFilesPath=os.environ["LT_OUTPUT_FILES_PATH"];
  cmdToExecute=sourceFilesPath+"/SetMarketTiming.py "+inputFilesPath+"/LoadPattern.txt "+preOpenTime;
  os.system(cmdToExecute);
  #hostname="sa01xewspe01";
  #logFile=outputFilesPath+"/MarketStatelog.out";
  #fpout=open(logFile,"w");
  PrintColor.print_info("Removing All Trade Events");
  #cmdArguments=sourceFilesPath+"~/.setenviron;qtestcsv -c "+inputFilesPath+"/RemoveTrdEvents.csv";
  #proc=subprocess.Popen(['ssh','-q',hostname],stdin=subprocess.PIPE,stdout=fpout,stderr=fpout);
  #proc.stdin.write(cmdArguments);
  #outs, errs = proc.communicate();
  #PrintColor.print_info("Process Id of process is :"+str(proc1.pid));
  option=raw_input("Do you want to remove all trade events now (y/n) ? ");
  if (option.upper() == "Y"):
    cmdToExecute="ssh -q sa01xewspe01 ~/.setenviron;qtestcsv -c "+inputFilesPath+"/RemoveTrdEvents.csv >"+outputFilesPath+"/MarketStateLog.out";
    os.system(cmdToExecute);
  
  option=raw_input("Do you want to add all trade events now (y/n) ? ");
  if (option.upper() == "Y"):
    cmdToExecute="ssh -q sa01xewspe01 ~/.setenviron;qtestcsv -c "+inputFilesPath+"/AddTrdEvents.csv >>"+outputFilesPath+"/MarketStateLog.out";
    os.system(cmdToExecute);

  option=raw_input("Do you want to schedule Measurement points now (y/n) ? ");
  if (option.upper() == "Y"):
    ScheduleMeasurementPoints(preOpenTime);

  PrintColor.print_bold("Press enter to continue to main menu...");
  response=raw_input();
  MainMenu();
  #os.system(cmdToExecute);

def StartLoad(loadStartTime):
  sourceFilesPath=os.environ["LT_SOURCE_FILES_PATH"];
  inputFilesPath=os.environ["LT_INPUT_FILES_PATH"];
  outputFilesPath=os.environ["LT_OUTPUT_FILES_PATH"];
  logFile=outputFilesPath+"/StartLoadlog.out";
  fpout=open(logFile,"w");
  if len(serverList) == 0:
     PrintColor.print_error("Server List is empty. First Generate Load..");
     return;
  i=1;
  processList=[];
  for hostname in serverList:
      PrintColor.print_info("Going to start load in server:"+hostname);
      cmdArguments=sourceFilesPath+"/.set_environ;"+sourceFilesPath+"/FIXRattler.py "+outputFilesPath+"/set"+str(i)+"/FIXRattlerStart.txt "+loadStartTime;
      cmdToExecute="nohup ssh -q "+hostname+" \""+cmdArguments +"\" &";
      #print(cmdToExecute);
      os.system(cmdToExecute);
      #proc1=subprocess.Popen(['ssh','-q',hostname],stdin=subprocess.PIPE,stdout=fpout,stderr=fpout,SHELL=);
      #proc1.stdin.write(cmdArguments);
      #PrintColor.print_info("Process Id of process is :"+str(proc1.pid));
      i=i+1;
  PrintColor.print_bold("FIX Load Triggered.Press enter to continue to main menu...");
  response=raw_input();
  MainMenu();

def ScheduleMeasurementPoints(preOpenTime):
  PrintColor.print_bold("Going to Schedule Measurement Points");
  sourceFilesPath=os.environ["LT_SOURCE_FILES_PATH"];
  cmdToExecute="nohup "+sourceFilesPath+"/ScheduleEvents.py "+preOpenTime +" >/dev/null &";
  os.system(cmdToExecute);

def MainMenu():
  PrintColor.print_header("This will guide you to set up the Load Test and Start it.Choose the option you want to proceed with.")
  PrintColor.print_bold("1. Generate Load\n2. Set Reference Price\n3. Set Market Timing \n4. Set Load Start Time\n5. Quit\n");
  option=int(raw_input('Enter Option:'));
  if (option == 1):
    GenerateLoad();
  elif (option == 2):
    SetReferencePrice();
  elif (option == 3):
    timeToStart=raw_input("Enter the time you want to pre-open the market in HH:MM:SS.To skip press \"n\":");
    if (timeToStart.upper()!="N"):
      SetMarketStates(timeToStart);
  elif (option == 4):
    timeToStart=raw_input("Enter the time you want to start the Load in HH:MM:SS or \"now\" for immediate.To skip press \"n\":");
    if (timeToStart.upper()!="N"):
      StartLoad(timeToStart);
  elif (option == 5):
     sys.exit(0);
  else:
     os.system("clear");
     PrintColor.print_warn("Invalid Option. Please try Again..\n\n");
     MainMenu();


  


#Main Starts here
if __name__ == "__main__":
  setupEnvironVariables();
  MainMenu();


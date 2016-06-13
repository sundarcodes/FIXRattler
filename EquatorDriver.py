import threading;
import socket;
import datetime;
import string;
import random;
import time;
import logging;
from FIXSession import FIXSession;
from XMLReader import XMLReader;

class EqDriver():
        def __init__(self,symList,driverConfigList,inputList):
                self.symList=symList;
                self.driverConfigList=driverConfigList;
                self.fixMsgDict={}
                self.seqNumCounter=1;
                self.clientUniqueIdCounter=0;
                self.clientRefIdInitializer=''.join(random.choice(string.ascii_uppercase) for _ in range(4));
                self.clientReqIdInitializer=''.join(random.choice(string.ascii_uppercase) for _ in range(3));
                self.shutDown=False;
                self.inputList=inputList; 
                for property in self.driverConfigList:
                        if (len(property)==0):
                                continue;
                        if (property['name']=='Host'):
                                self.host=property['value'];
                        elif (property['name']=='Port'):
                                self.port=property['value'];
                        elif (property['name']=='SenderCompID'):
                                self.senderCompId=property['value'];
                        elif (property['name']=='SenderSubID'):
                                self.senderSubId=property['value'];
                        elif (property['name']=='TargetCompID'):
                                self.targetCompId=property['value'];
                        elif (property['name']=='BuyAccount'):
                                self.buyAccount=property['value'];
                        elif (property['name']=='SellAccount'):
                                self.sellAccount=property['value'];
                        elif (property['name']=='NIN'):
                                self.NIN=property['value'];
                        elif (property['name']=='Murabha'):
                                self.MurabhaStr=property['value'];
                        elif (property['name']=='NINAccList'):
                                self.NinAccListStr=property['value'];
                        elif (property['name']=='SendRate'):
                                self.sendRate=property['value'];
                        elif (property['name']=='BatchSize'):
                                self.batchSize=property['value'];
                        elif (property['name']=='U10Percentage'):
                                self.U10Percent=property['value'];
                        elif (property['name']=='U12Percentage'):
                                self.U12Percent=property['value'];
                        elif (property['name']=='U14Percentage'):
                                self.U14Percent=property['value'];
                        elif (property['name']=='U16Percentage'):
                                self.U16Percent=property['value'];
                        elif (property['name']=='U18Percentage'):
                                self.U18Percent=property['value'];
                        elif (property['name']=='U20Percentage'):
                                self.U20Percent=property['value'];
                        elif (property['name']=='U22Percentage'):
                                self.U22Percent=property['value'];
                        elif (property['name']=='LogMessages'):
                                self.LogMessages=property['value'];

                #Put the buy and Sell Account in a list
                self.buyAccList=self.buyAccount.split(',');
                self.sellAccList=self.sellAccount.split(',');
                self.NINList=self.NIN.split(',');
                self.NINAccList=[];
                self.NINAccMap={};
                self.NINMultiAccMap={};
                self.MurabhaList=self.MurabhaStr.split('|');
                self.ninAccStrList=self.NinAccListStr.split(',');
                acclist=[];
                for ninAccStr in self.ninAccStrList:
                    list=ninAccStr.split('|');
                    nin=list[0];
                    acc=list[1];
                    if (nin in self.NINAccMap):
                #Add this to the multiAccMap
                      if (nin in self.NINMultiAccMap):
                         acclist=self.NINMultiAccMap[nin];
                      else:
                         acclist=[];
                         acclist.append(self.NINAccMap[nin]);
                      acclist.append(acc)
                      self.NINMultiAccMap[nin]=acclist;
                    else:
                      self.NINAccMap[nin]=acc;

                #Get the Account Counts
                self.buyAccCount=len(self.buyAccList);
                self.sellAccCount=len(self.sellAccList);
                self.symbolCount=len(self.symList);
                self.ninCount=len(self.NINList);
                self.ninAccMapCount=len(self.NINAccMap);
                self.ninMultiAccMapCount=len(self.NINMultiAccMap);

                #Create a lock Object
                self.receivedUMessageList=[];
                self.lockObj=threading.Lock();

                self.testReqID="";
                self.HeartBeatToBeSent=False;
                self.loggerObj=logging.getLogger("FIXRattler.U_"+self.senderCompId);

        def reloadProperties(self,driverConfigList):
                self.driverConfigList=driverConfigList;
                for property in self.driverConfigList:
                        if (len(property)==0):
                                continue;
                        if (property['name']=='Host'):
                                self.host=property['value'];
                        elif (property['name']=='Port'):
                                self.port=property['value'];
                        elif (property['name']=='SenderCompID'):
                                self.senderCompId=property['value'];
                        elif (property['name']=='SenderSubID'):
                                self.senderSubId=property['value'];
                        elif (property['name']=='TargetCompID'):
                                self.targetCompId=property['value'];
                        elif (property['name']=='BuyAccount'):
                                self.buyAccount=property['value'];
                        elif (property['name']=='SellAccount'):
                                self.sellAccount=property['value'];
                        elif (property['name']=='NIN'):
                                self.NIN=property['value'];
                        elif (property['name']=='Murabha'):
                                self.MurabhaStr=property['value'];
                        elif (property['name']=='NINAccList'):
                                self.NinAccListStr=property['value'];
                        elif (property['name']=='SendRate'):
                                self.sendRate=property['value'];
                        elif (property['name']=='BatchSize'):
                                self.batchSize=property['value'];
                        elif (property['name']=='U10Percentage'):
                                self.U10Percent=property['value'];
                        elif (property['name']=='U12Percentage'):
                                self.U12Percent=property['value'];
                        elif (property['name']=='U14Percentage'):
                                self.U14Percent=property['value'];
                        elif (property['name']=='U16Percentage'):
                                self.U16Percent=property['value'];
                        elif (property['name']=='U18Percentage'):
                                self.U18Percent=property['value'];
                        elif (property['name']=='U20Percentage'):
                                self.U20Percent=property['value'];
                        elif (property['name']=='U22Percentage'):
                                self.U22Percent=property['value'];
                        elif (property['name']=='LogMessages'):
                                self.LogMessages=property['value'];



        def printProperties(self):
                #print self.host,self.port,self.senderCompId,self.senderSubId,self.targetCompId,self.buyAccount,self.sellAccount,self.sendRate,self.U10Percent,self.U12Percent,self.U14Percent,self.U16Percent,self.U18Percent,self.U20Percent,self.U22Percent,self.LogMessages
                print(self.host,self.port,self.senderCompId,self.senderSubId,self.targetCompId,self.buyAccount,self.sellAccount,self.sendRate,self.U10Percent,self.U12Percent,self.U14Percent,self.U16Percent,self.U18Percent,self.U20Percent,self.U22Percent,self.LogMessages);

        def frameLogonMsg(self):
                self.fixMsgDict['35']='A';
                self.fixMsgDict['34']=str(self.seqNumCounter);
                self.fixMsgDict['49']=self.senderCompId;
                self.fixMsgDict['50']=self.senderSubId;
                self.fixMsgDict['56']=self.targetCompId;
                self.fixMsgDict['98']='0';
                self.fixMsgDict['108']='60';
                self.fixMsgDict['141']='Y';
                self.fixMsgDict['1137']='4';
                self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
                self.seqNumCounter=self.seqNumCounter+1;

        def frameU10Msg(self):
                self.fixMsgDict['35']='U10';
                self.fixMsgDict['34']=str(self.seqNumCounter);
                self.fixMsgDict['49']=self.senderCompId;
                self.fixMsgDict['50']=self.senderSubId;
                self.fixMsgDict['56']=self.targetCompId;
                self.fixMsgDict['9700']=self.senderCompId;
                self.fixMsgDict['9701']=self.getRefNum();
                self.fixMsgDict['9742']=self.getReqID();
                self.fixMsgDict['9702']='2';     #Account Type
                self.fixMsgDict['9703']=str(random.choice([1,2,3,4]));  #Client Type
                self.fixMsgDict['9704']=self.getRandomNin();     
                self.fixMsgDict['9710']='1';
                self.fixMsgDict['9711']=''.join(random.choice(string.ascii_uppercase) for _ in range(4));
                self.fixMsgDict['9712']="Riyadh";
                self.fixMsgDict['9713']="11200";
                self.fixMsgDict['9714']='1';
                self.fixMsgDict['9705']='1';
                randomBankAccStr=''.join(random.choice(string.digits) for _ in range(12));
                randomBankAccStr="NCBKSA"+randomBankAccStr
                #randomBankAccStr="NCBKSA".join("1223344");
                self.fixMsgDict['9706']=randomBankAccStr;
                self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");				
                self.seqNumCounter=self.seqNumCounter+1;

        def frameU12Msg(self):
          self.fixMsgDict['35']='U12';
          self.fixMsgDict['34']=str(self.seqNumCounter);
          self.fixMsgDict['49']=self.senderCompId;
          self.fixMsgDict['50']=self.senderSubId;
          self.fixMsgDict['56']=self.targetCompId;		  
          self.fixMsgDict['9700']=self.senderCompId;
          i=random.randint(1,2);
          if (i == 1):
               acc=self.getRandomBuyAcc();
          else:
               acc=self.getRandomSellAcc();
          self.fixMsgDict['9708']=acc;
          self.fixMsgDict['9742']=self.getReqID();
          self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");		  
          self.seqNumCounter=self.seqNumCounter+1;

        def frameU14Msg(self):
          self.fixMsgDict['35']='U14';
          self.fixMsgDict['34']=str(self.seqNumCounter);
          self.fixMsgDict['49']=self.senderCompId;
          self.fixMsgDict['50']=self.senderSubId;
          self.fixMsgDict['56']=self.targetCompId;		  
          self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");		  		  
          self.fixMsgDict['9700']=self.senderCompId;
          i=random.randint(1,2);
          if (i == 1):
               acc=self.getRandomBuyAcc();
          else:
               acc=self.getRandomSellAcc();
          self.fixMsgDict['9708']=acc;
          self.fixMsgDict['9742']=self.getReqID();
          randomBankAccStr=''.join(random.choice(string.digits) for _ in range(12));
          randomBankAccStr="NCBKSA"+randomBankAccStr
          self.fixMsgDict['9706']=randomBankAccStr;
          self.seqNumCounter=self.seqNumCounter+1;

        def frameU16Msg(self):
          self.fixMsgDict['35']='U16';
          self.fixMsgDict['34']=str(self.seqNumCounter);
          self.fixMsgDict['49']=self.senderCompId;
          self.fixMsgDict['50']=self.senderSubId;
          self.fixMsgDict['56']=self.targetCompId;		  
          self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");		  
          self.fixMsgDict['9700']=self.senderCompId;
          i=random.randint(1,2);
          if (i == 1):
               acc=self.getRandomBuyAcc();
          else:
               acc=self.getRandomSellAcc();
          self.fixMsgDict['9708']=acc;
          self.fixMsgDict['9742']=self.getReqID();
          self.seqNumCounter=self.seqNumCounter+1;

        def frameU18Msg(self):
          self.fixMsgDict['35']='U18';
          self.fixMsgDict['34']=str(self.seqNumCounter);
          self.fixMsgDict['49']=self.senderCompId;
          self.fixMsgDict['50']=self.senderSubId;
          self.fixMsgDict['56']=self.targetCompId;		  
          self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");		  
          self.fixMsgDict['9700']=self.senderCompId;
          i=random.randint(1,2);
          if (i == 1):
               acc=self.getRandomBuyAcc();
          else:
               acc=self.getRandomSellAcc();
          self.fixMsgDict['9708']=acc;
          self.fixMsgDict['9742']=self.getReqID();
          self.fixMsgDict['55']=self.getRandomSymbol();
          self.fixMsgDict['60']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
          self.seqNumCounter=self.seqNumCounter+1;

        def frameU20Msg(self):
          self.fixMsgDict['35']='U20';
          self.fixMsgDict['34']=str(self.seqNumCounter);
          self.fixMsgDict['49']=self.senderCompId;
          self.fixMsgDict['50']=self.senderSubId;
          self.fixMsgDict['56']=self.targetCompId;		  
          self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");		  
          self.fixMsgDict['9742']=self.getReqID();
          self.fixMsgDict['9701']=self.getRefNum();
          self.fixMsgDict['9726']=self.senderCompId;
          self.fixMsgDict['9723']=self.senderCompId;
          self.fixMsgDict['55']=self.getRandomSymbol();
          self.fixMsgDict['53']='2';
          self.fixMsgDict['9722']='I';
          nin=random.choice(list(self.NINAccMap.keys()));
          acc=self.NINAccMap[nin];
          self.fixMsgDict['9704']=nin;     
          self.fixMsgDict['9724']=acc;
          self.fixMsgDict['9744']=datetime.datetime.now().strftime("%Y%m%d");     
          self.seqNumCounter=self.seqNumCounter+1;

        def frameU22Msg(self,transferType):
          self.fixMsgDict['35']='U22';
          self.fixMsgDict['34']=str(self.seqNumCounter);
          self.fixMsgDict['49']=self.senderCompId;
          self.fixMsgDict['50']=self.senderSubId;
          self.fixMsgDict['56']=self.targetCompId;		  
          self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");		  
          self.fixMsgDict['9742']=self.getReqID();
          self.fixMsgDict['9701']=self.getRefNum();
          self.fixMsgDict['9732']=self.senderCompId;
          self.fixMsgDict['9735']=self.senderCompId;
          if (transferType == 0):
             self.fixMsgDict['9731']='M';
             nin=self.MurabhaList[0];
             acc=self.MurabhaList[1];
             self.fixMsgDict['9733']=acc;
             self.fixMsgDict['9734']=nin;
             nin=random.choice(list(self.NINAccMap.keys()));
             acc=self.NINAccMap[nin];
             self.fixMsgDict['9736']=acc;
             self.fixMsgDict['9737']=nin;
             
          else:
             self.fixMsgDict['9731']='A';
             nin=random.choice(list(self.NINMultiAccMap.keys()));
             acc1=self.NINMultiAccMap[nin][0];
             acc2=self.NINMultiAccMap[nin][1];
             self.fixMsgDict['9733']=acc1;
             self.fixMsgDict['9734']=nin;
             self.fixMsgDict['9736']=acc2;
             self.fixMsgDict['9737']=nin;
          
          self.fixMsgDict['55']=self.getRandomSymbol();
          self.fixMsgDict['53']='10';
          self.seqNumCounter=self.seqNumCounter+1;

        def frameHeartBeatMsg(self):
          self.fixMsgDict['35']='0';
          self.fixMsgDict['34']=str(self.seqNumCounter);
          if (self.testReqID != ""):
              self.fixMsgDict['112']=self.testReqID;
          self.testReqID="";
          self.HeartBeatToBeSent=False;
          self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
          self.seqNumCounter=self.seqNumCounter+1;



        def getRandomSymbol(self):
                randomIndex=random.randint(0,self.symbolCount-1);
                return self.symList[randomIndex]['name'];

        def getRandomBuyAcc(self):
                randomIndex=random.randint(0,self.buyAccCount-1);
                return self.buyAccList[randomIndex];

        def getRandomSellAcc(self):
                randomIndex=random.randint(0,self.sellAccCount-1);
                return self.sellAccList[randomIndex];

        def getRandomNin(self):
                randomIndex=random.randint(0,self.ninCount-1);
                return self.NINList[randomIndex];

        def getRefNum(self):
                self.clientUniqueIdCounter=self.clientUniqueIdCounter+1;
                return self.clientRefIdInitializer+str(self.clientUniqueIdCounter);

        def getReqID(self):
                self.clientUniqueIdCounter=self.clientUniqueIdCounter+1;
                return self.clientReqIdInitializer+str(self.clientUniqueIdCounter);

        def removeTags(self,tagList):
                for tag in tagList:
                        if tag in self.fixMsgDict:
                                del self.fixMsgDict[tag];

        def recvLogonMsg(self):
                msgList=self.fixSessionobj.recvFIXMsg();
                for executionReport in msgList:
                        if (executionReport['35'] == 'A'):
                                self.printMsg("Received Logon")
                                return True;
                        else:
                                return False;

        def recvExecutionReport(self):
                while(not self.shutDown):
                        msgList=self.fixSessionobj.recvFIXMsg();
                        for executionReport in msgList:
                                if (executionReport['35'] == 'A'):
                                        self.printMsg("Received Logon")
                                        self.loggedOn=True;
                                        continue;
                                if (executionReport['35'] == '5'):
                                        self.printMsg("Received Logout");
                                        self.shutDown=True;
                                        self.loggedOn=False;
                                        break;
                                if (executionReport['35'] == '0'):
                                        self.printMsg("Received Heartbeat Message");
                                        self.HeartBeatToBeSent=True;
                                if (executionReport['35'] == '1'):
                                        self.printMsg("Received Test Request");
                                        self.testReqID=executionReport['112'];
                                        self.HeartBeatToBeSent=True;


        def stopRunning(self):
                #Set the shutdownFlag to True
                self.shutDown=True;

        def doCleanup(self):
                self.fixSessionobj.closeSocket();

        def closeFilePointers(self):
                self.fixSessionobj.close();

        def checkHeartBeatMsg(self):
                if (self.HeartBeatToBeSent):
                  self.frameHeartBeatMsg();
                  self.removeTags(['59','55','44','1','40','11','54','60','38']);
                  self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                  self.removeTags(['112']);

        def printMsg(self,msg):
                #fmtString=str(datetime.datetime.now())+"\t"+self.threadName+"\t"+msg;
                #print(fmtString);
                self.loggerObj.debug(msg);


        def run(self):
                self.fixSessionobj=FIXSession(self.host,self.port,"U_"+self.senderCompId);
                if(not self.fixSessionobj.connect()):
                        return;
                self.frameLogonMsg();
                self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                if (not self.recvLogonMsg()):
                        self.printMsg("Failure in receiving Logon Msg")
                        return;


                self.removeTags(['141','98','108','1137']);

                #Start the recv thread

                recvThread=threading.Thread(target=self.recvExecutionReport)
                recvThread.start();

                # For each Run. reload the properties from XML File
                run=1;
                for line in self.inputList:
                   tmpList=line.split(',');
                   numberOfSecondsToRun=int(tmpList[0]);
                   file=tmpList[1].strip('\n');
                   try:
                       xmlReaderobj=XMLReader(file);
                   except Exception:
                      self.printMsg("Exception while parsing XML File...Shutting down...");
                      self.shutDown=True;
                      self.doCleanup();
                      recvThread.join();
                      self.closeFilePointers();
                      return;

                   reloadList=xmlReaderobj.getDriverProperties(self.senderCompId);
                   self.reloadProperties(reloadList);
                   runStartTime=datetime.datetime.now();
                   currentTime=datetime.datetime.now();
                   totMsgSentInThisRun=0;

                   while((datetime.datetime.now()-runStartTime).seconds < numberOfSecondsToRun):
                        msgsPerSec=float(self.sendRate);
                        msgSent=0;
                        U10Msgs=round(msgsPerSec*float(self.U10Percent)*0.01,0);
                        U12Msgs=round(msgsPerSec*float(self.U12Percent)*0.01,0);
                        U14Msgs=round(msgsPerSec*float(self.U14Percent)*0.01,0);
                        U16Msgs=round(msgsPerSec*float(self.U16Percent)*0.01,0);
                        U18Msgs=round(msgsPerSec*float(self.U18Percent)*0.01,0);
                        U20Msgs=round(msgsPerSec*float(self.U20Percent)*0.01,0);
                        U22Msgs=round(msgsPerSec*float(self.U22Percent)*0.01,0);
                        totalMsgToBeSent=U10Msgs+U12Msgs+U14Msgs+U16Msgs+U18Msgs+U20Msgs+U22Msgs;
                        sleepInterval=1.0/totalMsgToBeSent;
                        batchStartTime=datetime.datetime.now();
                        while(msgSent<totalMsgToBeSent):
                                if (self.shutDown):
                                        self.doCleanup();
                                        self.printMsg("Received shutdown event");
                                        recvThread.join();
                                        self.closeFilePointers();
                                        return;

                                self.checkHeartBeatMsg();

                                currentTime=datetime.datetime.now();
                                if ((currentTime-batchStartTime).seconds > 1):
                                        self.printMsg("Breaking out ");
                                        break;

                                if (U10Msgs>0):
                                        #print("Sending new order")
                                        self.frameU10Msg();
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        U10Msgs=U10Msgs-1;
                                        msgSent=msgSent+1;
                                        self.fixMsgDict.clear();
                                        time.sleep(sleepInterval);

                                if (U12Msgs>0):
                                        self.frameU12Msg();
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        U12Msgs=U12Msgs-1;
                                        msgSent=msgSent+1;
                                        self.fixMsgDict.clear();
                                        time.sleep(sleepInterval);

                                if (U14Msgs>0):
                                        self.frameU14Msg();
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        self.removeTags(['110']);
                                        U14Msgs=U14Msgs-1;
                                        msgSent=msgSent+1;
                                        self.fixMsgDict.clear();
                                        time.sleep(sleepInterval);

                                if (U16Msgs>0):
                                        self.frameU16Msg();
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        self.removeTags(['110']);
                                        U16Msgs=U16Msgs-1;
                                        msgSent=msgSent+1;
                                        self.fixMsgDict.clear();
                                        time.sleep(sleepInterval);

                                if (U18Msgs>0):
                                        self.frameU18Msg();
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        self.removeTags(['110']);
                                        U18Msgs=U18Msgs-1;
                                        msgSent=msgSent+1;
                                        self.fixMsgDict.clear();
                                        time.sleep(sleepInterval);

                                if (U20Msgs>0):
                                        self.frameU20Msg();
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        self.removeTags(['110']);
                                        U20Msgs=U20Msgs-1;
                                        msgSent=msgSent+1;
                                        self.fixMsgDict.clear();
                                        time.sleep(sleepInterval);

                                if (U22Msgs>0):
                                        self.frameU22Msg(random.randint(0,2));
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        self.removeTags(['110']);
                                        U22Msgs=U22Msgs-1;
                                        msgSent=msgSent+1;
                                        self.fixMsgDict.clear();
                                        time.sleep(sleepInterval);
                        self.printMsg("Number of messages send in this batch is "+str(msgSent));
                        currentTime=datetime.datetime.now();
                        if(currentTime-batchStartTime).seconds < 1:
                            microsecondsLeft=1000000.0-((currentTime-batchStartTime).microseconds);
                            if microsecondsLeft>0:
                                self.printMsg("Sleeping before starting the next run")
                                time.sleep(float(microsecondsLeft/1000000));
                        currentTime=datetime.datetime.now();
                        totMsgSentInThisRun=totMsgSentInThisRun+msgSent;
                   self.printMsg("Number of messages send in run-"+str(run)+" is "+str(totMsgSentInThisRun));
                   run=run+1;
                self.printMsg("Completed Runs..Going to shutdown..");
                self.shutDown=True;
                self.doCleanup();
                recvThread.join();
                self.closeFilePointers();
                return;






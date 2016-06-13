import threading;
import socket;
import datetime;
import string;
import random;
import time;
import Utilities;
import logging;
import math;
from FIXSession import FIXSession;
from XMLReader import XMLReader;

class Driver():
        def __init__(self,symList,driverConfigList,inputList):
                self.symList=symList;
                self.driverConfigList=driverConfigList;
                self.fixMsgDict={}
                self.seqNumCounter=1;
                self.clientOrderIdCounter=0;
                self.clientOrderIdInitializer=''.join(random.choice(string.ascii_uppercase) for _ in range(4));
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
                        elif (property['name']=='SendRate'):
                                self.sendRate=property['value'];
                        elif (property['name']=='BatchSize'):
                                self.batchSize=property['value'];
                        elif (property['name']=='MarketOrderPercentage'):
                                self.marketOrderPercent=property['value'];
                        elif (property['name']=='HiddenOrderPercentage'):
                                self.hiddenOrderPercent=property['value'];
                        elif (property['name']=='ModifyOrderPercentage'):
                                self.modifyOrderPercent=property['value'];
                        elif (property['name']=='CancelOrderPercentage'):
                                self.cancelOrderPercent=property['value'];
                        elif (property['name']=='GTDOrderPercentage'):
                                self.GTDOrderPercent=property['value'];
                        elif (property['name']=='ExpiryOrderPercentage'):
                                self.expiryOrderPercent=property['value'];
                        elif (property['name']=='RejectOrderPercentage'):
                                self.rejectOrderPercent=property['value'];
                        elif (property['name']=='TradePercentage'):
                                self.tradableLimitOrderPercent=property['value'];
                        elif (property['name']=='TimeInForce'):
                                self.timeInForce=property['value'];
                        elif (property['name']=='OrderQuantity'):
                                self.orderQty=property['value'];
                        elif (property['name']=='LogMessages'):
                                self.LogMessages=property['value'];

                self.newOrderPercent=float(self.tradableLimitOrderPercent);

                #Put the buy and Sell Account in a list
                self.buyAccList=self.buyAccount.split(',');
                self.sellAccList=self.sellAccount.split(',');

                #Get the Account Counts
                self.buyAccCount=len(self.buyAccList);
                self.sellAccCount=len(self.sellAccList);
                self.symbolCount=len(self.symList);

                #Create a lock Object
                self.queuedModifyOrdersList=[];
                self.queuedCancelOrdersList=[];
                self.lockObj=threading.Lock();
                self.threadName="Thread_"+self.senderCompId;
                self.loggerObj=logging.getLogger("FIXRattler."+self.senderCompId);

                self.SymbolMap={};
		self.symbolLinearMap={};
                self.LastSentOrderTypeMap={};
		self.lastSentSymbolIndex=0;
                symCount=0;

		if (self.LogMessages == "true"):
                   self.Logging=True;
		else:
                   self.Logging=False;

                #Find Min,Max,TickSize
                for symDic in self.symList:
                        symbolName=symDic['name'];
                        priceRange=symDic['price'];
                        tickSize=round(float(symDic['ticksize']),2);
                        qty=int(symDic['qty']);
                        priceList=priceRange.split(',');
                        minPrice=float(priceList[0]);
                        maxPrice=float(priceList[1]);
                        spread=int((maxPrice-minPrice)/tickSize);
                        delta=int(spread*0.5);
                        medianPrice=minPrice+(delta*tickSize);
                        symbolTuple=tuple([medianPrice,tickSize,minPrice,maxPrice,spread,delta,qty]);
                        self.SymbolMap[symbolName]=symbolTuple;
                        self.LastSentOrderTypeMap[symbolName]=symCount%2;
			self.symbolLinearMap[symCount]=symbolName;
                        symCount=symCount+1;

                #Shuffle the symbolList Array
		random.shuffle(self.symbolLinearMap);

                #Distribution for Trade Percentage
                #tradePercentage=int(self.tradePercent);
                #self.matchingCounter=[];
                #self.counterTicker=0;
                #lengthOfMatchingList=100;

                #for i in list(range(0,tradePercentage)):
                #        self.matchingCounter.append(True);

                #for i in list(range(tradePercentage,lengthOfMatchingList)):
                #        self.matchingCounter.append(False);

                #Shuffle the elements in matchingCounter
                #random.shuffle(self.matchingCounter);

                #For Sending Heartbeat in response to 
                self.testReqID="";
                self.HeartBeatToBeSent=False;


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
                        elif (property['name']=='SendRate'):
                                self.sendRate=property['value'];
                        elif (property['name']=='BatchSize'):
                                self.batchSize=property['value'];
                        elif (property['name']=='MarketOrderPercentage'):
                                self.marketOrderPercent=property['value'];
                        elif (property['name']=='HiddenOrderPercentage'):
                                self.hiddenOrderPercent=property['value'];
                        elif (property['name']=='ModifyOrderPercentage'):
                                self.modifyOrderPercent=property['value'];
                        elif (property['name']=='CancelOrderPercentage'):
                                self.cancelOrderPercent=property['value'];
                        elif (property['name']=='GTDOrderPercentage'):
                                self.GTDOrderPercent=property['value'];
                        elif (property['name']=='ExpiryOrderPercentage'):
                                self.expiryOrderPercent=property['value'];
                        elif (property['name']=='RejectOrderPercentage'):
                                self.rejectOrderPercent=property['value'];
                        elif (property['name']=='TradePercentage'):
                                self.tradableLimitOrderPercent=property['value'];
                        elif (property['name']=='TimeInForce'):
                                self.timeInForce=property['value'];
                        elif (property['name']=='OrderQuantity'):
                                self.orderQty=property['value'];
                        elif (property['name']=='LogMessages'):
                                self.LogMessages=property['value'];

                self.newOrderPercent=float(self.tradableLimitOrderPercent);

                #Put the buy and Sell Account in a list
                self.buyAccList=self.buyAccount.split(',');
                self.sellAccList=self.sellAccount.split(',');

                #Get the Account Counts
                self.buyAccCount=len(self.buyAccList);
                self.sellAccCount=len(self.sellAccList);

		if (self.LogMessages == "true"):
                   self.Logging=True;
		else:
                   self.Logging=False;


                #Distribution for Trade Percentage
                #tradePercentage=int(self.tradePercent);
                #self.matchingCounter=[];
                #self.counterTicker=0;
                #lengthOfMatchingList=100;

                #for i in list(range(0,tradePercentage)):
                #        self.matchingCounter.append(True);

                #for i in list(range(tradePercentage,lengthOfMatchingList)):
                #        self.matchingCounter.append(False);

                #Shuffle the elements in matchingCounter
                #random.shuffle(self.matchingCounter);

        def printMsg(self,msg):
                #fmtString=str(datetime.datetime.now())+"\t"+self.threadName+"\t"+msg;
                #print(fmtString);
                self.loggerObj.debug(msg);


        def printProperties(self):
                print(self.host,self.port,self.senderCompId,self.senderSubId,self.targetCompId,self.buyAccount,self.sellAccount,self.sendRate,self.batchSize,self.marketOrderPercent,self.hiddenOrderPercent,self.modifyOrderPercent,self.cancelOrderPercent,self.tradePercent,self.timeInForce,self.orderQty,self.LogMessages);

        def frameLogonMsg(self):
                self.fixMsgDict['35']='A';
                self.fixMsgDict['34']=str(self.seqNumCounter);
                self.fixMsgDict['49']=self.senderCompId;
                self.fixMsgDict['50']=self.senderSubId;
                self.fixMsgDict['56']=self.targetCompId;
                self.fixMsgDict['98']='0';
                self.fixMsgDict['108']='60';
                self.fixMsgDict['141']='Y';
                self.fixMsgDict['1137']='8';
                self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
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

        def frameTradingSessionStatus(self):
                self.fixMsgDict['35']='g';
                self.fixMsgDict['34']=str(self.seqNumCounter);
                self.fixMsgDict['335']="REQ_"+self.clientOrderIdInitializer+str(self.seqNumCounter);
		self.fixMsgDict['263']="1"; # 1 is for Subscription;0 for Snapshot;2 for Update/Disbale
                self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
                self.seqNumCounter=self.seqNumCounter+1;

        def frameNewOrderSingleMsg(self):
                self.fixMsgDict['35']='D';
                self.fixMsgDict['34']=str(self.seqNumCounter);
                self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
                self.seqNumCounter=self.seqNumCounter+1;

        def getRandomSymbol(self):
	        if (self.lastSentSymbolIndex == self.symbolCount):
			self.lastSentSymbolIndex=0;
                symbol=self.symbolLinearMap[self.lastSentSymbolIndex];
		self.lastSentSymbolIndex=self.lastSentSymbolIndex+1;
                return symbol;

        def getRandomBuyAcc(self):
                randomIndex=random.randint(0,self.buyAccCount-1);
                return self.buyAccList[randomIndex];

        def getRandomSellAcc(self):
                randomIndex=random.randint(0,self.sellAccCount-1);
                return self.sellAccList[randomIndex];

        def getPrice(self,symbol,side,matchFlag):
                symbolTuple=self.SymbolMap[symbol];
                tickSize=symbolTuple[1];
                refPrice=symbolTuple[0];
                delta=int(symbolTuple[5]/2);
                if (matchFlag == 0):
                    if (side == '1'):
                        val=random.randint(0,delta);
                        price=str(refPrice+(val*tickSize));
                    elif (side == '2'):
                        val=random.randint(0,delta);
                        price=str(refPrice-(val*tickSize));
                elif (matchFlag == 1):
                    if (side == '1'):
                        minBuyPrice=symbolTuple[2];
                        val=random.randint(0,int(delta/2));
                        price=str(minBuyPrice+val*tickSize);
                    elif (side == '2'):
                        maxSellPrice=symbolTuple[3];
                        val=random.randint(0,int(delta/2));
                        price=str(maxSellPrice-val*tickSize);
                elif (matchFlag == 2):
                    if (side == '1'):
                        minBuyPrice=symbolTuple[2];
                        val=random.randint(0,delta);
                        price=str(minBuyPrice-val*tickSize);
                    elif (side == '2'):
                        maxSellPrice=symbolTuple[3];
                        val=random.randint(0,delta);
                        price=str(maxSellPrice+val*tickSize);

                return price;

        def getSymbolQty(self,symbol):
                 qty=self.SymbolMap[symbol][6];
                 return qty;

        def getClientOrderId(self,type):
                self.clientOrderIdCounter=self.clientOrderIdCounter+1;
                if (type == 0):
                        return "M_"+self.clientOrderIdInitializer+str(self.clientOrderIdCounter);
                elif (type == 1):
                        return "N_"+self.clientOrderIdInitializer+str(self.clientOrderIdCounter);
                elif (type == 2):
                        return "C_"+self.clientOrderIdInitializer+str(self.clientOrderIdCounter);

        def getOrderSide(self,symbol,orderType):
                if (orderType == 0):
                   orderSide=str(random.randint(1,2));
                elif (orderType == 1):
                   #Process for Limit Order
                   orderFlagBits=self.LastSentOrderTypeMap[symbol];
                   result=orderFlagBits&4;
                   if (result == 0):
                      orderSide="1";
                      self.LastSentOrderTypeMap[symbol]=orderFlagBits|4;
                   elif (result == 4):
                      orderSide="2";
                      self.LastSentOrderTypeMap[symbol]=orderFlagBits&3;
                elif (orderType == 2):
                   #Process for Market Order
                   orderFlagBits=self.LastSentOrderTypeMap[symbol];
                   result=orderFlagBits&2;
                   if (result == 0):
                      orderSide="1";
                      self.LastSentOrderTypeMap[symbol]=orderFlagBits|2;
                   elif (result == 2):
                      orderSide="2";
                      self.LastSentOrderTypeMap[symbol]=orderFlagBits&5;
                elif (orderType == 3):
                   #Process for Hidden Order
                   orderFlagBits=self.LastSentOrderTypeMap[symbol];
                   result=orderFlagBits&1;
                   if (result == 0):
                      orderSide="1";
                      self.LastSentOrderTypeMap[symbol]=orderFlagBits|1;
                   elif (result == 1):
                      orderSide="2";
                      self.LastSentOrderTypeMap[symbol]=orderFlagBits&4;
                
                return orderSide;

        def alterNewOrderMsg(self):
                self.fixMsgDict['55']=self.getRandomSymbol();
                orderSide=self.getOrderSide(self.fixMsgDict['55'],1);
                self.fixMsgDict['54']=orderSide;
                if (orderSide == "1"):
                        self.fixMsgDict['1']=self.getRandomBuyAcc();
                else:
                        self.fixMsgDict['1']=self.getRandomSellAcc();
                self.fixMsgDict['40']='2';
                self.fixMsgDict['44']=self.getPrice(self.fixMsgDict['55'],orderSide,0);
                price=float(self.fixMsgDict['44']);
                self.fixMsgDict['38']=str(self.getSymbolQty(self.fixMsgDict['55']));
                self.fixMsgDict['60']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
                self.fixMsgDict['11']=self.getClientOrderId(0);


        def alterNewHiddenOrderMsg(self):
                self.fixMsgDict['55']=self.getRandomSymbol();
                orderSide=self.getOrderSide(self.fixMsgDict['55'],3);
                self.fixMsgDict['54']=orderSide;
                if (orderSide == "1"):
                        self.fixMsgDict['1']=self.getRandomBuyAcc();
                else:
                        self.fixMsgDict['1']=self.getRandomSellAcc();
                self.fixMsgDict['40']='2';
                self.fixMsgDict['44']=self.getPrice(self.fixMsgDict['55'],orderSide,0);
                price=float(self.fixMsgDict['44']);
                qty=self.getSymbolQty(self.fixMsgDict['55']);
                orgQty=qty*11;
                disclosedQty=int(math.ceil(orgQty/10.0));
                self.fixMsgDict['38']=str(orgQty);
                self.fixMsgDict['1138']=str(disclosedQty);
                self.fixMsgDict['60']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
                self.fixMsgDict['11']=self.getClientOrderId(0);

        def alterNewGTDOrderMsg(self):
                self.fixMsgDict['55']=self.getRandomSymbol();
                orderSide=self.getOrderSide(self.fixMsgDict['55'],0);
                self.fixMsgDict['54']=orderSide;
                if (orderSide == "1"):
                        self.fixMsgDict['1']=self.getRandomBuyAcc();
                else:
                        self.fixMsgDict['1']=self.getRandomSellAcc();
                self.fixMsgDict['40']='2';
                self.fixMsgDict['44']=self.getPrice(self.fixMsgDict['55'],orderSide,1);
                qty=self.getSymbolQty(self.fixMsgDict['55']);
                self.fixMsgDict['38']=str(qty);
                self.fixMsgDict['59']='1';
                self.fixMsgDict['60']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
                self.fixMsgDict['11']=self.getClientOrderId(1);

        def alterNewExpiryOrderMsg(self):
                self.fixMsgDict['55']=self.getRandomSymbol();
                orderSide=self.getOrderSide(self.fixMsgDict['55'],0);
                self.fixMsgDict['54']=orderSide;
                if (orderSide == "1"):
                        self.fixMsgDict['1']=self.getRandomBuyAcc();
                else:
                        self.fixMsgDict['1']=self.getRandomSellAcc();
                self.fixMsgDict['40']='2';
                self.fixMsgDict['44']=self.getPrice(self.fixMsgDict['55'],orderSide,1);
                qty=self.getSymbolQty(self.fixMsgDict['55']);
                self.fixMsgDict['38']=str(qty);
                self.fixMsgDict['60']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
                self.fixMsgDict['11']=self.getClientOrderId(2);

        def alterNewRejectOrderMsg(self):
                self.fixMsgDict['55']=self.getRandomSymbol();
                orderSide=self.getOrderSide(self.fixMsgDict['55'],0);
                self.fixMsgDict['54']=orderSide;
                if (orderSide == "1"):
                        self.fixMsgDict['1']=self.getRandomBuyAcc();
                else:
                        self.fixMsgDict['1']=self.getRandomSellAcc();
                self.fixMsgDict['40']='2';
                self.fixMsgDict['44']=self.getPrice(self.fixMsgDict['55'],orderSide,2);
                qty=self.getSymbolQty(self.fixMsgDict['55']);
                self.fixMsgDict['38']=str(qty);
                self.fixMsgDict['11']=self.getClientOrderId(1);

        def removeTags(self,tagList):
                for tag in tagList:
                        if tag in self.fixMsgDict:
                                del self.fixMsgDict[tag];


        def alterNewMarketOrderMsg(self):
                self.fixMsgDict['55']=self.getRandomSymbol();
                orderSide=self.getOrderSide(self.fixMsgDict['55'],2);
                self.fixMsgDict['54']=orderSide;
                if (orderSide == "1"):
                        self.fixMsgDict['1']=self.getRandomBuyAcc();
                else:
                        self.fixMsgDict['1']=self.getRandomSellAcc();
                self.fixMsgDict['38']=self.getSymbolQty(self.fixMsgDict['55']);
                self.fixMsgDict['40']='1';
                self.fixMsgDict['60']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
                self.fixMsgDict['38']=str(self.getSymbolQty(self.fixMsgDict['55']));
                self.fixMsgDict['11']=self.getClientOrderId(0);
                self.removeTags(['44']);

        def frameModifyOrderMsg(self,orgClientOrderId,side,orderType,qty,price,symbol,account):
                self.fixMsgDict['35']='G';
                self.fixMsgDict['34']=str(self.seqNumCounter);
                self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
                self.fixMsgDict['41']=orgClientOrderId;
                self.fixMsgDict['54']=side;
                self.fixMsgDict['38']=qty;
                self.fixMsgDict['55']=symbol;
                self.fixMsgDict['1']=account;
                if (orderType == '1'):
                        self.fixMsgDict['40']='1';
                else:
                        self.fixMsgDict['40']='2';
                        self.fixMsgDict['44']=price
                self.fixMsgDict['11']=self.getClientOrderId(1);
                self.seqNumCounter=self.seqNumCounter+1;

        def frameCancelOrderMsg(self,orgClientOrderId,side,qty,symbol):
                self.fixMsgDict['35']='F';
                self.fixMsgDict['34']=str(self.seqNumCounter);
                self.fixMsgDict['52']=datetime.datetime.utcnow().strftime("%Y%m%d-%H:%M:%S");
                self.fixMsgDict['41']=orgClientOrderId;
                self.fixMsgDict['54']=side;
                self.fixMsgDict['38']=qty;
                self.fixMsgDict['55']=symbol;
                self.fixMsgDict['11']=self.getClientOrderId(2);
                self.seqNumCounter=self.seqNumCounter+1;

        def recvLogonMsg(self):
                msgList=self.fixSessionobj.recvFIXMsg();
                for executionReport in msgList:
                        if (executionReport['35'] == 'A'):
                                self.printMsg("Received Logon");
                                return True;
                        else:
                                return False;

        def recvExecutionReport(self):
                while(not self.shutDown):
                        msgList=self.fixSessionobj.recvFIXMsg();
                        #self.printMsg "Msg List received in Driver is "+str(len(msgList))
                        for executionReport in msgList:
                                if (executionReport['35'] == 'A'):
                                        self.printMsg("Received Logon");
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
                                if (executionReport['35'] == '8'):
                                        if (executionReport['39'] == '0'):
                                                #To handle execution report from XW
                                                if ('11' in executionReport):
                                                 clientOrdId=executionReport['11'];
                                                 if (clientOrdId.startswith("N_") ):
                                                        self.lockObj.acquire(True);
                                                        self.queuedModifyOrdersList.append(executionReport);
                                                        self.lockObj.release();
                                                 elif (clientOrdId.startswith("C_") ):
                                                        self.lockObj.acquire(True);
                                                        self.queuedCancelOrdersList.append(executionReport);
                                                        self.lockObj.release();
                self.printMsg("Recv Thread: Exiting..");

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


        def run(self):
                self.fixSessionobj=FIXSession(self.host,self.port,self.senderCompId,self.Logging);
                if(not self.fixSessionobj.connect()):
                        self.printMsg("Unable to connect to Server.. Exiting:");
                        return;
                self.frameLogonMsg();
                self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                if (not self.recvLogonMsg()):
                        self.printMsg("Failure in receiving Logon Msg...Exiting from Thread:");
                        return;


                self.removeTags(['141','98','108','1137']);

		#Subscribe to Trading Session Status Request
		self.frameTradingSessionStatus();
                self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                self.removeTags(['335','263']);

                #Start the recv thread

                recvThread=threading.Thread(target=self.recvExecutionReport)
                recvThread.start();

                # For each Run. reload the properties from XML File
                run=1;
                for line in self.inputList:
                   tmpList=line.split(',');
                   numberOfSecondsToRun=int(tmpList[0]);
                   file=tmpList[1].strip('\n');
                   #self.printMsg(file);
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
                        msgSent=0;
                        msgPerSec=float(self.sendRate);
                        if(msgPerSec == 0):
                          time.sleep(2); 
                          self.checkHeartBeatMsg();
                          continue;
                        if(msgPerSec<1):
                           newOrders=1;
                        else:
                           newOrders=round(msgPerSec*self.newOrderPercent*0.01,0);
                        cancelOrders=round(msgPerSec*float(self.cancelOrderPercent)*0.01,0);
                        modifyOrders=round(msgPerSec*float(self.modifyOrderPercent)*0.01,0);
                        hiddenOrders=round(msgPerSec*float(self.hiddenOrderPercent)*0.01,0);
                        marketOrders=round(msgPerSec*float(self.marketOrderPercent)*0.01,0);
                        GTDOrders=round(msgPerSec*float(self.GTDOrderPercent)*0.01,0);
                        expiryOrders=round(msgPerSec*float(self.expiryOrderPercent)*0.01,0)+cancelOrders;
                        rejectOrders=round(msgPerSec*float(self.rejectOrderPercent)*0.01,0);
                        #self.printMsg("New="+str(newOrders)+";Cancel="+str(cancelOrders)+";Modifies="+str(modifyOrders)+";Hidden="+str(hiddenOrders)+";Market="+str(marketOrders)+";GTD="+str(GTDOrders)+";Expiry="+str(expiryOrders)+";Rejects="+str(rejectOrders));
                        totalMsgToBeSent=newOrders+cancelOrders+modifyOrders+hiddenOrders+marketOrders+GTDOrders+expiryOrders+rejectOrders;
                        if(totalMsgToBeSent == 0):
                          time.sleep(1);
                          self.checkHeartBeatMsg();
                          continue;
                        sleepInterval=1.0/(totalMsgToBeSent*2);
                        #print(sleepInterval,msgPerSec,totalMsgToBeSent,newOrders,cancelOrders,modifyOrders,hiddenOrders,marketOrders);
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
                                if ((currentTime-batchStartTime).seconds > 1.0):
                                        self.printMsg("Breaking out ");
                                        break;
                                if (newOrders>0):
                                        self.frameNewOrderSingleMsg();
                                        self.alterNewOrderMsg();
                                        self.removeTags(['41']);
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        newOrders=newOrders-1;
                                        msgSent=msgSent+1;
                                        time.sleep(sleepInterval);

                                if (marketOrders>0):
                                        self.frameNewOrderSingleMsg();
                                        self.alterNewMarketOrderMsg();
                                        self.removeTags(['41']);
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        marketOrders=marketOrders-1;
                                        msgSent=msgSent+1;
                                        time.sleep(sleepInterval);

                                if (hiddenOrders>0):
                                        self.frameNewOrderSingleMsg();
                                        self.alterNewHiddenOrderMsg();
                                        self.removeTags(['41']);
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        self.removeTags(['1138']);
                                        hiddenOrders=hiddenOrders-1;
                                        msgSent=msgSent+1;
                                        time.sleep(sleepInterval);

                                if (GTDOrders>0):
                                        self.frameNewOrderSingleMsg();
                                        self.alterNewGTDOrderMsg();
                                        self.removeTags(['41']);
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        self.removeTags(['59']);
                                        GTDOrders=GTDOrders-1;
                                        msgSent=msgSent+1;
                                        time.sleep(sleepInterval);

                                if (expiryOrders>0):
                                        self.frameNewOrderSingleMsg();
                                        self.alterNewExpiryOrderMsg();
                                        self.removeTags(['41']);
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        expiryOrders=expiryOrders-1;
                                        msgSent=msgSent+1;
                                        time.sleep(sleepInterval);

                                if (rejectOrders>0):
                                        self.frameNewOrderSingleMsg();
                                        self.alterNewRejectOrderMsg();
                                        self.removeTags(['41']);
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        rejectOrders=rejectOrders-1;
                                        msgSent=msgSent+1;
                                        time.sleep(sleepInterval);

                                if (cancelOrders>0):
                                        queuedOrderListSize=len(self.queuedCancelOrdersList);
                                        if (queuedOrderListSize==0):
                                                continue;
                                        self.lockObj.acquire(True);
                                        execReport=self.queuedCancelOrdersList.pop(queuedOrderListSize-1);
                                        self.lockObj.release();
                                        self.removeTags(['336','386','44','1']);
                                        self.frameCancelOrderMsg(execReport['11'],execReport['54'],execReport['38'],execReport['55']);
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        cancelOrders=cancelOrders-1;
                                        msgSent=msgSent+1;
                                        time.sleep(sleepInterval);

                                if (modifyOrders>0):
                                        queuedOrderListSize=len(self.queuedModifyOrdersList);
                                        if (queuedOrderListSize==0):
                                                continue;
                                        self.lockObj.acquire(True);
                                        execReport=self.queuedModifyOrdersList.pop(queuedOrderListSize-1);
                                        self.lockObj.release();
                                        self.removeTags(['336','386']);
                                        qty=str(int(execReport['38'])+1);
                                        self.frameModifyOrderMsg(execReport['11'],execReport['54'],execReport['40'],qty,execReport['44'],execReport['55'],execReport['1']);
                                        self.fixSessionobj.sendFIXMsg(self.fixMsgDict);
                                        self.removeTags(['41']);
                                        modifyOrders=modifyOrders-1;
                                        msgSent=msgSent+1;
                                        time.sleep(sleepInterval);

          
                        self.printMsg("Number of messages send in this batch is "+str(msgSent) );
                        self.checkHeartBeatMsg();
                        #Clear of the queuedOrderList queus
                        self.lockObj.acquire(True);
                        nomodifyOrders=int(msgPerSec*int(self.modifyOrderPercent)*0.01);
                        queueSize=len(self.queuedModifyOrdersList);
                        #print(queueSize,nomodifyOrders,nocancelOrders);
                        if(queueSize > nomodifyOrders):
                            for i in list(range(queueSize-nomodifyOrders)):
                                 try:
                                     execReport=self.queuedModifyOrdersList.pop(i);
                                 except Exception:
                                     #self.printMsg("Exception Occured while popping out element at "+str(i));
                                     break;
                        self.lockObj.release();
                        self.lockObj.acquire(True);
                        nocancelOrders=int(msgPerSec*int(self.cancelOrderPercent)*0.01);
                        queueSize=len(self.queuedCancelOrdersList);
                        #print(queueSize,nomodifyOrders,nocancelOrders);
                        if(queueSize > nocancelOrders):
                            for i in list(range(queueSize-nocancelOrders)):
                                 try:
                                     execReport=self.queuedCancelOrdersList.pop(i);
                                 except Exception:
                                     #self.printMsg("Exception Occured while popping out element at "+str(i));
                                     break;
                        self.lockObj.release();
                        currentTime=datetime.datetime.now();
                        if(currentTime-batchStartTime).seconds < 1:
                           microsecondsLeft=1000000.0-((currentTime-batchStartTime).microseconds);
                           if microsecondsLeft>0:
                                self.printMsg("Sleeping before starting the next run");
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


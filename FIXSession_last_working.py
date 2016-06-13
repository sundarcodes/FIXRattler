import socket;
import threading;
import datetime;
import time;
import logging;
import os;

class FIXSession:
        def __init__(self,hostname,port,mbrCode,isLogging):
                self.host=hostname;
                self.port=int(port);
                #Create a socket
                self.clientsocket=socket.socket(socket.AF_INET,socket.SOCK_STREAM);
                self.fixmsgseparator='\001'
                dirName=os.getenv("LT_FIX_LOG_FILES_PATH","/home/tesysop/FIXRattler/logs");
                FIXFileName=dirName+"/FIX_"+mbrCode+".log";
                self.fp=open(FIXFileName,"a");
                self.lockObj=threading.Lock();
                self.pendingMsg="";
                self.loggerObj=logging.getLogger("FIXRattler."+mbrCode);
		self.doLogging=isLogging;

        def connect(self):
                retryCount=0;
                while(retryCount<3):
                   try:
                        self.clientsocket.connect((self.host,self.port));
                        return True;
                   except Exception as e:
                        self.loggerObj.debug( "Not able to connect to "+self.host+":"+str(self.port));
                        retryCount=retryCount+1;
                        time.sleep(1);
                self.loggerObj.info("Tried "+str(retryCount)+" times to connect. But couldn't...Check Server/Port connectivity");
                return False;

        def closeSocket(self):
                 self.clientsocket.close();

        def close(self):
                 self.fp.close();

        def sendFIXMsg(self,FIXmsg):
                #Frame the FIX messages
                #Frame the header first
                fixMsgBody="";
                fixMsgHeader=""
                for i in FIXmsg:
                        tag=i;
                        value=FIXmsg[tag];
                        FIXTagValue=tag+"="+value;
                        if (tag == '49' or tag =='56' or tag == '50' or tag == '34' or tag == '52'):
                                fixMsgHeader=fixMsgHeader+FIXTagValue+self.fixmsgseparator;
                        elif ( not tag == '35'):
                                fixMsgBody=fixMsgBody+FIXTagValue+self.fixmsgseparator;

                #Computer Body Length
                fixMsgBody="35="+FIXmsg['35']+self.fixmsgseparator+fixMsgHeader+fixMsgBody;
                bodyLength=len(fixMsgBody);
                fixMsg="8=FIXT.1.1"+self.fixmsgseparator+"9="+str(bodyLength)+self.fixmsgseparator+fixMsgBody;
                #fixMsg="8=FIX.4.2"+self.fixmsgseparator+"9="+str(bodyLength)+self.fixmsgseparator+fixMsgBody;
                checkSum=0
                for i in list(range(len(fixMsg))):
                        asValue=int(ord(fixMsg[i]));
                        #print asValue
                        checkSum=checkSum+asValue;
                checkSum=checkSum%256;
                chkSum=('{0:03}').format(checkSum)
                fixMsg=fixMsg+"10="+chkSum+self.fixmsgseparator;
                fixMsgi=fixMsg.replace(self.fixmsgseparator,'@');
		if self.doLogging:
                  self.lockObj.acquire(True);
                  timestamp=datetime.datetime.now().strftime("%H:%M:%S.%f");
                  self.fp.write("["+timestamp+":OUT]"+fixMsgi+"\n");
                  self.fp.flush();
                  self.lockObj.release();
                #self.fp.close()
                #sent=self.clientsocket.send(fixMsg)
                bytesMsg=fixMsg.encode('cp1256');
                #print(bytesMsg);
                sent=self.clientsocket.send(bytesMsg)
                if (sent == 0):
                        self.loggerObj.error("Error sending to socket");
                #print "Bytes sent" + str(sent)


        def recvFIXMsg(self):
            try:
                bytesbuf1=[];
                bytesbuf1=self.clientsocket.recv(8192);
                bytesrecvd=len(bytesbuf1);
                msgList=[];
                execReport={};
                bytesLeftTobeParsed=bytesrecvd;
                bytesParsed=0;
                while (bytesLeftTobeParsed>0):
               #Get the body length
                        #Parse the message Till 9=
                        findString="9=";
                        bytesbuf=bytesbuf1.decode('cp1256');
                        bodyLenIndex=bytesbuf.find(findString,bytesParsed);
                        msgTypeIndex=bytesbuf.find("35=",bytesParsed);
                        bodyLen=int(bytesbuf[bodyLenIndex+2:msgTypeIndex-1]);
                        #print bodyLen
                        inMsg=bytesbuf[msgTypeIndex:msgTypeIndex+bodyLen+20];
                        self.pendingMsg=bytesbuf[bodyLen+20:];
                        #print inMsg
			if self.doLogging:
                          inMsg1=bytesbuf[bytesParsed:bytesParsed+11+(msgTypeIndex-bodyLenIndex)+bodyLen+8].replace(self.fixmsgseparator,'@');
                          #inMsg1=bytesbuf.replace(self.fixmsgseparator,'@');
                          #if (inMsg1 != ''):
                                #self.lockObj.acquire(True);
                                #timestamp=datetime.datetime.now().strftime("%H:%M:%S.%f");
                                #self.fp.write("["+timestamp+":IN]"+inMsg1+"\n");
                                #self.fp.flush();
                                #self.lockObj.release();
                        #msgLen=len(inMsg);
                        msgLen=12+8+(msgTypeIndex-bodyLenIndex)+bodyLen;
                        bytesParsed=bytesParsed+msgLen;
                        bytesLeftTobeParsed=bytesrecvd-bytesParsed;
                        #Parse the message
                        #arrList=inMsg1.split('@');
                        arrList=inMsg.split(self.fixmsgseparator);
                        for i in list(range(len(arrList))):
                                tagValue=arrList[i];
                                if (tagValue == ''):
                                        continue;
                                arr=tagValue.split('=');
                                try:
                                  if (len(arr)!=0 ):
                                        execReport[arr[0]]=arr[1];
                                except Exception as e:
                                    print(arr)
                        msgList.append(execReport)
            except Exception:
                 self.loggerObj.error("Exception occured in recv Thread... continuing");
                 #self.loggerObj.info(bytesbuf1,bytesrecvd);
                 return [];
          #print "Length of Message List is: "+str(len(msgList))
            return msgList;


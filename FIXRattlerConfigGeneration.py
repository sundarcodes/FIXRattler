#!/usr/bin/python

import sys;
import time;
import os;
import getopt;
from decimal import Decimal;

def usage():
        print("Usage:FIXRattlerConfigGeneration.py --InputFile <Input File containing Broker/Acc details> --InputOrderbookFile <Orderbooks InputFile> --OutFile <Output FIXSlammer XML File>");

def getTickSize(refPrice):
        if refPrice>=0 and refPrice<=25:
                return 0.05;
        elif refPrice>25 and refPrice<=50:
                return 0.10;
        elif refPrice>50:
                return 0.25;

def adjustToNearestTick(price,flag):
        ticksize=getTickSize(price);
        #Round it of to the nearest tick size
        diff=round(Decimal(str(price))%Decimal(str(ticksize)),2);
        #print price,ticksize,diff
        #Its already a multiple of tick size return the same
        if (diff == 0):
                return price;
        #print price,diff
        if (flag == 1):
                roundedprice=round(price+(ticksize-diff),2);
        elif (flag == 2):
                roundedprice=round(price-diff,2);
        #Chec rounded price again to be doubly sure
        diff=roundedprice%ticksize;
        if (diff == 0):
                return roundedprice;
        else:
                return roundedprice-diff



def createNewDriver(FilePointer,Hostname,Port,SenderCompID,SenderSubID,BuyAcc,SellAcc,CancelPercent,SendRate,Batchsize,TradePercent,MarketOrderPercent,HiddenOrderPercent,ModifyOrderPercent,GTDOrderPercent,ExpiryOrderPercent,RejectOrderPercent,MeasureLatency,LogMessages):
        strWrite="\n\t\t\t<driver name=\""+SenderCompID+"\" >"
        FilePointer.write(strWrite);
        FilePointer.write(formatPropertySring("Target","XStream"));
        FilePointer.write(formatPropertySring("Host",Hostname));
        FilePointer.write(formatPropertySring("Port",Port));
        FilePointer.write(formatPropertySring("SenderCompID",SenderCompID));
        FilePointer.write(formatPropertySring("SenderSubID",SenderSubID));
        FilePointer.write(formatPropertySring("TargetCompID","XSAU"));
        FilePointer.write(formatPropertySring("BuyAccount",BuyAcc));
        FilePointer.write(formatPropertySring("SellAccount",SellAcc));
        FilePointer.write(formatPropertySring("SendRate",SendRate));
        FilePointer.write(formatPropertySring("BatchSize",Batchsize));
        FilePointer.write(formatPropertySring("CancelOrderPercentage",CancelPercent));
        FilePointer.write(formatPropertySring("ModifyOrderPercentage",ModifyOrderPercent));
        FilePointer.write(formatPropertySring("TradePercentage",TradePercent));
        FilePointer.write(formatPropertySring("MarketOrderPercentage",MarketOrderPercent));
        FilePointer.write(formatPropertySring("HiddenOrderPercentage",HiddenOrderPercent));
        FilePointer.write(formatPropertySring("GTDOrderPercentage",GTDOrderPercent));
        FilePointer.write(formatPropertySring("ExpiryOrderPercentage",ExpiryOrderPercent));
        FilePointer.write(formatPropertySring("RejectOrderPercentage",RejectOrderPercent));
        FilePointer.write(formatPropertySring("LogMessages",LogMessages));
        strWrite="\n\t\t\t</driver>"
        FilePointer.write(strWrite);


def formatPropertySring(property,value):
        strWrite="\n\t\t\t\t<property name=\""+property+"\" value=\""+value+"\" />"
        return strWrite;

def getSymbolFormatString(sym,priceRange,ticksize,qty):
        strTmp="\n\t\t\t<symbol name=\""+sym+"\" price=\""+priceRange+"\" ticksize=\""+ticksize+"\" qty=\""+qty+"\" />"
        return strTmp;


# Main Starts here
def main():
        try:
                opts,args=getopt.getopt(sys.argv[1:],"i:o:w:h",["InputFile=","InputOrderbookFile=","OutFile=","help"]);
        except getopt.GetoptError as err:
                print(err);
                usage();
                sys.exit(2);

        # Initialize all variables
        orderbookInputFile="";
        inputFileName="";
        obkarr=[];
        ConfigFileFP=""
        outputLoadFile="";

        #Parse the command line arguments
        for o, a in opts:
                if o in ("-i","--InputFile"):
                        inputFileName = a;
                elif o in ("-o","--InputOrderbookFile"):
                        orderbookInputFile = a;
                elif o in ("-w","--OutFile"):
                        outputLoadFileName = a;
                elif o in ("-h", "--help"):
                        usage()
                        sys.exit()
                else:
                        assert False, "unhandled option"

        # Open the FIX Slammer config file for writing
        ConfigFileFP=open(outputLoadFileName,"w+");

        # Dump the headers and other constant stuffs

        ConfigFileFP.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        ConfigFileFP.write("\n<root>");
        ConfigFileFP.write("\n\t<FixRattler DriverType=\"XStream\" >");
        strTmp=formatPropertySring("BeginString","FIXT.1.1");
        ConfigFileFP.write(strTmp);


        # Read the order books along with the reference prices and store them in a container
        orderbookfp = open(orderbookInputFile,"r");
        cmd="cat "+orderbookInputFile+" | wc -l"
        cmdOutput=os.popen(cmd).read().strip();
        totalOrderbooks=int(cmdOutput);

        orderbookCount=0;

        #Add the symbol Start Tag
        ConfigFileFP.write("\n\n\t\t");
        ConfigFileFP.write("<symbols>");

        minOrderRatio=0.0;
        orderRatioMainSymbols=0.0;
        obkCount_Rest_Category=0;

        # For each line in order book file, split and get the orderbook and reference price
        for line in orderbookfp:
                arr=line.strip().split(":");
                obksym=arr[0].strip('"');
                obkrefPrice=float(arr[1]);
		obkRatio=arr[2].strip('"');
                qty=arr[3].strip('"');
                if (obkRatio == "EQ"):
                        obkCount_Rest_Category=obkCount_Rest_Category+1;
                        obkRatio="";
                elif (obkRatio == "0"):
                        continue;
                else:
                        orderRatioMainSymbols=orderRatioMainSymbols+float(obkRatio);
                orderbookCount+=1;
                orderTuple=[obksym,obkrefPrice,obkRatio,qty];
                obkarr.append(orderTuple);


        #Compute the order ratio for others category
        orderBookRatioOthers=100-orderRatioMainSymbols;
        #print "Order Main Symbols:"+str(orderRatioMainSymbols);
        minOrderRatio=orderBookRatioOthers/obkCount_Rest_Category;
        #print "Minimum Order Ratio:"+str(minOrderRatio);
        numberOfLinesToWrite=0;

        #There should be one line for minOrderRatio in the FIX slammer file

        for i in range(0,orderbookCount):
                orderTuple=obkarr[i];
                obksym=orderTuple[0];
                obkrefPrice=orderTuple[1];
                obkRatio=orderTuple[2];
                qty=orderTuple[3];
                obkRatioOthers=0.0;
                if (obkRatio == ""):
                        obkRatioOthers=minOrderRatio;
                        numberOfLinesToWrite=1;
                else:
                        numberOfLinesToWrite=round(float(obkRatio)/minOrderRatio,0);

                #Scale is minOrderRatio is 1 Line in FIX Slammer Config File

                ticksize=getTickSize(obkrefPrice);
                minPrice=obkrefPrice-0.1*obkrefPrice;
                minPrice=adjustToNearestTick(minPrice,1)
                #print minPrice
                maxPrice=obkrefPrice+0.1*obkrefPrice;
                maxPrice=adjustToNearestTick(maxPrice,2)
                #print maxPrice
                strPriceRange=str(minPrice)+","+str(maxPrice)
                strTmp=getSymbolFormatString(obksym,strPriceRange,str(ticksize),qty);
                #print obksym,numberOfLinesToWrite
                while(numberOfLinesToWrite>0):
                        ConfigFileFP.write(strTmp);
                        numberOfLinesToWrite=numberOfLinesToWrite-1;


        #Add the symbol End Tag
        ConfigFileFP.write("\n\t\t</symbols>");

        #Hostname,Port,SenderCompID,SenderSubID,BuyAcc,SellAcc,CancelPercent,SendRate,Batchsize,TradePercent,TimeInForce,MeasureLatency,LogMessages
        #sa01xubt01,45688,001,0114T01,0109ACC4,0109ACC3,0,30,25,100,0,true,true

        #Add the drivers Start Tag
        ConfigFileFP.write("\n\n\t\t<drivers>");

        # Read the input file
        inputfilefp = open(inputFileName,"r");
        # For each line in the file, split and get the the required fields
        for line in inputfilefp:
                if line.startswith("#"):
                        pass;
                strTmp=line.strip();
                arr=strTmp.split(":");
                hostname=arr[0];
                port=arr[1];
                senderCompID=arr[2];
                senderSubID=arr[3];
                buyAcc=arr[4];
                sellAcc=arr[5];
                cancelPercent=arr[6];
                sendRate=arr[7];
                batchSize=arr[8];
                tradePercent=arr[9];
                marketOrderPercent=arr[10];
                hiddenOrderPercent=arr[11];
                modifyOrderPercent=arr[12];
                GTDOrderPercent=arr[13];
                ExpiryOrderPercent=arr[14];
                RejectOrderPercent=arr[15];
                measureLatency=arr[16];
                logMessages=arr[17];
                createNewDriver(ConfigFileFP,hostname,port,senderCompID,senderSubID,buyAcc,sellAcc,cancelPercent,sendRate,batchSize,tradePercent,marketOrderPercent,hiddenOrderPercent,modifyOrderPercent,GTDOrderPercent,ExpiryOrderPercent,RejectOrderPercent,measureLatency,logMessages)

        #Add the drivers End Tag
        ConfigFileFP.write("\n\n\t\t</drivers>");


        # Dump the footers
        ConfigFileFP.write("\n\n\t</FixRattler>");
        ConfigFileFP.write("\n</root>\n");

        orderbookfp.close()
        inputfilefp.close()
        ConfigFileFP.close()

if __name__ == "__main__":
    main()


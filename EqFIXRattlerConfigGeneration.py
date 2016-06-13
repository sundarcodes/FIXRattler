#!/usr/bin/python
import sys;
import time;
import os;
import getopt;

def usage():
        print("Usage:EqFIXRattlerConfigGeneration.py --InputFile <Input File containing Broker/Acc details> --InputOrderbookFile <Orderbooks InputFile> --OutFile <Output FIXSlammer XML File>");


def createNewDriver(FilePointer,Hostname,Port,SenderCompID,SenderSubID,BuyAcc,SellAcc,SendRate,U10Percent,U12Percent,U14Percent,U16Percent,U18Percent,U20Percent,U22Percent,NINs,MurabhaAcc,NINAccList,LogMessages):
        strWrite="\n\t\t\t<driver name=\""+SenderCompID+"\" >"
        FilePointer.write(strWrite);
        FilePointer.write(formatPropertySring("Target","Equator"));
        FilePointer.write(formatPropertySring("Host",Hostname));
        FilePointer.write(formatPropertySring("Port",Port));
        FilePointer.write(formatPropertySring("SenderCompID",SenderCompID));
        FilePointer.write(formatPropertySring("SenderSubID",SenderSubID));
        FilePointer.write(formatPropertySring("TargetCompID","998"));
        FilePointer.write(formatPropertySring("BuyAccount",BuyAcc));
        FilePointer.write(formatPropertySring("SellAccount",SellAcc));
        FilePointer.write(formatPropertySring("SendRate",SendRate));
        FilePointer.write(formatPropertySring("U10Percentage",U10Percent));
        FilePointer.write(formatPropertySring("U12Percentage",U12Percent));
        FilePointer.write(formatPropertySring("U14Percentage",U14Percent));
        FilePointer.write(formatPropertySring("U16Percentage",U16Percent));
        FilePointer.write(formatPropertySring("U18Percentage",U18Percent));
        FilePointer.write(formatPropertySring("U20Percentage",U20Percent));
        FilePointer.write(formatPropertySring("U22Percentage",U22Percent));
        FilePointer.write(formatPropertySring("NIN",NINs));
        FilePointer.write(formatPropertySring("Murabha",MurabhaAcc));
        FilePointer.write(formatPropertySring("NINAccList",NINAccList));
        FilePointer.write(formatPropertySring("LogMessages",LogMessages));
        strWrite="\n\t\t\t</driver>"
        FilePointer.write(strWrite);


def formatPropertySring(property,value):
        strWrite="\n\t\t\t\t<property name=\""+property+"\" value=\""+value+"\" />"
        return strWrite;

def getSymbolFormatString(sym,priceRange,ticksize):
        strTmp="\n\t\t\t<symbol name=\""+sym+"\" price=\""+priceRange+"\" ticksize=\""+ticksize+"\" />"
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
        ConfigFileFP.write("\n\t<FixRattler DriverType=\"EqFIX\" >");
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

        # For each line in order book file, split and get the orderbook and reference price
        for line in orderbookfp:
                arr=line.strip().split(":");
                obksym=arr[0].strip('"');
                strTmp=getSymbolFormatString(obksym,"","");
                ConfigFileFP.write(strTmp);


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
                sendRate=arr[6];
                U10Percent=arr[7];
                U12Percent=arr[8];
                U14Percent=arr[9];
                U16Percent=arr[10];
                U18Percent=arr[11];
                U20Percent=arr[12];
                U22Percent=arr[13];
                NIN=arr[14];
                Murabha=arr[15];
                NINAccList=arr[16];
                logMessages=arr[17];
                #orderQty="100";

                createNewDriver(ConfigFileFP,hostname,port,senderCompID,senderSubID,buyAcc,sellAcc,sendRate,U10Percent,U12Percent,U14Percent,U16Percent,U18Percent,U20Percent,U22Percent,NIN,Murabha,NINAccList,logMessages);

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

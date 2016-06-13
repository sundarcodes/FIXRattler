import os;
import sys;


def getEnvValue(envVariableName):
        tmpStr=os.getenv(envVariableName,"");
        if (tmpStr==""):
                return ".";
        else:
                return tmpStr;

def getHostNameRunning(processName):
        #Open the servers.list
        fileName=os.getenv("ASTSDIR")+"/etc/servers.list";
        fp=open(fileName)
        searchString="["+processName;
        found=False;
        hostNameArr=[];

        for line in fp:
                strLine=line.strip();

                if (strLine.startswith(searchString)):
                        found=True;
                        continue;

                if (strLine.startswith("[")):
                        if (found):
                                found=False;
                                break;
                        else:
                                continue;

                if (found):
                        arr=strLine.split(",");
                        hostname=arr[0];
                        if (hostname =="" or (hostname in hostNameArr)):
                                continue;
                        else:
                                hostNameArr.append(hostname);


        return hostNameArr;


#For Testing

#arr=getHostNameRunning("tsmrte");
#print arr


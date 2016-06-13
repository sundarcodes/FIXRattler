import xml.etree.ElementTree as ET

class XMLReader:
        def __init__(self,XMLFileName):
                try:
                        self.elementTree=ET.parse(XMLFileName)
                except Exception:
                        print("ExceptionOccured while Parsing XML File");
                        raise Exception;
                        #print "ExceptionOccured while Parsing XML File";
                self.symbolList=[];
                self.driversList=[];

        def getSymbolList(self):
                for child in self.elementTree.getiterator('symbol'):
                        self.symbolList.append(child.attrib)
                return self.symbolList;

        def getDriversList(self):
                driverConfigList=[];
                for child in self.elementTree.getiterator('driver'):
                        for subchild in child.getiterator():
                                driverConfigList.append(subchild.attrib);
                        self.driversList.append(driverConfigList);
                        driverConfigList=[];
                return self.driversList;

        def getDriverType(self):
                element=self.elementTree.find('FixRattler');
                driverType=element.get('DriverType');
                return driverType;

        def getDriverProperties(self,driverName):
                driverConfigList=[];
                for child in self.elementTree.getiterator('driver'):
                    if (child.get('name') == driverName):
                      for subchild in child.getiterator():
                         driverConfigList.append(subchild.attrib);
                      return driverConfigList;
                


#Test XML Reader
#obj=XMLReader("FIXSlammerRun1.xml");
#print obj.getSymbolList();
#print obj.getDriversList();



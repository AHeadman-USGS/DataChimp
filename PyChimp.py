#! /usr/bin/python3
# -*- coding: utf-8 -*-

from PyQt5 import QtCore, QtGui, QtWidgets
import os, isodate
from MesoPy import Meso
import lxml.etree as et
from suds.client import Client
from urllib.request import urlopen
from datetime import datetime, timedelta

# Global variables developed as "default" setting Widgets for existing models.
tempListIn = []
precipListIn=[]
snowListIn=[]
runoffListIn=[]
key=''

def DataChimp(file, start, TList, PList, SWEList, RList, key):
    
    m = Meso(token=key)

    def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + timedelta(n)
            
    def readLast(fileName):
        fileName = open(fileName, 'r')
        lineList = fileName.readlines()
        date = lineList[-1]
        if int(date[5:7]) < 10:
            if int(date[7:9]) < 10:
                dateobj = datetime.strptime(date[:8], '%Y %m %d')
            else:
                dateobj = datetime.strptime(date[:9], '%Y %m %d')
        else:
            dateobj = datetime.strptime(date[:10], '%Y %m %d')
        return dateobj
    
    def metaPullMeso(station):
        try:
            meta = m.metadata(stid=station)
            x=meta['STATION'][0]
            name = x['NAME']
            lat = x['LATITUDE']
            lon = x['LONGITUDE']
            elev = x['ELEV_DEM']
            periodStart = x['PERIOD_OF_RECORD']['start']
            periodEnd = x['PERIOD_OF_RECORD']['end']
            return name, elev, lat, lon, periodStart, periodEnd
        except:
            return "None","None","None","None","None","None"
    
    def tminPull(station, start_date):
        try:
            end_d = start_date + timedelta(hours=32)
            stats = m.time_stats(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                                 end=end_d.strftime("%Y%m%d%H%M"), type='all', units="temp|F")
            x=stats['STATION'][0]
            tmin = x['STATISTICS']['air_temp_set_1']['minimum']
            return tmin
        except:
            return -100.0
    
    def tmaxPull(station, start_date):
        try:
            end_d = start_date + timedelta(hours=32)
            stats = m.time_stats(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                                 end=end_d.strftime("%Y%m%d%H%M"), type='all', units="temp|F")
            x=stats['STATION'][0]
            tmax = x['STATISTICS']['air_temp_set_1']['maximum']
            return tmax
        except:
            return -100.0
    
    def precipPull(station, start_date):
        try:
            end_d = start_date + timedelta(hours=32)
            precip = m.precip(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                                 end=end_d.strftime("%Y%m%d%H%M"), units='precip|in')
            y=precip['STATION'][0]
            PrecT = y['OBSERVATIONS']['total_precip_value_1']
            return PrecT
        except:
            return -9.0
        
    def PRCPLookUp(station, start):
        try:
            NRCS = Client('https://www.wcc.nrcs.usda.gov/awdbWebService/services?WSDL')
            data = NRCS.service.getData(
                    heightDepth = None,        
                    stationTriplets=station,
                    elementCd='PRCP',
                    ordinal=1,
                    duration='DAILY',
                    getFlags=False,
                    alwaysReturnDailyFeb29=False,
                    beginDate=start,
                    endDate=start)
            x = data[0]
            prcp = x['values'][0]
            return prcp
        except:
            return -9.0
    
    def SWELookUp(station, start):
        try:
            NRCS = Client('https://www.wcc.nrcs.usda.gov/awdbWebService/services?WSDL')
            data = NRCS.service.getData(
                    heightDepth = None,        
                    stationTriplets=station,
                    elementCd='WTEQ',
                    ordinal=1,
                    duration='DAILY',
                    getFlags=False,
                    alwaysReturnDailyFeb29=False,
                    beginDate=start,
                    endDate=start)
            
            x = data[0]
            swe = x['values'][0]
            return swe
        except:
            return -9.0
        
    def runOffPullv2(station, start_date, end_date):
        try:
            if start_date == end_date:
                return -99.0
            else:
                datetime_formatter = isodate.datetime_isoformat
                start = datetime_formatter(start_date)
                start = start[0:10]
                dvXml = urlopen('https://waterservices.usgs.gov/nwis/dv/?format=waterml&site='
                                +station+'&parameterCd=00060&startDT='+start+'&endDT='+start)
                tree = et.parse(dvXml)
                root = tree.getroot()
                values = root.findall('.//{http://www.cuahsi.org/waterML/1.1/}values')
                for value in values:
                    returned = value.find('{http://www.cuahsi.org/waterML/1.1/}value').text
                    if float(returned) < 0:
                        return -99.0
                    elif returned is None:
                        return -99.0 
                    else:
                        return returned
        except:
            return -99.0

    if os.path.isfile(file) == True:
        # if the .dat file exists this updates the file with the information
        # from the last entry to the current day
        
        NewStart = readLast(file)
        start_date = NewStart+timedelta(days=1)
        end_date = datetime(datetime.now().year, datetime.now().month, datetime.now().day,0,0)
        if start_date == end_date:
            print("No update needed")
        else:
            file = open(file, 'a')
            for single_date in daterange(start_date, end_date):
                print("Processing "+single_date)
                TminList=[]
                TmaxList=[]
                PrecipList=[]
                SnowList=[]
                ROList=[]
                print("Processing "+ str(single_date))
                # mesowest uses UTC, this adjusts for PST to UTC
                start_d = single_date #+ timedelta(hours=8)
                for station in TList:
                    if station == "xxxxx":
                        TminList.append('-99.9')
                        TmaxList.append('-99.9')
                    else:
                        tmin = tminPull(station, start_d)
                        TminList.append(tmin)
                        tmax = tmaxPull(station, start_d)
                        TmaxList.append(tmax)
                for station in PList:
                    if station == "xxxxx":
                        PrecipList.append('-9.9')
                    else:
                        precip = precipPull (station, start_d)
                        PrecipList.append(precip)
                for station in SWEList:
                    if station == "xxxxx":
                        PrecipList.append('-9.9')
                    else:    
                        precip = PRCPLookUp (station, start_d)
                        PrecipList.append(precip)
                for station in SWEList:
                    if station == "xxxxx":
                        SnowList.append('-9.9')
                    else:
                        SWE = SWELookUp(station, single_date)
                        SnowList.append(SWE)
                for station in RList:
                    if station == "xxxxx":
                        ROList.append('-9.9')
                    else:
                        runoffval = runOffPullv2(station, single_date, end_date)
                        ROList.append(runoffval)
                        for (i,val) in enumerate(ROList):
                            if val == None:
                                ROList[i] = 0.0
                file.write(str(single_date.year)+" "+str(single_date.month)+" "+str(single_date.day)+" "
                           +str(0)+" "+str(0)+" "+str(0)+" "+" ".join(map(str,TmaxList))+" "+" ".join(map(str,TminList))+
                           " "+ " ".join(map(str,PrecipList))+" "+" ".join(map(str,SnowList))+" "+" ".join(map(str,ROList))+'\n')
            file.close()
    else:
        ## if no .dat file is found this begins the process of creating one    
        file = open(file,"w")
        file.write("// Station metadata:"+'\n')
        file.write("// ID Name Type Latitude Longitude Elevation"+'\n')
        
        #creating the metadata header
        for station in TList:
            a,b,c,d,e,f = metaPullMeso(station)
            file.write("// "+str(station)+" "+str(a)+" "+"TMAX "+str(b)+" "+str(c)+" "+str(d)+'\n')
        for station in TList:
            a,b,c,d,e,f = metaPullMeso(station)
            file.write("// "+str(station)+" "+str(a)+" "+"TMIN "+str(b)+" "+str(c)+" "+str(d)+'\n')
        for station in PList:
            a,b,c,d,e,f = metaPullMeso(station)
            file.write("// "+str(station)+" "+str(a)+" "+"PRECIP "+str(b)+" "+str(c)+" "+str(d)+'\n')    
    
        #creating the data object IDs
        
        file.write("tmax "+str(len(TList))+'\n')
        file.write("tmin "+str(len(TList))+'\n')
        file.write("precip "+str(len(PList)+len(SWEList))+'\n')
        file.write("snowdepth "+str(len(SWEList))+'\n')
        file.write("runoff "+str(len(RList))+'\n') 
        file.write("########################################"+'\n')   
        
        ## datetime object provides previous X years/days/months of data
            
        start_date = datetime.strptime(start, '%m/%d/%Y')
        start_date = datetime(start_date.year, start_date.month, start_date.day, 0,0)
        end_date = datetime(datetime.now().year, datetime.now().month, datetime.now().day, 0, 0)
        
        for single_date in daterange(start_date, end_date):
            print("Processing "+str(single_date))
            TminList=[]
            TmaxList=[]
            PrecipList=[]
            SnowList=[]
            ROList=[]
            
            # mesowest uses UTC, this adjusts for PST to UTC
            start_d = single_date
            for station in TList:
                if station == "xxxxx":
                    TminList.append('-99.9')
                    TmaxList.append('-99.9')
                else:
                    tmin = tminPull(station, start_d)
                    TminList.append(tmin)
                    tmax = tmaxPull(station, start_d)
                    TmaxList.append(tmax)
            for station in PList:
                if station == "xxxxx":
                    PrecipList.append('-9.9')
                else:
                    precip = precipPull (station, start_d)
                    PrecipList.append(precip)
            for station in SWEList:
                if station == "xxxxx":
                    PrecipList.append('-9.9')
                else:    
                    precip = PRCPLookUp (station, start_d)
                    PrecipList.append(precip)
            for station in SWEList:
                if station == "xxxxx":
                    SnowList.append('-9.9')
                else:
                    SWE = SWELookUp(station, single_date)
                    SnowList.append(SWE)
            for station in RList:
                if station == "xxxxx":
                    ROList.append('-9.9')
                else:
                    runoffval = runOffPullv2(station, single_date, end_date)
                    ROList.append(runoffval)
                    for (i,val) in enumerate(ROList):
                        if val == None:
                            ROList[i] = 0.0
            file.write(str(single_date.year)+" "+str(single_date.month)+" "+str(single_date.day)+" "
                           +str(0)+" "+str(0)+" "+str(0)+" "+" ".join(map(str,TmaxList))+" "+" ".join(map(str,TminList))+
                           " "+ " ".join(map(str,PrecipList))+" "+" ".join(map(str,SnowList))+" "+" ".join(map(str,ROList))+'\n')
        file.close()
    file.close()


class Ui_Form(object):
    def setupUi(self, Form):
        Form.setObjectName("DataChimp Widget")
        Form.resize(840, 509)
        self.goButton = QtWidgets.QPushButton(Form)
        self.goButton.setGeometry(QtCore.QRect(30, 400, 75, 27))
        self.goButton.setObjectName("goButton")
        self.fileNameLineEdit = QtWidgets.QLineEdit(Form)
        self.fileNameLineEdit.setGeometry(QtCore.QRect(30, 50, 113, 20))
        self.fileNameLineEdit.setObjectName("fileNameLineEdit")
        self.dateEdit = QtWidgets.QDateEdit(Form)
        self.dateEdit.setGeometry(QtCore.QRect(160, 50, 110, 21))
        self.dateEdit.setDateTime(QtCore.QDateTime(QtCore.QDate(2000, 10, 1), QtCore.QTime(0, 0, 0)))
        self.dateEdit.setObjectName("dateEdit")
        self.apiKeyEdit = QtWidgets.QLineEdit(Form)
        self.apiKeyEdit.setGeometry(QtCore.QRect(300, 50, 200, 21))
        self.apiKeyEdit.setObjectName("apiKeyEdit")
        self.label = QtWidgets.QLabel(Form)
        self.label.setGeometry(QtCore.QRect(160, 30, 61, 20))
        self.label.setObjectName("label")
        self.label_2 = QtWidgets.QLabel(Form)
        self.label_2.setGeometry(QtCore.QRect(30, 30, 51, 16))
        self.label_2.setObjectName("label_2")
        self.apilabel = QtWidgets.QLabel(Form)
        self.apilabel.setGeometry(QtCore.QRect(300, 30, 61, 20))
        self.apilabel.setObjectName("label")
        self.layoutWidget = QtWidgets.QWidget(Form)
        self.layoutWidget.setGeometry(QtCore.QRect(170, 80, 121, 213))
        self.layoutWidget.setObjectName("layoutWidget")
        self.verticalLayout_2 = QtWidgets.QVBoxLayout(self.layoutWidget)
        self.verticalLayout_2.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.label_4 = QtWidgets.QLabel(self.layoutWidget)
        self.label_4.setObjectName("label_4")
        self.verticalLayout_2.addWidget(self.label_4)
        self.listWidget_2 = QtWidgets.QListWidget(self.layoutWidget)
        self.listWidget_2.setObjectName("listWidget_2")
        self.verticalLayout_2.addWidget(self.listWidget_2)
        self.layoutWidget_2 = QtWidgets.QWidget(Form)
        self.layoutWidget_2.setGeometry(QtCore.QRect(310, 80, 121, 213))
        self.layoutWidget_2.setObjectName("layoutWidget_2")
        self.verticalLayout_3 = QtWidgets.QVBoxLayout(self.layoutWidget_2)
        self.verticalLayout_3.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        self.label_5 = QtWidgets.QLabel(self.layoutWidget_2)
        self.label_5.setObjectName("label_5")
        self.verticalLayout_3.addWidget(self.label_5)
        self.listWidget_3 = QtWidgets.QListWidget(self.layoutWidget_2)
        self.listWidget_3.setObjectName("listWidget_3")
        self.verticalLayout_3.addWidget(self.listWidget_3)
        self.layoutWidget_3 = QtWidgets.QWidget(Form)
        self.layoutWidget_3.setGeometry(QtCore.QRect(450, 80, 121, 213))
        self.layoutWidget_3.setObjectName("layoutWidget_3")
        self.verticalLayout_4 = QtWidgets.QVBoxLayout(self.layoutWidget_3)
        self.verticalLayout_4.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout_4.setObjectName("verticalLayout_4")
        self.label_6 = QtWidgets.QLabel(self.layoutWidget_3)
        self.label_6.setObjectName("label_6")
        self.verticalLayout_4.addWidget(self.label_6)
        self.listWidget_4 = QtWidgets.QListWidget(self.layoutWidget_3)
        self.listWidget_4.setObjectName("listWidget_4")
        self.verticalLayout_4.addWidget(self.listWidget_4)
        self.tempLineEdit = QtWidgets.QLineEdit(Form)
        self.tempLineEdit.setGeometry(QtCore.QRect(30, 300, 113, 20))
        self.tempLineEdit.setObjectName("tempLineEdit")
        self.precipLineEdit = QtWidgets.QLineEdit(Form)
        self.precipLineEdit.setGeometry(QtCore.QRect(170, 300, 113, 20))
        self.precipLineEdit.setObjectName("precipLineEdit")
        self.snotelLineEdit = QtWidgets.QLineEdit(Form)
        self.snotelLineEdit.setGeometry(QtCore.QRect(310, 300, 113, 20))
        self.snotelLineEdit.setObjectName("snotelLineEdit")
        self.runoffLineEdit = QtWidgets.QLineEdit(Form)
        self.runoffLineEdit.setGeometry(QtCore.QRect(450, 300, 113, 20))
        self.runoffLineEdit.setObjectName("runoffLineEdit")
        self.widget = QtWidgets.QWidget(Form)
        self.widget.setGeometry(QtCore.QRect(30, 80, 121, 213))
        self.widget.setObjectName("widget")
        self.verticalLayout = QtWidgets.QVBoxLayout(self.widget)
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout.setObjectName("verticalLayout")
        self.label_3 = QtWidgets.QLabel(self.widget)
        self.label_3.setObjectName("label_3")
        self.verticalLayout.addWidget(self.label_3)
        self.listWidget = QtWidgets.QListWidget(self.widget)
        self.listWidget.setObjectName("listWidget")
        self.verticalLayout.addWidget(self.listWidget)
        self.tempAddButton = QtWidgets.QPushButton(Form)
        self.tempAddButton.setGeometry(QtCore.QRect(30, 325, 101, 21))
        self.tempAddButton.setObjectName("tempAddButton")
        self.precipAddButton = QtWidgets.QPushButton(Form)
        self.precipAddButton.setGeometry(QtCore.QRect(170, 325, 101, 21))
        self.precipAddButton.setObjectName("precipAddButton")
        self.snotelAddButton = QtWidgets.QPushButton(Form)
        self.snotelAddButton.setGeometry(QtCore.QRect(310, 325, 101, 21))
        self.snotelAddButton.setObjectName("snotelAddButton")
        self.runoffAddButton = QtWidgets.QPushButton(Form)
        self.runoffAddButton.setGeometry(QtCore.QRect(450, 325, 101, 21))
        self.runoffAddButton.setObjectName("runoffAddButton")
        self.tempRmSelected = QtWidgets.QPushButton(Form)
        self.tempRmSelected.setGeometry(QtCore.QRect(30, 347, 101, 21))
        self.tempRmSelected.setObjectName("tempRmSelected")
        self.precipRmSelected = QtWidgets.QPushButton(Form)
        self.precipRmSelected.setGeometry(QtCore.QRect(170, 347, 101, 21))
        self.precipRmSelected.setObjectName("precipRmSelected")
        self.snotelRmSelected = QtWidgets.QPushButton(Form)
        self.snotelRmSelected.setGeometry(QtCore.QRect(310, 347, 101, 21))
        self.snotelRmSelected.setObjectName("snotelRmSelected")
        self.runoffRmSelected = QtWidgets.QPushButton(Form)
        self.runoffRmSelected.setGeometry(QtCore.QRect(450, 347, 101, 21))
        self.runoffRmSelected.setObjectName("runoffRmSelected")
        self.retranslateUi(Form)
        self.goButton.clicked.connect(self.io)
        
        # If global lists are populated this will add them.
        self.listWidget.addItems(tempListIn)
        self.listWidget_2.addItems(precipListIn)
        self.listWidget_3.addItems(snowListIn)
        self.listWidget_4.addItems(runoffListIn)
        self.apiKeyEdit.setText(key)
         
        # Handling for the station lists - adding items
        
        self.tempLineEdit.returnPressed.connect(self.createTempItem)
        self.tempLineEdit.returnPressed.connect(self.tempLineEdit.clear)
        self.precipLineEdit.returnPressed.connect(self.createPrecipItem)
        self.precipLineEdit.returnPressed.connect(self.precipLineEdit.clear)
        self.snotelLineEdit.returnPressed.connect(self.createSnotelItem)
        self.snotelLineEdit.returnPressed.connect(self.snotelLineEdit.clear)
        self.runoffLineEdit.returnPressed.connect(self.createRunoffItem)
        self.runoffLineEdit.returnPressed.connect(self.runoffLineEdit.clear)
        self.tempAddButton.clicked.connect(self.createTempItem)
        self.tempAddButton.clicked.connect(self.tempLineEdit.clear)
        self.precipAddButton.clicked.connect(self.createPrecipItem)
        self.precipAddButton.clicked.connect(self.precipLineEdit.clear)
        self.snotelAddButton.clicked.connect(self.createSnotelItem)
        self.snotelAddButton.clicked.connect(self.snotelLineEdit.clear)
        self.runoffAddButton.clicked.connect(self.createRunoffItem)
        self.runoffAddButton.clicked.connect(self.runoffLineEdit.clear)
        
        # Handling for station lists - deleting items
        self.tempRmSelected.clicked.connect(self.removeSelTemp)
        self.precipRmSelected.clicked.connect(self.removeSelPrecip)
        self.snotelRmSelected.clicked.connect(self.removeSelSnotel)
        self.runoffRmSelected.clicked.connect(self.removeSelRunoff)
        
        QtCore.QMetaObject.connectSlotsByName(Form)
    
    # Master IO for the chimp.
    def io(self):
        fileName = self.fileNameLineEdit.text()
        Date = self.dateEdit.text()
        key = self.apiKeyEdit.text()
        Temps = []
        Precip = []
        Snotel = []
        Runoff = []
        for index in range(self.listWidget.count()):
            Temps.append(self.listWidget.item(index).text())
        for index in range(self.listWidget_2.count()):
            Precip.append(self.listWidget_2.item(index).text())
        for index in range(self.listWidget_3.count()):
            Snotel.append(self.listWidget_3.item(index).text())
        for index in range(self.listWidget_4.count()):
            Runoff.append(self.listWidget_4.item(index).text())
        if len(Temps) == 0:
            Temps.append('xxxxx')
        if len(Precip) == 0:
            Precip.append('xxxxx')
        if len(Snotel) == 0:
            Snotel.append('xxxxx')
        if len(Runoff) == 0:
            Runoff.append('xxxxx')
        DataChimp(fileName, Date, Temps, Precip, Snotel, Runoff, key)
    
    #Functions for adding items to list.
    def createTempItem(self):
        item = QtWidgets.QListWidgetItem(self.tempLineEdit.text())
        self.listWidget.addItem(item)
    def createPrecipItem(self):
        item = QtWidgets.QListWidgetItem(self.precipLineEdit.text())
        self.listWidget_2.addItem(item)
    def createSnotelItem(self):
        item = QtWidgets.QListWidgetItem(self.snotelLineEdit.text())
        self.listWidget_3.addItem(item)  
    def createRunoffItem(self):
        item = QtWidgets.QListWidgetItem(self.runoffLineEdit.text())
        self.listWidget_4.addItem(item)
   
    #Functions for deleting items. Only will delete 1 item at a time, I'm ok with this.
    def removeSelTemp(self):
        self.listWidget.takeItem(self.listWidget.currentRow()) 
    def removeSelPrecip(self):
        self.listWidget_2.takeItem(self.listWidget_2.currentRow())         
    def removeSelSnotel(self):
        self.listWidget_3.takeItem(self.listWidget_3.currentRow())     
    def removeSelRunoff(self):
        self.listWidget_4.takeItem(self.listWidget_4.currentRow())
    
    #Stuff that give the things names so you know what they do.    
    def retranslateUi(self, Form):
        _translate = QtCore.QCoreApplication.translate
        Form.setWindowTitle(_translate("Form", "DataChimp Widget"))
        self.goButton.setText(_translate("Form", "Go"))
        self.label.setText(_translate("Form", "Start Date"))
        self.label_2.setText(_translate("Form", "File Name"))
        self.label_3.setText(_translate("Form", "Temp Stations"))
        self.label_4.setText(_translate("Form", "Precip Stations"))
        self.label_5.setText(_translate("Form", "Snotel Stations"))
        self.label_6.setText(_translate("Form", "Runoff"))   
        self.apilabel.setText(_translate("Form", "API Key"))
        self.tempAddButton.setText(_translate("Form", "Add"))
        self.precipAddButton.setText(_translate("Form", "Add"))
        self.snotelAddButton.setText(_translate("Form", "Add"))
        self.runoffAddButton.setText(_translate("Form", "Add"))
        self.tempRmSelected.setText(_translate("Form", "Remove Selected"))
        self.precipRmSelected.setText(_translate("Form", "Remove Selected"))
        self.snotelRmSelected.setText(_translate("Form", "Remove Selected"))
        self.runoffRmSelected.setText(_translate("Form", "Remove Selected"))
        
        
if __name__ == "__main__":
    import sys
    app=0
    app = QtWidgets.QApplication(sys.argv)
    Form = QtWidgets.QWidget()
    ui = Ui_Form()
    ui.setupUi(Form)
    Form.show()
    app.exec_()
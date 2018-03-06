# Compatibility note: This script uses WaterML1.1 as a standard.
# If MesoPy breaks it's likely they have an expiring token, so start at 
# that point.  If the data file is full of -999.0 (null value) then something
# is broken. 

import os, isodate
from MesoPy import Meso
import lxml.etree as et
from suds.client import Client
from urllib.request import urlopen
from datetime import datetime, timedelta

# mesowest api Token, use this until it breaks.

m = Meso(token='81a655ec55cf450a87caa21486692770')

# functions
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
    meta = m.metadata(stid=station)
    x=meta['STATION'][0]
    name = x['NAME']
    lat = x['LATITUDE']
    lon = x['LONGITUDE']
    elev = x['ELEV_DEM']
    periodStart = x['PERIOD_OF_RECORD']['start']
    periodEnd = x['PERIOD_OF_RECORD']['end']
    return name, elev, lat, lon, periodStart, periodEnd

def tminPull(station, start_date):
    try:
        end_d = start_date + timedelta(hours=32)
        stats = m.time_stats(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                             end=end_d.strftime("%Y%m%d%H%M"), type='all', units="temp|F")
        x=stats['STATION'][0]
        tmin = x['STATISTICS']['air_temp_set_1']['minimum']
        return tmin
    except:
        return -99.0

def tmaxPull(station, start_date):
    try:
        end_d = start_date + timedelta(hours=32)
        stats = m.time_stats(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                             end=end_d.strftime("%Y%m%d%H%M"), type='all', units="temp|F")
        x=stats['STATION'][0]
        tmax = x['STATISTICS']['air_temp_set_1']['maximum']
        return tmax
    except:
        return -99.0

def precipPull(station, start_date):
    try:
        end_d = start_date + timedelta(hours=32)
        precip = m.precip(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                             end=end_d.strftime("%Y%m%d%H%M"), units='precip|in')
        y=precip['STATION'][0]
        PrecT = y['OBSERVATIONS']['total_precip_value_1']
        return PrecT
    except:
        return -99.0
    
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
        return -99.0

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
        return -99.0
    
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
    
# Current working lists for weather stations.

TList = ['ANEW1','SAMW1', 'KOMK', 'CDAW1', 'FBFW1','KMFW1', 'LEFW1']
PList = ['ANEW1', 'KOMK', 'CDAW1', 'FBFW1','KMFW1', 'LEFW1']
SWEList = ['728:WA:SNTL', '1259:WA:SNTL']
RList = ['12446150', '12446400']
file = "SalmonCreek_Long.dat"

if os.path.isfile(file) == True:
    # if the .dat file exists this updates the file with the information
    # from the last entry to the current day
    
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day
    hour = 0
    minute = 0
    NewStart = readLast(file)
    start_date = NewStart+timedelta(days=1)
    end_date = datetime(year, month, day, hour, minute)
    if start_date == end_date:
        print("No update needed")
    else:
        file = open("SalmonCreek_Long.dat", 'a')
        for single_date in daterange(start_date, end_date):
            TminList=[]
            TmaxList=[]
            PrecipList=[]
            SnowList=[]
            ROList=[]
            # mesowest uses UTC, this adjusts for PST to UTC
            start_d = single_date #+ timedelta(hours=8)
            for station in TList:
                tmin = tminPull(station, start_d)
                TminList.append(tmin)
                tmax = tmaxPull(station, start_d)
                TmaxList.append(tmax)
            for station in PList:
                precip = precipPull (station, start_d)
                PrecipList.append(precip)
            for station in SWEList:
                precip = PRCPLookUp (station, start_d)
                PrecipList.append(precip)
            for station in SWEList:
                SWE = SWELookUp(station, single_date)
                SnowList.append(SWE)
            for station in RList:
                runoffval = runOffPullv2(station, single_date, end_date)
                ROList.append(runoffval)
            file.write(str(single_date.year)+" "+str(single_date.month)+" "+str(single_date.day)+" "
                       +str(0)+" "+str(0)+" "+str(0)+" "+" ".join(map(str,TmaxList))+" "+" ".join(map(str,TminList))+
                       " "+ " ".join(map(str,PrecipList))+" "+" ".join(map(str,SnowList))+" "+" ".join(map(str,ROList))+'\n')
        file.close()
else:
    ## if no .dat file is found this begins the process of creating one 
    
    file = open("SalmonCreek_Long.dat","w")
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
        
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day
    hour = datetime.now().hour
    minute = datetime.now().minute
    
#    start_date = datetime(year, month, day, hour, minute)
#    end_date = datetime(year, month, day, hour, minute)
   
    start_date = datetime(2000, 1, 1, 0, 0)

    end_date = datetime(2018, 3, 5, 0, 0)
    
    for single_date in daterange(start_date, end_date):
        TminList=[]
        TmaxList=[]
        PrecipList=[]
        SnowList=[]
        ROList=[]
        
        # mesowest uses UTC, this adjusts for PST to UTC
        start_d = single_date
        for station in TList:
            tmin = tminPull(station, start_d)
            TminList.append(tmin)
            tmax = tmaxPull(station, start_d)
            TmaxList.append(tmax)
        for station in PList:
            precip = precipPull (station, start_d)
            PrecipList.append(precip)
        for station in SWEList:
            precip = PRCPLookUp (station, start_d)
            PrecipList.append(precip)
        for station in SWEList:
            SWE = SWELookUp(station, single_date)
            SnowList.append(SWE)
        for station in RList:
            runoffval = runOffPullv2(station, single_date, end_date)
            ROList.append(runoffval)
            for (i,val) in enumerate(ROList):
                if val == None:
                    ROList[i] = -999.0
        file.write(str(single_date.year)+" "+str(single_date.month)+" "+str(single_date.day)+" "
                   +str(0)+" "+str(0)+" "+str(0)+" "+" ".join(map(str,TmaxList))+" "+" ".join(map(str,TminList))+
                   " "+ " ".join(map(str,PrecipList))+" "+" ".join(map(str,SnowList))+" "+" ".join(map(str,ROList))+'\n')
    file.close()
file.close()

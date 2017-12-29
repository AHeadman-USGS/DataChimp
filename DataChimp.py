import os
from MesoPy import Meso
from ulmo import usgs
from datetime import datetime, timedelta

# ulmo is throwing useless depreciation warnings like its going out of style.
# I got annoyed.  Ulmo has too much console flavor text to begin with.
# New version will phase out Ulmo in favor of my own urllib recipe.

import warnings
warnings.filterwarnings("ignore")

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
        return -999.0

def tmaxPull(station, start_date):
    try:
        end_d = start_date + timedelta(hours=32)
        stats = m.time_stats(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                             end=end_d.strftime("%Y%m%d%H%M"), type='all', units="temp|F")
        x=stats['STATION'][0]
        tmax = x['STATISTICS']['air_temp_set_1']['maximum']
        return tmax
    except:
        return -999.0

def precipPull(station, start_date):
    try:
        end_d = start_date + timedelta(hours=32)
        precip = m.precip(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                             end=end_d.strftime("%Y%m%d%H%M"), units='precip|in')
        y=precip['STATION'][0]
        PrecT = y['OBSERVATIONS']['total_precip_value_1']
        return PrecT
    except:
        return -999.0
    
def runoffPull(station, start_date, end_date):
    try:
        if start_date == end_date:
            return -999.0
        else:
            runoff = usgs.nwis.get_site_data(site_code=station,service='dv',
                                                  parameter_code='00060', start=start_date, end=start_date)
            x=runoff['00060:00003']['values'][0]
            runOffDV = float(x['value'])
            if runOffDV < 0:
                return -999.0
            else:
                return runOffDV
    except:
        return -999.0

# Current working lists for weather stations.

TList = ['ANEW1','SAMW1', 'KOMK', 'CDAW1', 'FBFW1','KMFW1', 'LEFW1']
PList = ['MUKW1']
RList = ['12446150', '12446400']
file = "SalmonCreekActual.dat"

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
        file = open("SalmonCreekActual.dat", 'a')
        for single_date in daterange(start_date, end_date):
            TminList=[]
            TmaxList=[]
            PrecipList=[]
            ROList=[]
            # mesowest uses UTC, this adjusts for PST to UTC
            start_d = single_date + timedelta(hours=8)
            end_d = single_date + timedelta(hours=32)
            for station in TList:
                tmin = tminPull(station, start_d)
                TminList.append(tmin)
                tmax = tmaxPull(station, start_d)
                TmaxList.append(tmax)
                precip = precipPull (station, start_d)
                PrecipList.append(precip)
            for station in PList:
                precip = precipPull (station, start_d)
                PrecipList.append(precip)
            for station in RList:
                runoffval = runoffPull(station, single_date, end_date)
                ROList.append(runoffval)
            file.write(str(single_date.year)+" "+str(single_date.month)+" "+str(single_date.day)+" "
                       +str(0)+" "+str(0)+" "+str(0)+" "+" ".join(map(str,TmaxList))+" "+" ".join(map(str,TminList))+
                       " "+ " ".join(map(str,PrecipList))+" "+" ".join(map(str,ROList))+'\n')
        file.close()
else:
    ## if no .dat file is found this begins the process of creating one 
    
    file = open("SalmonCreekActual.dat","w")
    file.write("// Station metadata:"+'\n')
    file.write("// ID Name Type Latitude Longitude Elevation"+'\n')
    
    #creating the metadata header
    for station in TList:
        a,b,c,d,e,f = metaPullMeso(station)
        file.write("// "+str(station)+" "+str(a)+" "+"TMAX "+str(b)+" "+str(c)+" "+str(d)+'\n')
    for station in TList:
        a,b,c,d,e,f = metaPullMeso(station)
        file.write("// "+str(station)+" "+str(a)+" "+"TMIN "+str(b)+" "+str(c)+" "+str(d)+'\n')
    for station in TList + PList:
        a,b,c,d,e,f = metaPullMeso(station)
        file.write("// "+str(station)+" "+str(a)+" "+"Precip "+str(b)+" "+str(c)+" "+str(d)+'\n')
    
    #creating the data object IDs
    
    file.write("tmax "+str(len(TList))+'\n')
    file.write("tmin "+str(len(TList))+'\n')
    file.write("precip "+str(len(TList)+len(PList))+'\n')
    file.write("runoff "+str(len(RList))+'\n') 
    file.write("########################################"+'\n')   
    
    ## datetime object provides previous X years/days/months of data
        
    year = datetime.now().year
    month = datetime.now().month
    day = datetime.now().day
    hour = datetime.now().hour
    minute = datetime.now().minute
    
    start_date = datetime(year-5, month, day, hour, minute)
    end_date = datetime(year, month, day, hour, minute)
    for single_date in daterange(start_date, end_date):
        TminList=[]
        TmaxList=[]
        PrecipList=[]
        ROList=[]
        # mesowest uses UTC, this adjusts for PST to UTC
        start_d = single_date + timedelta(hours=8)
        end_d = single_date + timedelta(hours=32)
        for station in TList:
            tmin = tminPull(station, start_d)
            TminList.append(tmin)
            tmax = tmaxPull(station, start_d)
            TmaxList.append(tmax)
            precip = precipPull (station, start_d)
            PrecipList.append(precip)
        for station in PList:
            precip = precipPull (station, start_d)
            PrecipList.append(precip)
        for station in RList:
            runoffval = runoffPull(station, single_date, end_date)
            ROList.append(runoffval)
        file.write(str(single_date.year)+" "+str(single_date.month)+" "+str(single_date.day)+" "
                   +str(0)+" "+str(0)+" "+str(0)+" "+" ".join(map(str,TmaxList))+" "+" ".join(map(str,TminList))+
                   " "+ " ".join(map(str,PrecipList))+" "+" ".join(map(str,ROList))+'\n')
    file.close()

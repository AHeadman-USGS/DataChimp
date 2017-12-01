from MesoPy import Meso
from ulmo import usgs
from datetime import datetime, timedelta

# ulmo is throwing depreciation warnings like its going out of style.
import warnings
warnings.filterwarnings("ignore")

# mesowest api Token, use this until it breaks.
m = Meso(token='81a655ec55cf450a87caa21486692770')

# functions
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def tminPull(station, start_date):
    end_d = start_date + timedelta(hours=32)
    stats = m.time_stats(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                         end=end_d.strftime("%Y%m%d%H%M"), type='all', units="temp|F")
    x=stats['STATION'][0]
    tmin = x['STATISTICS']['air_temp_set_1']['minimum']
    return tmin

def tmaxPull(station, start_date):
    end_d = start_date + timedelta(hours=32)
    stats = m.time_stats(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                         end=end_d.strftime("%Y%m%d%H%M"), type='all', units="temp|F")
    x=stats['STATION'][0]
    tmax = x['STATISTICS']['air_temp_set_1']['maximum']
    return tmax

def precipPull(station, start_date):
    end_d = start_date + timedelta(hours=32)
    precip = m.precip(stid=station, start=start_date.strftime("%Y%m%d%H%M"),
                         end=end_d.strftime("%Y%m%d%H%M"), units='precip|in')
    y=precip['STATION'][0]
    PrecT = y['OBSERVATIONS']['total_precip_value_1']
    return PrecT

def runoffPull(station, start_date, end_date):
    if start_date == end_date:
        return -999
    else:
        runoff = usgs.nwis.get_site_data(site_code=station,service='dv',
                                              parameter_code='00060', start=start_date, end=start_date)
        x=runoff['00060:00003']['values'][0]
        runOffDV = float(x['value'])
        return runOffDV

# Current working lists for weather stations.

TList = ['ANEW1','SAMW1', 'KOMK', 'CDAW1','NCSW1', 'FBFW1', 
         'KMFW1', 'LEFW1', 'DIFW1','D7538', 'MMSW1']
PList = ['MUKW1']
RList = ['12446150']

# datetime object provides previous two years of data
year = datetime.now().year
month = datetime.now().month
day = datetime.now().day
hour = datetime.now().hour
minute = datetime.now().minute

start_date = datetime(year, month, day-3, hour, minute)
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
    csvline = single_date.year, single_date.month, single_date.day, 0,0,0,*TminList, *TmaxList, *PrecipList, *ROList
    print(csvline)
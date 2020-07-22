import requests
import pandas as pd
import geopandas as gpd
import numpy as np
import datetime
import time
import os
from shapely import wkt



path='C:/Users/mayij/Desktop/DOC/DCP2019/GTFS-RT/Bus/'
url='http://bustime.mta.info/api/siri/vehicle-monitoring.json?key='+pd.read_csv(path+'KEY.csv',dtype=str).loc[0,'key']



for i in range(0,10):
    tp=pd.DataFrame(requests.get(url).json()).loc['ServiceDelivery','Siri']
    ts=datetime.datetime.strptime(tp['ResponseTimestamp'].split('.')[0],'%Y-%m-%dT%H:%M:%S').strftime('%m%d%H%M%S')
    tp=tp['VehicleMonitoringDelivery'][0]['VehicleActivity']
    veh=pd.DataFrame()
    veh['veh']=''
    veh['time']=''
    veh['line']=''
    veh['dir']=''
    veh['dest']=''
    veh['lat']=np.nan
    veh['long']=np.nan
    for j in range(0,len(tp)):
        veh.loc[j,'veh']=tp[j]['MonitoredVehicleJourney']['VehicleRef']
        veh.loc[j,'time']=datetime.datetime.strptime(tp[j]['RecordedAtTime'].split('.')[0],'%Y-%m-%dT%H:%M:%S').strftime('%m%d%H%M%S')
        veh.loc[j,'line']=tp[j]['MonitoredVehicleJourney']['PublishedLineName']
        veh.loc[j,'dir']=tp[j]['MonitoredVehicleJourney']['DirectionRef']
        veh.loc[j,'dest']=tp[j]['MonitoredVehicleJourney']['DestinationName']
        veh.loc[j,'lat']=tp[j]['MonitoredVehicleJourney']['VehicleLocation']['Latitude']
        veh.loc[j,'long']=tp[j]['MonitoredVehicleJourney']['VehicleLocation']['Longitude']
    veh.to_csv(path+'SIRI/'+ts+'.csv',index=False)
    time.sleep(60)



df=[]
vehs=os.listdir(path+'SIRI')
for i in vehs:
    df+=[pd.read_csv(path+'SIRI/'+i,dtype=str)]
df=pd.concat(df,axis=0,ignore_index=True)
df=df.sort_values(['veh','time'],ascending=True).reset_index(drop=True)
df['geom']=df['long']+' '+df['lat']
df=df.groupby(['veh','line','dir','dest'],as_index=False).agg({'geom': lambda x: ', '.join(x),'lat':'count'}).reset_index(drop=True)
df['geom']='LINESTRING ('+df['geom']+')'
df=df.loc[df['lat']!=1,['veh','line','dir','dest','geom']].reset_index(drop=True)
df=gpd.GeoDataFrame(df,geometry=df['geom'].map(wkt.loads),crs={'init': 'epsg:4326'})
df.to_file(path+'SIRI/SIRI.shp')


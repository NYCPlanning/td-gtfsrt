import requests
import pandas as pd
import geopandas as gpd
import numpy as np
import datetime
import time
import os
from shapely import wkt
import pytz



pd.set_option('display.max_columns', None)
path='C:/Users/mayij/Desktop/DOC/DCP2019/GTFS-RT/Bus/'
# path='/home/mayijun/GTFS-RT/Bus/'




dates=sorted(pd.unique([x.split('_')[1] for x in os.listdir(path+'SIRI/Raw/') if x.startswith('rttp')]))
for d in dates:
    rttp=[]
    for i in sorted([x for x in os.listdir(path+'SIRI/Raw/') if x.startswith('rttp_'+str(d))]):
        print(str(i))
        rttp.append(pd.read_csv(path+'SIRI/Raw/'+str(i),dtype=str))
    rttp=pd.concat(rttp,axis=0,ignore_index=True)
    rttp['time']=pd.to_numeric(rttp['time'])
    rttp['pax']=pd.to_numeric(rttp['pax'])
    rttp['cap']=pd.to_numeric(rttp['cap'])
    rttp=rttp.groupby(['veh','line','dir','dest','jrn','stpid','stpnm'],as_index=False).agg({'time':'max','dist':'max','pax':'max','cap':'max'})

    rttp=rttp.sort_values(['line','dir','dest','veh','dist']).reset_index(drop=True)

    rttp=rttp.groupby(['routeid','tripdate','tripid'],as_index=False).apply(calduration).reset_index(drop=True)


    # Combine
    tp=pd.merge(rttp,sctp,how='left',on=['routeid','tripdate','tripid','startstopid','endstopid'])
    tp=tp.dropna()
    tp['delay']=tp.duration-tp.schedule
    tp['delaypct']=tp.duration/tp.schedule
    tp.to_csv(path+'Output/API/tp_'+str(d)+'.csv',index=False,header=True,mode='w')





# Remove data except the last date
dates=sorted(pd.unique([x.split('_')[1] for x in os.listdir(path+'SIRI/Raw/') if x.startswith('rttp')]))[:-1]
for d in dates:
    for i in sorted([x for x in os.listdir(path+'SIRI/Raw/') if x.startswith('rttp_'+str(d))]):
        os.remove(path+'SIRI/Raw/'+str(i))
    for i in sorted([x for x in os.listdir(path+'SIRI/Raw/') if x.startswith('sctp_'+str(d))]):
        os.remove(path+'SIRI/Raw/'+str(i))









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


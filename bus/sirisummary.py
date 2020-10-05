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



# dt=rttp[0:4].reset_index(drop=True)
def calc(dt):
    dt=dt.reset_index(drop=True)
    dt['stpid1']=dt['stpid'].copy()
    dt['stpnm1']=dt['stpnm'].copy()
    dt['epoch1']=dt['epoch'].copy()
    dt['dist1']=dt['dist'].copy()
    dt['pax1']=dt['pax'].copy()
    dt['cap1']=dt['cap'].copy()
    dt['stpid2']=np.roll(dt['stpid'],-1)
    dt['stpnm2']=np.roll(dt['stpnm'],-1)
    dt['epoch2']=np.roll(dt['epoch'],-1)
    dt['dist2']=np.roll(dt['dist'],-1)
    dt['pax2']=np.roll(dt['pax'],-1)
    dt['cap2']=np.roll(dt['cap'],-1)
    dt['dur']=dt['epoch2']-dt['epoch1']
    dt['dist']=dt['dist2']-dt['dist1']
    dt['spd']=(dt['dist']/1609)/(dt['dur']/3600)
    dt['pax']=dt['pax2'].copy()
    dt['cap']=dt['cap2'].copy()
    dt=dt.iloc[:-1,:].reset_index(drop=True)
    dt=dt[['line','dir','dest','jrn','veh','stpid1','stpnm1','epoch1','dist1',
           'stpid2','stpnm2','epoch2','dist2','dur','dist','spd','pax','cap']].reset_index(drop=True)
    return dt



dates=sorted(pd.unique([x.split('_')[1] for x in os.listdir(path+'SIRI/Raw/') if x.startswith('rttp')]))
for d in dates:
    rttp=[]
    for i in sorted([x for x in os.listdir(path+'SIRI/Raw/') if x.startswith('rttp_'+str(d))]):
        print(str(i))
        rttp.append(pd.read_csv(path+'SIRI/Raw/'+str(i),dtype=str))
    rttp=pd.concat(rttp,axis=0,ignore_index=True)
    rttp['epoch']=pd.to_numeric(rttp['epoch'])
    rttp['dist']=pd.to_numeric(rttp['dist'])
    rttp['pax']=pd.to_numeric(rttp['pax'])
    rttp['cap']=pd.to_numeric(rttp['cap'])
    rttp=rttp.groupby(['line','dir','dest','jrn','veh','stpid','stpnm'],as_index=False).agg({'epoch':'max','dist':'max','pax':'mean','cap':'mean'})
    rttp=rttp.sort_values(['line','dir','dest','jrn','veh','dist']).reset_index(drop=True)
    rttp=rttp.groupby(['line','dir','dest','jrn','veh'],as_index=False).apply(calc).reset_index(drop=True)
    rttp.to_csv(path+'SIRI/Output/tp_'+str(d)+'.csv',index=False,header=True,mode='w')





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


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



# dt=rttp[0:5].reset_index(drop=True)
def calc(dt):
    dt=dt.reset_index(drop=True)
    dt['stpid1']=dt['stpid'].copy()
    dt['stpnm1']=dt['stpnm'].copy()
    dt['time1']=dt['time'].copy()
    dt['epoch1']=dt['epoch'].copy()
    dt['dist1']=dt['dist'].copy()
    dt['pax1']=dt['pax'].copy()
    dt['cap1']=dt['cap'].copy()
    dt['stpid2']=np.roll(dt['stpid'],-1)
    dt['stpnm2']=np.roll(dt['stpnm'],-1)
    dt['time2']=np.roll(dt['time'],-1)
    dt['epoch2']=np.roll(dt['epoch'],-1)
    dt['dist2']=np.roll(dt['dist'],-1)
    dt['pax2']=np.roll(dt['pax'],-1)
    dt['cap2']=np.roll(dt['cap'],-1)
    dt['dur']=dt['epoch2']-dt['epoch1']
    dt['dist']=dt['dist2']-dt['dist1']
    dt['mph']=(dt['dist']/1609)/(dt['dur']/3600)
    dt['pax']=dt['pax2'].copy()
    dt['cap']=dt['cap2'].copy()
    dt=dt.iloc[:-1,:].reset_index(drop=True)
    dt=dt[['line','dir','dest','jrn','veh','stpid1','stpnm1','time1','epoch1','dist1',
           'stpid2','stpnm2','time2','epoch2','dist2','dur','dist','mph','pax','cap']].reset_index(drop=True)
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
    rttp=rttp.sort_values(['line','dir','dest','jrn','veh','epoch'],ascending=True).reset_index(drop=True)
    rttp=rttp.drop_duplicates(['line','dir','dest','jrn','veh','stpid'],keep='last').reset_index(drop=True)
    rttp=rttp.groupby(['line','dir','dest','jrn','veh'],as_index=False).apply(calc).reset_index(drop=True)
    rttp.to_csv(path+'SIRI/Output/tp_'+str(d)+'.csv',index=False,header=True,mode='w')



# Remove data except the last date
dates=sorted(pd.unique([x.split('_')[1] for x in os.listdir(path+'SIRI/Raw/') if x.startswith('rttp')]))[:-1]
for d in dates:
    for i in sorted([x for x in os.listdir(path+'SIRI/Raw/') if x.startswith('rttp_'+str(d))]):
        os.remove(path+'SIRI/Raw/'+str(i))
    for i in sorted([x for x in os.listdir(path+'SIRI/Raw/') if x.startswith('sctp_'+str(d))]):
        os.remove(path+'SIRI/Raw/'+str(i))



# Summarize all the data and calculate AM peak metrics
tp=[]
for i in sorted([x for x in os.listdir(path+'SIRI/Output/') if x.startswith('tp')]):
    tp.append(pd.read_csv(path+'SIRI/Output/'+str(i),dtype=str))
tp=pd.concat(tp,axis=0,ignore_index=True)
tp['epoch1']=pd.to_numeric(tp['epoch1'])
tp['dist1']=pd.to_numeric(tp['dist1'])
tp['epoch2']=pd.to_numeric(tp['epoch2'])
tp['dist2']=pd.to_numeric(tp['dist2'])
tp['dur']=pd.to_numeric(tp['dur'])
tp['dist']=pd.to_numeric(tp['dist'])
tp['mph']=pd.to_numeric(tp['mph'])
tp['pax']=pd.to_numeric(tp['pax'])
tp['cap']=pd.to_numeric(tp['cap'])
tp['wkd1']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('US/Eastern')).weekday() for x in tp['epoch1']]
tp['hour1']=[datetime.datetime.fromtimestamp(x,tz=pytz.timezone('US/Eastern')).hour for x in tp['epoch1']]
tp=tp[tp['wkd1'].isin([0,1,2,3,4])]
tp=tp[tp['hour1'].isin([6,7,8,9])]



tp=tp[['routeid','startstopid','endstopid','waittime','duration','schedule','delay','delaypct']]
tp=tp.groupby(['routeid','startstopid','endstopid']).describe(percentiles=[0.1,0.25,0.5,0.75,0.9]).reset_index()
tp.columns=[(x[0]+x[1]).replace('%','') for x in tp.columns]
tp['waittimeqcv']=(tp['waittime75']-tp['waittime25'])/tp['waittime50']
tp['durationqcv']=(tp['duration75']-tp['duration25'])/tp['duration50']
tp['scheduleqcv']=(tp['schedule75']-tp['schedule25'])/tp['schedule50']
tp['delayqcv']=(tp['delay75']-tp['delay25'])/tp['delay50']
tp['delaypctqcv']=(tp['delaypct75']-tp['delaypct25'])/tp['delaypct50']
tp=tp[tp['durationcount']>100]
tp=pd.merge(tp,routes,how='left',left_on='routeid',right_on='route_id')
tp=pd.merge(tp,stops[['stop_id','stop_name','stop_lat','stop_lon']],how='left',left_on='startstopid',right_on='stop_id')
tp=pd.merge(tp,stops[['stop_id','stop_name','stop_lat','stop_lon']],how='left',left_on='endstopid',right_on='stop_id')
tp=tp[['routeid','route_color','startstopid','stop_name_x','stop_lat_x','stop_lon_x',
      'endstopid','stop_name_y','stop_lat_y','stop_lon_y',
      'waittimecount','waittimemin','waittimemax','waittimemean','waittimestd',
      'waittime10','waittime25','waittime50','waittime75','waittime90','waittimeqcv',
      'durationcount','durationmin','durationmax','durationmean','durationstd',
      'duration10','duration25','duration50','duration75','duration90','durationqcv',
      'schedulecount','schedulemin','schedulemax','schedulemean','schedulestd',
      'schedule10','schedule25','schedule50','schedule75','schedule90','scheduleqcv',
      'delaycount','delaymin','delaymax','delaymean','delaystd',
      'delay10','delay25','delay50','delay75','delay90','delayqcv',
      'delaypctcount','delaypctmin','delaypctmax','delaypctmean','delaypctstd',
      'delaypct10','delaypct25','delaypct50','delaypct75','delaypct90','delaypctqcv']]
tp.columns=['routeid','routecolor','startstopid','startstopname','startstoplat','startstoplong',
            'endstopid','endstopname','endstoplat','endstoplong',
            'waittimecount','waittimemin','waittimemax','waittimemean','waittimestd',
            'waittime10','waittime25','waittime50','waittime75','waittime90','waittimeqcv',
            'durationcount','durationmin','durationmax','durationmean','durationstd',
            'duration10','duration25','duration50','duration75','duration90','durationqcv',
            'schedulecount','schedulemin','schedulemax','schedulemean','schedulestd',
            'schedule10','schedule25','schedule50','schedule75','schedule90','scheduleqcv',
            'delaycount','delaymin','delaymax','delaymean','delaystd',
            'delay10','delay25','delay50','delay75','delay90','delayqcv',
            'delaypctcount','delaypctmin','delaypctmax','delaypctmean','delaypctstd',
            'delaypct10','delaypct25','delaypct50','delaypct75','delaypct90','delaypctqcv']
tp.to_csv(path+'Output/API/APIOutput.csv',index=False,header=True,mode='w')
#
#
#
## Create shapes and calculate speeds
#tp=pd.read_csv(path+'Output/API/APIOutput.csv',dtype=str)
#for i in tp.columns[10:]:
#    tp[i]=pd.to_numeric(tp[i])
#tp['startzip']=list(zip(round(pd.to_numeric(tp['startstoplong']),4),round(pd.to_numeric(tp['startstoplat']),4)))
#tp['endzip']=list(zip(round(pd.to_numeric(tp['endstoplong']),4),round(pd.to_numeric(tp['endstoplat']),4)))
#for i in tp.index:
#    if len(sh[sh['shape_id'].isin(pd.unique(trips[trips['route_id']==tp.loc[i,'routeid']]['shape_id']))])!=0:
#        start=sh[sh['shape_id'].isin(pd.unique(trips[trips['route_id']==tp.loc[i,'routeid']]['shape_id']))].reset_index(drop=True)
#    else:
#        start=sh[[x.split('.')[0]==tp.loc[i,'routeid'] for x in sh['shape_id']]].reset_index(drop=True)
#    start['zip']=list(zip(start['shape_pt_lon'],start['shape_pt_lat']))
#    if len(start)!=0:
#        start=start[start['zip']==tp.loc[i,'startzip']]
#    else:
#        start=pd.DataFrame(columns=['shape_id','shape_pt_lat','shape_pt_lon','shape_pt_sequence','zip'])
#    if len(sh[sh['shape_id'].isin(pd.unique(trips[trips['route_id']==tp.loc[i,'routeid']]['shape_id']))])!=0:
#        end=sh[sh['shape_id'].isin(pd.unique(trips[trips['route_id']==tp.loc[i,'routeid']]['shape_id']))].reset_index(drop=True)
#    else:
#        end=sh[[x.split('.')[0]==tp.loc[i,'routeid'] for x in sh['shape_id']]].reset_index(drop=True)
#    end['zip']=list(zip(end['shape_pt_lon'],end['shape_pt_lat']))
#    if len(end)!=0:
#        end=end[end['zip']==tp.loc[i,'endzip']]
#    else:
#        end=pd.DataFrame(columns=['shape_id','shape_pt_lat','shape_pt_lon','shape_pt_sequence','zip'])
#    startend=pd.merge(start,end,how='inner',on='shape_id')
#    startend=startend[startend['shape_pt_sequence_y']>startend['shape_pt_sequence_x']].reset_index(drop=True)
#    if len(startend)>0:
#        startend=startend.loc[0,:]
#        startindex=shapes[(shapes['shape_id']==startend['shape_id'])&(shapes['shape_pt_sequence']==startend['shape_pt_sequence_x'])].index[0]
#        endindex=shapes[(shapes['shape_id']==startend['shape_id'])&(shapes['shape_pt_sequence']==startend['shape_pt_sequence_y'])].index[0]
#        geom=shapes.loc[startindex:endindex,:].reset_index()
#        geom='LINESTRING('+', '.join(geom['shape_pt_lon']+' '+geom['shape_pt_lat'])+')'
#        tp.loc[i,'geom']=geom
#    else:
#        tp.loc[i,'geom']='LINESTRING(0 0, 0 0)'
#tp=gpd.GeoDataFrame(tp,crs={'init': 'epsg:4326'},geometry=tp['geom'].map(wkt.loads))
#tp=tp.to_crs({'init': 'epsg:6539'})
#tp['dist']=tp.geometry.length
#tp['mphmin']=(tp['dist']/5280)/(tp['durationmax']/3600)
#tp['mphmax']=(tp['dist']/5280)/(tp['durationmin']/3600)
#tp['mphmean']=(tp['dist']/5280)/(tp['durationmean']/3600)
#tp['mph10']=(tp['dist']/5280)/(tp['duration90']/3600)
#tp['mph25']=(tp['dist']/5280)/(tp['duration75']/3600)
#tp['mph50']=(tp['dist']/5280)/(tp['duration50']/3600)
#tp['mph75']=(tp['dist']/5280)/(tp['duration25']/3600)
#tp['mph90']=(tp['dist']/5280)/(tp['duration10']/3600)
#tp=tp.drop(['startzip','endzip','geom'],axis=1)
#tp.to_file(path+'Output/API/APIOutput.shp')
#tp.to_csv(path+'Output/API/APIOutput.csv',index=False,header=True,mode='w')
#
#
#
## Offset the shapes (todo: express/local trains)
#tp=gpd.read_file(path+'Output/API/APIOutput.shp')
#tp['geom']=''
#for i in tp.index:
#    try:
#        tp.loc[i,'geom']=tp.loc[i,'geometry'].parallel_offset(distance=200,side='right')
#    except:
#        tp.loc[i,'geom']=tp.loc[i,'geometry']
#tp=gpd.GeoDataFrame(tp,geometry=tp['geom'],crs={'init': 'epsg:6539'})
#tp=tp.drop('geom',axis=1)
#tp.to_file(path+'Output/API/APIOutput2.shp')
































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


import os
import pandas as pd
import time
import numpy as np
from shapely import wkt
import geopandas as gpd



pd.set_option('display.max_columns', None)
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
#path='C:/Users/Y_Ma2/Desktop/GTFS-RT/'
path='/home/mayijun/GTFS-RT/Subway/'
#path='E:GTFS-RT/'
#path='D:GTFS-RT/'
stops=pd.read_csv(path+'Schedule/stops.txt',dtype=str)
routes=pd.read_csv(path+'Schedule/routes.txt',dtype=str)
routes=routes[['route_id','route_color']]
routes.loc[routes['route_id'].isin(['FS','H']),'route_color']='6D6E71'
routes.loc[routes['route_id'].isin(['SI']),'route_color']='2850AD'
shapes=pd.read_csv(path+'Schedule/shapes.txt',dtype=str)
shapes['shape_pt_sequence']=pd.to_numeric(shapes['shape_pt_sequence'])
sh=shapes[['shape_id','shape_pt_lat','shape_pt_lon','shape_pt_sequence']].reset_index(drop=True)
sh['shape_pt_lat']=round(pd.to_numeric(sh['shape_pt_lat']),4)
sh['shape_pt_lon']=round(pd.to_numeric(sh['shape_pt_lon']),4)
trips=pd.read_csv(path+'Schedule/trips.txt',dtype=str)
trips['shape_id']=[x.split('_')[-1] for x in trips['trip_id']]



def calduration(dt):
    dt['startstopid']=dt['stopid']
    dt['starttime']=dt['time']
    dt['endstopid']=np.roll(dt['stopid'],-1)
    dt['endtime']=np.roll(dt['time'],-1)
    dt['duration']=dt['endtime']-dt['starttime']
    dt=dt.iloc[:-1,:]
    dt=dt.drop(['stopid','time'],axis=1)
    return dt



def calwaittime(wt):
    wt=wt.sort_values(['starttime']).reset_index(drop=True)
    wt['previoustime']=np.roll(wt['starttime'],1)
    wt['waittime']=wt['starttime']-wt['previoustime']
    wt=wt.iloc[1:,:]
    wt=wt.drop(['previoustime'],axis=1)
    return wt



# Summarize data by date
dates=sorted(pd.unique([x.split('_')[1] for x in os.listdir(path+'Raw/API/') if x.startswith('rttp')]))[4:]
for d in dates:
    # Realtime
    rttp=[]
    for i in sorted([x for x in os.listdir(path+'Raw/API/') if x.startswith('rttp_'+str(d))]):
        print(str(i))
        rttp.append(pd.read_csv(path+'Raw/API/'+str(i),dtype=str))
    rttp=pd.concat(rttp,axis=0,ignore_index=True)
    rttp['time']=pd.to_numeric(rttp['time'])
    rttp=rttp.groupby(['routeid','tripdate','tripid','stopid'],as_index=False).agg({'time':'max'})
    rttp=rttp.sort_values(['routeid','tripdate','tripid','time']).reset_index(drop=True)
    rttp=rttp.groupby(['routeid','tripdate','tripid'],as_index=False).apply(calduration).reset_index(drop=True)
    # Schedule
    sctp=[]
    for i in sorted([x for x in os.listdir(path+'Raw/API/') if x.startswith('sctp_'+str(d))]):
        print(str(i))
        sctp.append(pd.read_csv(path+'Raw/API/'+str(i),dtype=str))
    sctp=pd.concat(sctp,axis=0,ignore_index=True)
    sctp['duration']=pd.to_numeric(sctp['duration'])
    sctp=sctp.groupby(['routeid','tripdate','tripid','startstopid','endstopid'],as_index=False).agg({'duration':'median'})
    sctp.columns=['routeid','tripdate','tripid','startstopid','endstopid','schedule']
    # Combine
    tp=pd.merge(rttp,sctp,how='left',on=['routeid','tripdate','tripid','startstopid','endstopid'])
    tp=tp.dropna()
    tp['delay']=tp.duration-tp.schedule
    tp['delaypct']=tp.duration/tp.schedule
    tp.to_csv(path+'Output/API/tp_'+str(d)+'.csv',index=False,header=True,mode='w')



# Remove data except the last date
dates=sorted(pd.unique([x.split('_')[1] for x in os.listdir(path+'Raw/API/') if x.startswith('rttp')]))[:-1]
for d in dates:
    for i in sorted([x for x in os.listdir(path+'Raw/API/') if x.startswith('rttp_'+str(d))]):
        os.remove(path+'Raw/API/'+str(i))
    for i in sorted([x for x in os.listdir(path+'Raw/API/') if x.startswith('sctp_'+str(d))]):
        os.remove(path+'Raw/API/'+str(i))



## Summarize all the data and calculate AM peak metrics
#tp=[]
#for i in sorted([x for x in os.listdir(path+'Output/API/') if x.startswith('tp')]):
#    tp.append(pd.read_csv(path+'Output/API/'+str(i),dtype=str))
#tp=pd.concat(tp,axis=0,ignore_index=True)
#tp['starttime']=pd.to_numeric(tp['starttime'])
#tp['endtime']=pd.to_numeric(tp['endtime'])
#tp['duration']=pd.to_numeric(tp['duration'])
#tp['schedule']=pd.to_numeric(tp['schedule'])
#tp['delay']=pd.to_numeric(tp['delay'])
#tp['delaypct']=pd.to_numeric(tp['delaypct'])
#tp=tp.groupby(['routeid','startstopid','endstopid'],as_index=False).apply(calwaittime).reset_index(drop=True)
#tp['starthour']=[time.localtime(x).tm_hour for x in tp['starttime']]
#tp['startweekday']=[time.localtime(x).tm_wday for x in tp['starttime']]
#tp=tp[tp['startweekday'].isin([0,1,2,3,4])]
#tp=tp[tp['starthour'].isin([6,7,8,9])]
#tp=tp[['routeid','startstopid','endstopid','waittime','duration','schedule','delay','delaypct']]
#tp=tp.groupby(['routeid','startstopid','endstopid']).describe(percentiles=[0.1,0.25,0.5,0.75,0.9]).reset_index()
#tp.columns=[(x[0]+x[1]).replace('%','') for x in tp.columns]
#tp['waittimeqcv']=(tp['waittime75']-tp['waittime25'])/tp['waittime50']
#tp['durationqcv']=(tp['duration75']-tp['duration25'])/tp['duration50']
#tp['scheduleqcv']=(tp['schedule75']-tp['schedule25'])/tp['schedule50']
#tp['delayqcv']=(tp['delay75']-tp['delay25'])/tp['delay50']
#tp['delaypctqcv']=(tp['delaypct75']-tp['delaypct25'])/tp['delaypct50']
#tp=tp[tp['durationcount']>100]
#tp=pd.merge(tp,routes,how='left',left_on='routeid',right_on='route_id')
#tp=pd.merge(tp,stops[['stop_id','stop_name','stop_lat','stop_lon']],how='left',left_on='startstopid',right_on='stop_id')
#tp=pd.merge(tp,stops[['stop_id','stop_name','stop_lat','stop_lon']],how='left',left_on='endstopid',right_on='stop_id')
#tp=tp[['routeid','route_color','startstopid','stop_name_x','stop_lat_x','stop_lon_x',
#       'endstopid','stop_name_y','stop_lat_y','stop_lon_y',
#       'waittimecount','waittimemin','waittimemax','waittimemean','waittimestd',
#       'waittime10','waittime25','waittime50','waittime75','waittime90','waittimeqcv',
#       'durationcount','durationmin','durationmax','durationmean','durationstd',
#       'duration10','duration25','duration50','duration75','duration90','durationqcv',
#       'schedulecount','schedulemin','schedulemax','schedulemean','schedulestd',
#       'schedule10','schedule25','schedule50','schedule75','schedule90','scheduleqcv',
#       'delaycount','delaymin','delaymax','delaymean','delaystd',
#       'delay10','delay25','delay50','delay75','delay90','delayqcv',
#       'delaypctcount','delaypctmin','delaypctmax','delaypctmean','delaypctstd',
#       'delaypct10','delaypct25','delaypct50','delaypct75','delaypct90','delaypctqcv']]
#tp.columns=['routeid','routecolor','startstopid','startstopname','startstoplat','startstoplong',
#            'endstopid','endstopname','endstoplat','endstoplong',
#            'waittimecount','waittimemin','waittimemax','waittimemean','waittimestd',
#            'waittime10','waittime25','waittime50','waittime75','waittime90','waittimeqcv',
#            'durationcount','durationmin','durationmax','durationmean','durationstd',
#            'duration10','duration25','duration50','duration75','duration90','durationqcv',
#            'schedulecount','schedulemin','schedulemax','schedulemean','schedulestd',
#            'schedule10','schedule25','schedule50','schedule75','schedule90','scheduleqcv',
#            'delaycount','delaymin','delaymax','delaymean','delaystd',
#            'delay10','delay25','delay50','delay75','delay90','delayqcv',
#            'delaypctcount','delaypctmin','delaypctmax','delaypctmean','delaypctstd',
#            'delaypct10','delaypct25','delaypct50','delaypct75','delaypct90','delaypctqcv']
#tp.to_csv(path+'Output/API/APIOutput.csv',index=False,header=True,mode='w')
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



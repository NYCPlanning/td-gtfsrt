import os
import pandas as pd
import time
import numpy as np
import shapely
import geopandas as gpd



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
#path='C:/Users/Y_Ma2/Desktop/GTFS-RT/'
#path='/home/mayijun/GTFS-RT/'
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
    dt['starttime']=[time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(x)) for x in dt['starttime']]
    dt['endtime']=[time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(x)) for x in dt['endtime']]
    dt['starthour']=[x[11:13] for x in dt['starttime']]
    dt=dt[['routeid','tripid','starthour','startstopid','starttime','endstopid','endtime','duration']]
    dt=dt.iloc[:-1,:]
    return dt



def calwaittime(wt):
    wt=wt.sort_values(['starttime']).reset_index(drop=True)
    wt['previoustime']=np.roll(wt['starttime'],1)
    wt['starttime']=[time.mktime(time.strptime(x,'%Y-%m-%d %H:%M:%S')) for x in wt['starttime']]
    wt['previoustime']=[time.mktime(time.strptime(x,'%Y-%m-%d %H:%M:%S')) for x in wt['previoustime']]
    wt['waittime']=wt['starttime']-wt['previoustime']
    wt=wt.iloc[1:,:]
    return wt



dates=sorted(pd.unique([x.split('_')[1] for x in os.listdir(path+'Output/Archive/') if x.startswith('rttp')]))
for d in dates:
    # Realtime
    rttp=[]
    for i in sorted([x for x in os.listdir(path+'Output/Archive/') if x.startswith('rttp_'+str(d))]):
        rttp.append(pd.read_csv(path+'Output/Archive/'+str(i),dtype=str))
    rttp=pd.concat(rttp,axis=0,ignore_index=True)
    rttp['time']=pd.to_numeric(rttp['time'])
    rttp=rttp.groupby(['routeid','tripid','stopid'],as_index=False).agg({'time':'median'})
    rttp=rttp.sort_values(['routeid','tripid','time']).reset_index(drop=True)
    rttp=rttp.groupby(['routeid','tripid'],as_index=False).apply(calduration).reset_index(drop=True)
    # Schedule
    sctp=[]
    for i in sorted([x for x in os.listdir(path+'Output/Archive/') if x.startswith('sctp_'+str(d))]):
        sctp.append(pd.read_csv(path+'Output/Archive/'+str(i),dtype=str))
    sctp=pd.concat(sctp,axis=0,ignore_index=True)
    sctp['duration']=pd.to_numeric(sctp['duration'])
    sctp=sctp.groupby(['routeid','tripid','startstopid','endstopid'],as_index=False).agg({'duration':'median'})
    sctp.columns=['routeid','tripid','startstopid','endstopid','schedule']
    # Combine
    tp=pd.merge(rttp,sctp,how='left',on=['routeid','tripid','startstopid','endstopid'])
    tp=tp.dropna()
    tp['delay']=tp.duration-tp.schedule
    tp['delaypct']=tp.duration/tp.schedule
    tp.to_csv(path+'Output/Archive/tp_'+str(d)+'.csv',index=False,header=True,mode='w')



tp=[]
for i in sorted([x for x in os.listdir(path+'Output/Archive/') if x.startswith('tp')]):
    tp.append(pd.read_csv(path+'Output/Archive/'+str(i),dtype=str))
tp=pd.concat(tp,axis=0,ignore_index=True)
tp['duration']=pd.to_numeric(tp['duration'])
tp['schedule']=pd.to_numeric(tp['schedule'])
tp['delay']=pd.to_numeric(tp['delay'])
tp['delaypct']=pd.to_numeric(tp['delaypct'])
tp['startweekday']=[time.strptime(x,'%Y-%m-%d %H:%M:%S').tm_wday for x in tp['starttime']]
tp=tp.groupby(['routeid','startstopid','endstopid'],as_index=False).apply(calwaittime).reset_index(drop=True)
tp=tp[tp['startweekday'].isin([0,1,2,3,4])]
tp=tp[tp['starthour'].isin(['06','07','08','09'])]
tp=tp[['routeid','startstopid','endstopid','waittime','duration','schedule','delay','delaypct']]
tp=tp.groupby(['routeid','startstopid','endstopid']).describe(percentiles=[0.1,0.25,0.5,0.75,0.9]).reset_index()
tp.columns=[(x[0]+x[1]).replace('%','') for x in tp.columns]
tp['waittimeqcv']=(tp['waittime75']-tp['waittime25'])/tp['waittime50']
tp['durationqcv']=(tp['duration75']-tp['duration25'])/tp['duration50']
tp['scheduleqcv']=(tp['schedule75']-tp['schedule25'])/tp['schedule50']
tp['delayqcv']=(tp['delay75']-tp['delay25'])/tp['delay50']
tp['delaypctqcv']=(tp['delaypct75']-tp['delaypct25'])/tp['delaypct50']
tp=tp[tp['durationcount']>10]
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
tp['startzip']=list(zip(round(pd.to_numeric(tp['startstoplong']),4),round(pd.to_numeric(tp['startstoplat']),4)))
tp['endzip']=list(zip(round(pd.to_numeric(tp['endstoplong']),4),round(pd.to_numeric(tp['endstoplat']),4)))
for i in tp.index:
    if len(sh[sh['shape_id'].isin(pd.unique(trips[trips['route_id']==tp.loc[i,'routeid']]['shape_id']))])!=0:
        start=sh[sh['shape_id'].isin(pd.unique(trips[trips['route_id']==tp.loc[i,'routeid']]['shape_id']))].reset_index(drop=True)
    else:
        start=sh[[x.split('..')[0]==tp.loc[i,'routeid'] for x in sh['shape_id']]].reset_index(drop=True)
    start['zip']=list(zip(start['shape_pt_lon'],start['shape_pt_lat']))
    start=start[start['zip']==tp.loc[i,'startzip']]
    if len(sh[sh['shape_id'].isin(pd.unique(trips[trips['route_id']==tp.loc[i,'routeid']]['shape_id']))])!=0:
        end=sh[sh['shape_id'].isin(pd.unique(trips[trips['route_id']==tp.loc[i,'routeid']]['shape_id']))].reset_index(drop=True)
    else:
        end=sh[[x.split('..')[0]==tp.loc[i,'routeid'] for x in sh['shape_id']]].reset_index(drop=True)
    end['zip']=list(zip(end['shape_pt_lon'],end['shape_pt_lat']))
    end=end[end['zip']==tp.loc[i,'endzip']]    
    startend=pd.merge(start,end,how='inner',on='shape_id')
    startend=startend[startend['shape_pt_sequence_y']>startend['shape_pt_sequence_x']].reset_index(drop=True)
    if len(startend)>0:
        startend=startend.loc[0,:]
        startindex=shapes[(shapes['shape_id']==startend['shape_id'])&(shapes['shape_pt_sequence']==startend['shape_pt_sequence_x'])].index[0]
        endindex=shapes[(shapes['shape_id']==startend['shape_id'])&(shapes['shape_pt_sequence']==startend['shape_pt_sequence_y'])].index[0]
        geom=shapes.loc[startindex:endindex,:].reset_index()
        geom='LINESTRING('+', '.join(geom['shape_pt_lon']+' '+geom['shape_pt_lat'])+')'
        tp.loc[i,'geom']=geom
    else:
        tp.loc[i,'geom']=''
tp=gpd.GeoDataFrame(tp,crs={'init': 'epsg:4326'},geometry=tp['geom'].map(shapely.wkt.loads))
tp=tp.to_crs({'init': 'epsg:6539'})
tp['dist']=tp.geometry.length
tp.drop(['startzip','endzip'],axis=1)
tp.to_file(path+'Output/Archive/ArchiveOutput.shp')
tp.to_csv(path+'Output/Archive/ArchiveOutput.csv',index=False,header=True,mode='w')


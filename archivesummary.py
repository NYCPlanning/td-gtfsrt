import os
import pandas as pd
import datetime
import time
import numpy as np



start=datetime.datetime.now()
pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
#path='C:/Users/Y_Ma2/Desktop/GTFS-RT/'
#path='/home/mayijun/GTFS-RT/'
stops=pd.read_csv(path+'Schedule/stops.txt',dtype=str)
routes=pd.read_csv(path+'Schedule/routes.txt',dtype=str)
routes=routes[['route_id','route_color']]
routes.loc[routes['route_id'].isin(['FS','H']),'route_color']='6D6E71'
routes.loc[routes['route_id'].isin(['SI']),'route_color']='2850AD'



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
tp['geom']='LINESTRING('+tp['startstoplong']+' '+tp['startstoplat']+', '+tp['endstoplong']+' '+tp['endstoplat']+')'
tp.to_csv(path+'Output/Archive/ArchiveOutput.csv',index=False,header=True,mode='w')





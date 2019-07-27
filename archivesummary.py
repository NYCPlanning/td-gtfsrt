import os
import pandas as pd
import datetime
import time
import numpy as np


start=datetime.datetime.now()
pd.set_option('display.max_columns', None)
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
#path='C:/Users/Y_Ma2/Desktop/GTFS-RT/'
path='/home/mayijun/GTFS-RT/'
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
    wt['previoustime']=np.roll(wt['starttime'],1)
    wt['starttime']=[time.mktime(time.strptime(x,'%Y-%m-%d %H:%M:%S')) for x in wt['starttime']]
    wt['previoustime']=[time.mktime(time.strptime(x,'%Y-%m-%d %H:%M:%S')) for x in wt['previoustime']]
    wt['waittime']=wt['starttime']-wt['previoustime']
    wt=wt.iloc[1:,:]
    return wt



dates=sorted(pd.unique([x.split('_')[1] for x in os.listdir(path+'Output/Archive/') if x.startswith('rttp')]))
for d in dates:
    rttp=[]
    for i in sorted([x for x in os.listdir(path+'Output/Archive/') if x.startswith('rttp_'+str(d))]):
        rttp.append(pd.read_csv(path+'Output/Archive/'+str(i),dtype=str))
    rttp=pd.concat(rttp,axis=0,ignore_index=True)
    rttp['time']=pd.to_numeric(rttp['time'])
    rttp=rttp.groupby(['routeid','tripid','stopid'],as_index=False).agg({'time':'median'})
    rttp=rttp.sort_values(['routeid','tripid','time']).reset_index(drop=True)
    rttp=rttp.groupby(['routeid','tripid'],as_index=False).apply(calduration).reset_index(drop=True)
    sctp=[]
    for i in sorted([x for x in os.listdir(path+'Output/Archive/') if x.startswith('sctp_'+str(d))]):
        sctp.append(pd.read_csv(path+'Output/Archive/'+str(i),dtype=str))
    sctp=pd.concat(sctp,axis=0,ignore_index=True)
    sctp['duration']=pd.to_numeric(sctp['duration'])
    sctp=sctp.groupby(['routeid','tripid','startstopid','endstopid'],as_index=False).agg({'duration':'median'})
    sctp.columns=['routeid','tripid','startstopid','endstopid','schedule']
    tp=pd.merge(rttp,sctp,how='left',on=['routeid','tripid','startstopid','endstopid'])
    tp=tp.dropna()
    tp['delay']=tp.duration-tp.schedule
    tp['delaypct']=tp.duration/tp.schedule
    tp.to_csv(path+'Output/Archive/tp_'+str(d)+'.csv',index=False,header=True,mode='w')



#tp=[]
#for i in sorted([x for x in os.listdir(path+'Output/Archive/') if x.startswith('tp')]):
#    tp.append(pd.read_csv(path+'Output/Archive/'+str(i),dtype=str))
#tp=pd.concat(tp,axis=0,ignore_index=True)
#tp['duration']=pd.to_numeric(tp['duration'])
#tp['schedule']=pd.to_numeric(tp['schedule'])
#tp['delay']=pd.to_numeric(tp['delay'])
#tp['delaypct']=pd.to_numeric(tp['delaypct'])
#tp['startweekday']=[time.strptime(x,'%Y-%m-%d %H:%M:%S').tm_wday for x in tp['starttime']]
#
#
#
#
#tp=tp[tp['startweekday'].isin([0,1,2,3,4])]
#tp=tp[tp['starthour'].isin(['06','07','08','09'])]
#tp=tp[['routeid','startstopid','endstopid','duration','schedule','delay','delaypct']]
#
#
#
#
#
#tp=tp.groupby(['routeid','startstopid','endstopid']).describe(percentiles=[0.1,0.5,0.9]).reset_index(drop=True)
#tp.columns=[(x[0]+x[1]).replace('%','') for x in tp.columns]
#tp=pd.merge(tp,routes,how='left',left_on='routeid',right_on='route_id')
#tp=pd.merge(tp,stops[['stop_id','stop_name','stop_lat','stop_lon']],how='left',left_on='startstopid',right_on='stop_id')
#tp=pd.merge(tp,stops[['stop_id','stop_name','stop_lat','stop_lon']],how='left',left_on='endstopid',right_on='stop_id')
#tp=tp[['routeid','route_color','startstopid','stop_name_x','stop_lat_x','stop_lon_x',
#       'endstopid','stop_name_y','stop_lat_y','stop_lon_y',
#       'durationcount','durationmin','durationmax','durationmean','durationstd','duration10','duration50','duration90',
#       'schedulecount','schedulemin','schedulemax','schedulemean','schedulestd','schedule10','schedule50','schedule90',
#       'delaycount','delaymin','delaymax','delaymean','delaystd','delay10','delay50','delay90',
#       'delaypctcount','delaypctmin','delaypctmax','delaypctmean','delaypctstd','delaypct10','delaypct50','delaypct90']]
#tp.columns=['routeid','routecolor','startstopid','startstopname','startstoplat','startstoplong',
#            'endstopid','endstopname','endstoplat','endstoplong',
#            'durationcount','durationmin','durationmax','durationmean','durationstd','duration10','duration50','duration90',
#            'schedulecount','schedulemin','schedulemax','schedulemean','schedulestd','schedule10','schedule50','schedule90',
#            'delaycount','delaymin','delaymax','delaymean','delaystd','delay10','delay50','delay90',
#            'delaypctcount','delaypctmin','delaypctmax','delaypctmean','delaypctstd','delaypct10','delaypct50','delaypct90']
#tp['geom']='LINESTRING('+tp['startstoplong']+' '+tp['startstoplat']+', '+tp['endstoplong']+' '+tp['endstoplat']+')'
#tp.to_csv(path+'Output/Archive/ArchiveOutput.csv',index=False,header=True,mode='w')


















#tp=tp.groupby(['routeid','startstopid','endstopid'],as_index=False).agg({'duration':['min','median','mean','max','count'],
#             'schedule':['min','median','mean','max','count'],'delay':['min','median','mean','max','count'],
#             'delaypct':['min','median','mean','max','count']})
#tp.columns=[x[0]+x[1] for x in tp.columns]
#tp=pd.merge(tp,routes,how='left',left_on='routeid',right_on='route_id')
#tp=pd.merge(tp,stops[['stop_id','stop_name','stop_lat','stop_lon']],how='left',left_on='startstopid',right_on='stop_id')
#tp=pd.merge(tp,stops[['stop_id','stop_name','stop_lat','stop_lon']],how='left',left_on='endstopid',right_on='stop_id')
#tp=tp[['routeid','route_color','startstopid','stop_name_x','stop_lat_x','stop_lon_x',
#       'endstopid','stop_name_y','stop_lat_y','stop_lon_y',
#       'durationmin','durationmedian','durationmean','durationmax','durationcount',
#       'schedulemin','schedulemedian','schedulemean','schedulemax','schedulecount',
#       'delaymin','delaymedian','delaymean','delaymax','delaycount',
#       'delaypctmin','delaypctmedian','delaypctmean','delaypctmax','delaypctcount']]
#tp.columns=['routeid','routecolorhex','startstopid','startstopname','startstoplat','startstoplong',
#            'endstopid','endstopname','endstoplat','endstoplong',
#            'durationmin','durationmedian','durationmean','durationmax','durationcount',
#            'schedulemin','schedulemedian','schedulemean','schedulemax','schedulecount',
#            'delaymin','delaymedian','delaymean','delaymax','delaycount',
#            'delaypctmin','delaypctmedian','delaypctmean','delaypctmax','delaypctcount']
#tp=tp[tp['durationcount']>10]
#tp['geom']='LINESTRING('+tp['startstoplong']+' '+tp['startstoplat']+', '+tp['endstoplong']+' '+tp['endstoplat']+')'
#tp.to_csv(path+'Output/Archive/ArchiveOutput.csv',index=False,header=True,mode='w')



#tp=pd.merge(tp,stops[['stop_id','stop_name']],how='left',left_on='startstopid',right_on='stop_id')
#tp=pd.merge(tp,stops[['stop_id','stop_name']],how='left',left_on='endstopid',right_on='stop_id')
#tp=tp[['routeid','tripid','starthour','startstopid','stop_name_x','starttime',
#       'endstopid','stop_name_y','endtime','duration','schedule','delay','delaypct']]
#tp.columns=['routeid','tripid','starthour','startstopid','startstopname','starttime',
#            'endstopid','endstopname','endtime','duration','schedule','delay','delaypct']
#tp=tp[tp.starthour.isin(['06','07','08','09'])]
#tp=tp.groupby(['routeid','startstopid','endstopid'],as_index=False).agg({'duration':['min','median','mean','max','count'],
#             'schedule':['min','median','mean','max','count'],'delaypct':['min','median','mean','max','count']})
#tp.columns=[x[0]+x[1] for x in tp.columns]
#tp=tp.groupby(['starthour'],as_index=False).agg({'delaypct':['min','median','mean','max','count']})
#tp.columns=[x[0]+x[1] for x in tp.columns]





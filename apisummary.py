import os
import pandas as pd
import time
import numpy as np



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
#path='C:/Users/Y_Ma2/Desktop/GTFS-RT/'
#path='/home/mayijun/GTFS-RT/'
stops=pd.read_csv(path+'Schedule/stops.txt')
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



# Realtime
rttp=[]
for i in [x for x in os.listdir(path+'Output/API/') if x.startswith('rttp')]:
    rttp.append(pd.read_csv(path+'Output/API/'+str(i),dtype=str))
rttp=pd.concat(rttp,axis=0,ignore_index=True)
rttp['time']=pd.to_numeric(rttp['time'])
rttp=rttp.groupby(['routeid','tripid','stopid'],as_index=False).agg({'time':'median'})
rttp=rttp.sort_values(['routeid','tripid','time']).reset_index(drop=True)
rttp=rttp.groupby(['routeid','tripid'],as_index=False).apply(calduration).reset_index(drop=True)
# Schedule
sctp=[]
for i in [x for x in os.listdir(path+'Output/API/') if x.startswith('sctp')]:
    sctp.append(pd.read_csv(path+'Output/API/'+str(i),dtype=str))
sctp=pd.concat(sctp,axis=0,ignore_index=True)
sctp['duration']=pd.to_numeric(sctp['duration'])
sctp=sctp.groupby(['routeid','tripid','startstopid','endstopid'],as_index=False).agg({'duration':'median'})
sctp.columns=['routeid','tripid','startstopid','endstopid','schedule']
# Combine
tp=pd.merge(rttp,sctp,how='left',on=['routeid','tripid','startstopid','endstopid'])
tp=tp.dropna()
tp['delay']=tp.duration-tp.schedule
tp['delaypct']=tp.duration/tp.schedule
tp=pd.merge(tp,stops[['stop_id','stop_name']],how='left',left_on='startstopid',right_on='stop_id')
tp=pd.merge(tp,stops[['stop_id','stop_name']],how='left',left_on='endstopid',right_on='stop_id')
tp=tp[['routeid','tripid','starthour','startstopid','stop_name_x','starttime',
       'endstopid','stop_name_y','endtime','duration','schedule','delay','delaypct']]
tp.columns=['routeid','tripid','starthour','startstopid','startstopname','starttime',
            'endstopid','endstopname','endtime','duration','schedule','delay','delaypct']



tp=tp[tp.starthour.isin(['06','07','08','09'])]
tp=tp.groupby(['routeid','startstopid','endstopid'],as_index=False).agg({'duration':['min','median','mean','max','count'],
             'schedule':['min','median','mean','max','count'],'delaypct':['min','median','mean','max','count']})
tp.columns=[x[0]+x[1] for x in tp.columns]
tp=tp.groupby(['starthour'],as_index=False).agg({'delaypct':['min','median','mean','max','count']})
tp.columns=[x[0]+x[1] for x in tp.columns]




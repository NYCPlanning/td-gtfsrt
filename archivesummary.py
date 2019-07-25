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



def calduration(df):
    df['startstopid']=df['stopid']
    df['starttime']=df['time']
    df['endstopid']=np.roll(df['stopid'],-1)
    df['endtime']=np.roll(df['time'],-1)
    df['duration']=df['endtime']-df['starttime']
    df['starttime']=[time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(x)) for x in df['starttime']]
    df['endtime']=[time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(x)) for x in df['endtime']]
    df['starthour']=[x[11:13] for x in df['starttime']]
    df=df[['routeid','tripid','starthour','startstopid','starttime','endstopid','endtime','duration']]
    df=df.iloc[:-1,:]
    return df




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



tp=[]
for i in sorted([x for x in os.listdir(path+'Output/Archive/') if x.startswith('tp')]):
    tp.append(pd.read_csv(path+'Output/Archive/'+str(i),dtype=str))
tp=pd.concat(tp,axis=0,ignore_index=True)
tp['duration']=pd.to_numeric(tp['duration'])
tp['schedule']=pd.to_numeric(tp['schedule'])
tp['delay']=pd.to_numeric(tp['delay'])
tp['delaypct']=pd.to_numeric(tp['delaypct'])
tp=tp.groupby(['routeid','startstopid','endstopid'],as_index=False).agg({'duration':['min','median','mean','max','count'],
             'schedule':['min','median','mean','max','count'],'delay':['min','median','mean','max','count'],
             'delaypct':['min','median','mean','max','count']})
tp.columns=[x[0]+x[1] for x in tp.columns]
tp=pd.merge(tp,stops[['stop_id','stop_name','stop_lat','stop_lon']],how='left',left_on='startstopid',right_on='stop_id')
tp=pd.merge(tp,stops[['stop_id','stop_name','stop_lat','stop_lon']],how='left',left_on='endstopid',right_on='stop_id')
tp=tp[['routeid','startstopid','stop_name_x','stop_lat_x','stop_lon_x',
       'endstopid','stop_name_y','stop_lat_y','stop_lon_y',
       'durationmin','durationmedian','durationmean','durationmax','durationcount',
       'schedulemin','schedulemedian','schedulemean','schedulemax','schedulecount',
       'delaymin','delaymedian','delaymean','delaymax','delaycount',
       'delaypctmin','delaypctmedian','delaypctmean','delaypctmax','delaypctcount']]
tp.columns=['routeid','startstopid','startstopname','startstoplat','startstoplong',
            'endstopid','endstopname','endstoplat','endstoplong',
            'durationmin','durationmedian','durationmean','durationmax','durationcount',
            'schedulemin','schedulemedian','schedulemean','schedulemax','schedulecount',
            'delaymin','delaymedian','delaymean','delaymax','delaycount',
            'delaypctmin','delaypctmedian','delaypctmean','delaypctmax','delaypctcount']
tp=tp[tp['durationcount']>10]
tp['geom']='LINESTRING('+tp['startstoplong']+' '+tp['startstoplat']+', '+tp['endstoplong']+' '+tp['endstoplat']+')'
tp.to_csv(path+'Output/Archive/tp.csv',index=False,header=True,mode='w')

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





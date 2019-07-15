import os
from google.transit import gtfs_realtime_pb2
import urllib
import pandas as pd
import datetime
import numpy as np


pd.set_option('display.max_columns', None)


path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
stops=pd.read_csv(path+'Schedule/stops.txt')


def calduration(df):
    df['startstopid']=df['stop_id']
    df['startstopname']=df['stop_name']
    df['starttime']=df['time']
    df['endstopid']=np.roll(df['stop_id'],-1)
    df['endstopname']=np.roll(df['stop_name'],-1)
    df['endtime']=np.roll(df['time'],-1)
    df['tripduration']=df['endtime']-df['starttime']
    df['starttime']=[datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S") for x in df['starttime']]
    df['endtime']=[datetime.datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S") for x in df['endtime']]
    df['starthour']=[x[11:13] for x in df['starttime']]
    df=df[['startstopid','startstopname','starttime','endstopid','endstopname','endtime','tripduration','starthour']]
    df=df.iloc[:-1,:]
    return df




feed = gtfs_realtime_pb2.FeedMessage()

realtime=pd.DataFrame()
schedule=pd.DataFrame()
f=[x for x in os.listdir(path+'20190501/') if x.startswith('gtfs_7_20190501')][0:1000]
for i in f:
    try:
        response=urllib.request.urlopen('file:///'+path+'20190501/'+i)
        feed.ParseFromString(response.read())
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                try:
                    # realtime
                    tp=pd.DataFrame(columns=['route_id','trip_id','stop_id','time'])
                    rt=list([entity.trip_update.trip.route_id])
                    tr=list([entity.trip_update.trip.trip_id])
                    st=list([entity.trip_update.stop_time_update[0].stop_id])
                    ti=list([entity.trip_update.stop_time_update[0].arrival.time])
                    tp['route_id']=rt
                    tp['trip_id']=tr
                    tp['stop_id']=st
                    tp['time']=ti
                    tp=pd.merge(tp,stops[['stop_id','stop_name']],how='left',on='stop_id')
                    tp=tp.dropna()
                    realtime=realtime.append(tp,ignore_index=True)
#                    # schedule
#                    tp=pd.DataFrame(columns=['stop_id','time'])
#                    s=[]
#                    t=[]
#                    for j in entity.trip_update.stop_time_update[1:]:
#                         s=s+list([j.stop_id])
#                         t=t+list([j.arrival.time])
#                    tp['stop_id']=s
#                    tp['time']=t
#                    tp=pd.merge(tp,stops.loc[stops.stop_id.isin(s),['stop_id','stop_name']],how='left',on='stop_id')
#                    tp=tp.dropna()
#                    tp=calduration(tp)
#                    schedule=schedule.append(tp,ignore_index=True)
                except:
                    print(i)
    except:
        print(i)


r=realtime.groupby(['route_id','trip_id','stop_id','stop_name']).agg({'time':np.median}).sort_values(['trip_id','time']).reset_index()
r=r.groupby('trip_id').filter(lambda x:len(x)>1)

r=calduration(rt)

schedule=schedule.groupby(['startstopid','startstopname','endstopid','endstopname','starthour'],sort=False).agg({'tripduration':np.median}).reset_index()







response.close()

from google.transit import gtfs_realtime_pb2
import multiprocessing as mp
import os
import urllib
import pandas as pd
import datetime
import time
import numpy as np



start=datetime.datetime.now()
pd.set_option('display.max_columns', None)
#path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
#path='C:/Users/Y_Ma2/Desktop/GTFS-RT/'
path='/home/mayijun/GTFS-RT/'
stops=pd.read_csv(path+'Schedule/stops.txt')



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



def cleangtfsrt(fs):
    realtime=[]
    schedule=[]
    for f in fs:
        try:
            feed=gtfs_realtime_pb2.FeedMessage()
            response=urllib.request.urlopen('file:///'+path+'Archive/'+str(m)+'/'+str(d)+'/'+str(f))
            feed.ParseFromString(response.read())
            for entity in feed.entity:
                if entity.HasField('trip_update'):
                    try:
                        # realtime
                        rt=pd.DataFrame(columns=['routeid','tripid','stopid','time'])
                        rt['stopid']=[entity.trip_update.stop_time_update[0].stop_id]
                        rt['time']=[entity.trip_update.stop_time_update[0].arrival.time]
                        rt['routeid']=[entity.trip_update.trip.route_id]
                        rt['tripid']=[entity.trip_update.trip.trip_id]
                        rt=rt.dropna()
                        realtime.append(rt)
                        # schedule
                        sc=pd.DataFrame(columns=['routeid','tripid','stopid','time'])
                        sc['stopid']=[x.stop_id for x in entity.trip_update.stop_time_update[1:]]
                        sc['time']=[x.arrival.time for x in entity.trip_update.stop_time_update[1:]]
                        sc['routeid']=entity.trip_update.trip.route_id
                        sc['tripid']=entity.trip_update.trip.trip_id
                        sc=sc.dropna()
                        sc=sc.sort_values(['routeid','tripid','time']).reset_index(drop=True)
                        sc=calduration(sc)
                        schedule.append(sc)
                    except:
                        print(str(f)+' entity error')
            response.close()
        except:
            print(str(f)+' response error')
    realtime=pd.concat(realtime,axis=0,ignore_index=True)
    schedule=pd.concat(schedule,axis=0,ignore_index=True)
    return realtime,schedule



def parallelize(data, func):
    data_split = np.array_split(data,mp.cpu_count()-1)
    pool = mp.Pool(mp.cpu_count()-1)
    res=pool.map(func, data_split)
    a=pd.concat([x[0] for x in res],axis=0,ignore_index=True)
    b=pd.concat([x[1] for x in res],axis=0,ignore_index=True)
    pool.close()
    pool.join()
    return a,b



if __name__=='__main__':
    months=sorted(os.listdir(path+'Archive/'))[1]
    for m in months:
        dates=sorted(os.listdir(path+'Archive/'+str(m)+'/'))
        for d in dates:
            routes=sorted(pd.unique([x.split('_')[1] for x in os.listdir(path+'Archive/'+str(m)+'/'+str(d)+'/')]))
            for r in routes:
                files=sorted([x for x in os.listdir(path+'Archive/'+str(m)+'/'+str(d)+'/') if x.startswith('gtfs_'+str(r)+'_'+str(d))])
                rttp,sctp=parallelize(files, cleangtfsrt)
                rttp.to_csv(path+'Output/Archive/rttp_'+str(d)+'_'+str(r)+'.csv',index=False,header=True,mode='w')
                sctp.to_csv(path+'Output/Archive/sctp_'+str(d)+'_'+str(r)+'.csv',index=False,header=True,mode='w')
        print(datetime.datetime.now()-start)


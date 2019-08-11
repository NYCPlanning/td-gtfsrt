from google.transit import gtfs_realtime_pb2
import multiprocessing as mp
import os
import urllib
import pandas as pd
import datetime
import numpy as np



starttime=datetime.datetime.now()
pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
#path='C:/Users/Y_Ma2/Desktop/GTFS-RT/'
#path='/home/mayijun/GTFS-RT/'
#path='E:/GTFS-RT/'



def calduration(dt):
    dt['startstopid']=dt['stopid']
    dt['starttime']=dt['time']
    dt['endstopid']=np.roll(dt['stopid'],-1)
    dt['endtime']=np.roll(dt['time'],-1)
    dt['duration']=dt['endtime']-dt['starttime']
    dt=dt.iloc[:-1,:]
    dt=dt.drop(['stopid','time'],axis=1)
    return dt



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
                        # Realtime
                        rt=pd.DataFrame(columns=['routeid','tripdate','tripid','stopid','time'])
                        rt['stopid']=[entity.trip_update.stop_time_update[0].stop_id]
                        rt['time']=[entity.trip_update.stop_time_update[0].arrival.time]
                        rt['routeid']=entity.trip_update.trip.route_id
                        rt['tripdate']=entity.trip_update.trip.start_date
                        rt['tripid']=entity.trip_update.trip.trip_id
                        rt=rt.dropna()
                        realtime.append(rt)
                        # Schedule
                        sc=pd.DataFrame(columns=['stopid','time'])
                        sc['stopid']=[x.stop_id for x in entity.trip_update.stop_time_update]
                        sc['time']=[x.arrival.time for x in entity.trip_update.stop_time_update]
                        sc=sc.dropna()
                        sc=sc.sort_values('time').reset_index(drop=True)
                        sc=calduration(sc)                        
                        sc['routeid']=entity.trip_update.trip.route_id
                        sc['tripdate']=entity.trip_update.trip.start_date
                        sc['tripid']=entity.trip_update.trip.trip_id
                        sc=sc[['routeid','tripdate','tripid','startstopid','endstopid','duration']]
                        schedule.append(sc)
                    except:
                        print(str(f)+' entity error')
            response.close()
        except:
            print(str(f)+' response error')
    try:
        realtime=pd.concat(realtime,axis=0,ignore_index=True)
        schedule=pd.concat(schedule,axis=0,ignore_index=True)
    except:
        print(str(f)+' empty data')
        realtime=pd.DataFrame()
        schedule=pd.DataFrame()
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
    months=sorted(os.listdir(path+'Archive/'))[-1]
    for m in months:
        dates=sorted(os.listdir(path+'Archive/'+str(m)+'/'))
        for d in dates:
            routes=sorted(pd.unique([x.split('_')[1] for x in os.listdir(path+'Archive/'+str(m)+'/'+str(d)+'/')]))
            for r in routes:
                files=sorted([x for x in os.listdir(path+'Archive/'+str(m)+'/'+str(d)+'/') if x.startswith('gtfs_'+str(r)+'_'+str(d))])
                rttp,sctp=parallelize(files, cleangtfsrt)
                rttp.to_csv(path+'Output/Archive/rttp_'+str(d)+'_'+str(r)+'.csv',index=False,header=True,mode='w')
                sctp.to_csv(path+'Output/Archive/sctp_'+str(d)+'_'+str(r)+'.csv',index=False,header=True,mode='w')
        print(datetime.datetime.now()-starttime)


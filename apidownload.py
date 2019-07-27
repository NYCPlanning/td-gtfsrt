from google.transit import gtfs_realtime_pb2
import multiprocessing as mp
import pandas as pd
import time
import numpy as np
import requests



pd.set_option('display.max_columns', None)
path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/'
#path='C:/Users/Y_Ma2/Desktop/GTFS-RT/'
#path='/home/mayijun/GTFS-RT/'
stops=pd.read_csv(path+'Schedule/stops.txt')
key='9aea332316c4d6c5332aecc2a733033d'
fds=['11','2','36','31','51','26','16','21','1']



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



def cleangtfsrt(fd):
    realtime=[]
    schedule=[]
    for f in fd:
        try:
            feed=gtfs_realtime_pb2.FeedMessage()
            response = requests.get('http://datamine.mta.info/mta_esi.php?key='+str(key)+'&feed_id='+str(f))
            feed.ParseFromString(response.content)
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
    try:
        realtime=pd.concat(realtime,axis=0,ignore_index=True)
        schedule=pd.concat(schedule,axis=0,ignore_index=True)
    except:
        print(str(f)+' empty data')
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
    endtime=time.strptime('2019-07-27 20:00:00','%Y-%m-%d %H:%M:%S')
    while time.localtime()<endtime:
        starttime=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
        rttp,sctp=parallelize(fds, cleangtfsrt)
        rttp.to_csv(path+'Output/API/'+'rttp_'+starttime.replace('-','').replace(':','').replace(' ','_')+'.csv',
                    index=False,header=True,mode='w')
        sctp.to_csv(path+'Output/API/'+'sctp_'+starttime.replace('-','').replace(':','').replace(' ','_')+'.csv',
                    index=False,header=True,mode='w')
        time.sleep(0)


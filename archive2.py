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
    for f in fs:
        try:
            response=urllib.request.urlopen('file:///'+path+d+'/'+f)
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
                        rt.to_csv(path+'Output/rttp.csv',index=False,header=False,mode='a')
                        # schedule
                        sc=pd.DataFrame(columns=['routeid','tripid','stopid','time'])
                        sc['stopid']=[x.stop_id for x in entity.trip_update.stop_time_update[1:]]
                        sc['time']=[x.arrival.time for x in entity.trip_update.stop_time_update[1:]]
                        sc['routeid']=entity.trip_update.trip.route_id
                        sc['tripid']=entity.trip_update.trip.trip_id
                        sc=sc.dropna()
                        sc=sc.sort_values(['routeid','tripid','time']).reset_index(drop=True)
                        sc=calduration(sc)
                        sc.to_csv(path+'Output/sctp.csv',index=False,header=False,mode='a')
                    except:
                        print(str(f)+' entity error')
            response.close()
        except:
            print(str(f)+' response error')




def parallelize(data, func):
    data_split = np.array_split(data,mp.cpu_count()-1)
    pool = mp.Pool(mp.cpu_count()-1)
    pool.map(func, data_split)
    pool.close()
    pool.join()
    return data



if __name__=='__main__':
    dates=['20190501']
    routes=['7','ace','bdfm','g','j','L','nqrw']
    for d in dates:
        for r in routes:
            rttp=pd.DataFrame(columns=['routeid','tripid','stopid','time'])
            rttp.to_csv(path+'Output/rttp.csv',index=False,header=True,mode='w')
            sctp=pd.DataFrame(columns=['routeid','tripid','starthour','startstopid','starttime','endstopid','endtime','duration'])
            sctp.to_csv(path+'Output/sctp.csv',index=False,header=True,mode='w')
            feed = gtfs_realtime_pb2.FeedMessage()
            files=sorted([x for x in os.listdir(path+d+'/') if x.startswith('gtfs_'+r+'_'+d)])[0:100]
            parallelize(files, cleangtfsrt)
            rttp=pd.read_csv(path+'Output/rttp.csv',dtype=str)
            rttp['time']=pd.to_numeric(rttp['time'])
            rttp=rttp.groupby(['routeid','tripid','stopid'],as_index=False).agg({'time':'median'})
            rttp=rttp.sort_values(['routeid','tripid','time']).reset_index(drop=True)
            rttp=rttp.groupby(['routeid','tripid'],as_index=False).apply(calduration).reset_index(drop=True)
            sctp=pd.read_csv(path+'Output/sctp.csv',dtype=str)
            sctp['duration']=pd.to_numeric(sctp['duration'])
            sctp=sctp.groupby(['routeid','tripid','startstopid','endstopid'],as_index=False).agg({'duration':'median'})
            sctp.columns=['routeid','tripid','startstopid','endstopid','schedule']
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
            tp.to_csv(path+'Output/'+d+'_'+r+'2.csv',index=False,header=True,mode='w')            
            print(datetime.datetime.now()-start)




#tp=tp[tp.starthour.isin(['06','07','08','09'])]
#tp=tp.groupby(['routeid','startstopid','endstopid'],as_index=False).agg({'duration':['min','median','mean','max','count'],
#             'schedule':['min','median','mean','max','count'],'delaypct':['min','median','mean','max','count']})
#
#tp=tp.groupby(['routeid','starthour','startstopid','endstopid'],as_index=False).agg({'duration':['min','median','max','count'],
#             'schedule':['min','median','max']})
#tp.columns=[x[0]+x[1] for x in tp.columns]
#
#
#



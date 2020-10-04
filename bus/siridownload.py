import requests
import pandas as pd
import numpy as np
import datetime
import time
import pytz



pd.set_option('display.max_columns', None)
# path='C:/Users/mayij/Desktop/DOC/DCP2019/GTFS-RT/Bus/'
path='/home/mayijun/GTFS-RT/Bus/'
url='http://bustime.mta.info/api/siri/vehicle-monitoring.json?key='+pd.read_csv(path+'KEY.csv',dtype=str).loc[0,'key']



endtime=datetime.datetime(2020,12,31,23,0,0,0,pytz.timezone('US/Eastern'))
while datetime.datetime.now(pytz.timezone('US/Eastern'))<endtime:
    tp=pd.DataFrame(requests.get(url).json()).loc['ServiceDelivery','Siri']
    ts=datetime.datetime.strptime(tp['ResponseTimestamp'].split('.')[0],'%Y-%m-%dT%H:%M:%S').strftime('%Y%m%d_%H%M%S')
    tp=tp['VehicleMonitoringDelivery'][0]['VehicleActivity']
    veh=pd.DataFrame()
    veh['veh']=''
    veh['time']=''
    veh['lat']=np.nan
    veh['long']=np.nan
    veh['line']=''
    veh['dir']=''
    veh['dest']=''
    veh['jrn']=''
    veh['stpid']=''
    veh['stpnm']=''
    veh['dist']=np.nan
    veh['pax']=np.nan
    veh['cap']=np.nan
    for j in range(0,len(tp)):
        try:
            if tp[j]['MonitoredVehicleJourney']['ProgressRate']=='normalProgress':
                veh.loc[j,'veh']=tp[j]['MonitoredVehicleJourney']['VehicleRef']
                veh.loc[j,'time']=datetime.datetime.strptime(tp[j]['RecordedAtTime'].split('.')[0],'%Y-%m-%dT%H:%M:%S').strftime('%m%d%H%M%S')
                veh.loc[j,'lat']=tp[j]['MonitoredVehicleJourney']['VehicleLocation']['Latitude']
                veh.loc[j,'long']=tp[j]['MonitoredVehicleJourney']['VehicleLocation']['Longitude']
                veh.loc[j,'line']=tp[j]['MonitoredVehicleJourney']['PublishedLineName']
                veh.loc[j,'dir']=tp[j]['MonitoredVehicleJourney']['DirectionRef']
                veh.loc[j,'dest']=tp[j]['MonitoredVehicleJourney']['DestinationName']
                veh.loc[j,'jrn']=tp[j]['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef']
                veh.loc[j,'stpid']=tp[j]['MonitoredVehicleJourney']['MonitoredCall']['StopPointRef']
                veh.loc[j,'stpnm']=tp[j]['MonitoredVehicleJourney']['MonitoredCall']['StopPointName']
                veh.loc[j,'dist']=tp[j]['MonitoredVehicleJourney']['MonitoredCall']['Extensions']['Distances']['CallDistanceAlongRoute']
                try:
                    veh.loc[j,'pax']=tp[j]['MonitoredVehicleJourney']['MonitoredCall']['Extensions']['Capacities']['EstimatedPassengerCount']
                    veh.loc[j,'cap']=tp[j]['MonitoredVehicleJourney']['MonitoredCall']['Extensions']['Capacities']['EstimatedPassengerCapacity']
                except:
                    pass
            else:
                print(tp[j]['MonitoredVehicleJourney']['VehicleRef']+' no progress')
        except:
            print(tp[j]['MonitoredVehicleJourney']['VehicleRef']+' error')
    veh.to_csv(path+'SIRI/Raw/rttp_'+ts+'.csv',index=False)
    time.sleep(30)




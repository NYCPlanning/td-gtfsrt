# -*- coding: utf-8 -*-
"""
Created on Tue Jul  9 16:38:52 2019

@author: Y_Ma2
"""

from google.transit import gtfs_realtime_pb2
import urllib

key=''

feed = gtfs_realtime_pb2.FeedMessage()
response = urllib.request.urlopen('http://datamine.mta.info/mta_esi.php?key='+str(key)+'&feed_id=1')
feed.ParseFromString(response.read())
for entity in feed.entity:
  if entity.HasField('trip_update'):
    print(entity.trip_update)
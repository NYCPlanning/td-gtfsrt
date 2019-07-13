import google.transit as gtfs
import urllib

path='C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2019/GTFS-RT/20190501/'

feed = gtfs.gtfs_realtime_pb2.FeedMessage()

response = urllib.request.urlopen('file:///'+path+'gtfs_7_20190501_041947.gtfs')
feed.ParseFromString(response.read())

for entity in feed.entity:
  if entity.HasField('trip_update'):
    print entity.trip_update
f.close()

from google.transit import gtfs_realtime_pb2
import requests

key='9aea332316c4d6c5332aecc2a733033d'

feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get('http://datamine.mta.info/mta_esi.php?key='+str(key)+'&feed_id=51')
feed.ParseFromString(response.content)
for entity in feed.entity:
  if entity.HasField('trip_update'):
    print(entity.trip_update)
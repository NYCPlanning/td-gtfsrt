from google.transit import gtfs_realtime_pb2
import requests

key='9aea332316c4d6c5332aecc2a733033d'

feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get('http://datamine.mta.info/mta_esi.php?key='+str(key)+'&feed_id=51')
feed.ParseFromString(response.content)
for entity in feed.entity:
  if entity.HasField('trip_update'):
    print(entity.trip_update)





import gtfs_tripify as gt
response1 = requests.get('http://datamine.mta.info/mta_esi.php?key='+str(key)+'&feed_id=1')
response2 = requests.get('http://datamine.mta.info/mta_esi.php?key='+str(key)+'&feed_id=51')
stream = [response1.content,response2.content]
logbook, timestamps, parse_errors = gt.logify(stream)

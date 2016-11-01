#import gtfs_realtime_pb2
from google.transit import gtfs_realtime_pb2
import urllib

feed = gtfs_realtime_pb2.FeedMessage()
response = urllib.urlopen('http://developers.cata.org/gtfsrt/vehicle/vehiclepositions.pb')
feed.ParseFromString(response.read())
print(feed.header.timestamp)
for entity in feed.entity:
  if entity.HasField('trip_update'):
    print (entity.trip_update)
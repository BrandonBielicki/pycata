#import gtfs_realtime_pb2
from google.transit import gtfs_realtime_pb2
import urllib
import auth
import requests

auth_key=auth.auth_key
ftp_url=auth.ftp_url
fb_timestamp_url="https://sizzling-fire-5776.firebaseio.com/timestamp.json?auth="+auth_key
feed_url="http://developers.cata.org/gtfsrt/GTFS-RealTime/TrapezeRealTimeFeed.pb"

def firebaseCall(_url, _method, _data):
  if(_method == "post"):
    response = requests.post(_url, _data)
  elif(_method == "get"):
    response = requests.get(_url, _data)
  elif(_method == "put"):
    response = requests.put(_url, _data)
  elif(_method == "delete"):
    response = requests.delete(_url, _data)
  content = response.content
  code = response.status_code
  return response
  
feed = gtfs_realtime_pb2.FeedMessage()
response = urllib.urlopen(feed_url)
feed.ParseFromString(response.read())
firebaseCall(fb_timestamp_url,"put",str(feed.header.timestamp));
print(feed.header.timestamp)


for entity in feed.entity:
  if entity.HasField('trip_update'):
    print (entity.trip_update)

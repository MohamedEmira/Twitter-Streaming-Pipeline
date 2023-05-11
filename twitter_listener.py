# For sending GET requests from the API
import requests

# For saving access tokens and for file management when creating and adding to the dataset
import os

# For dealing with json responses we receive from the API
import json

# For displaying the data after
import pandas as pd

# For saving the response data in CSV format
import csv

# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata

#To add wait time between requests
import time

#To open up a port to forward tweets
import socket 

os.environ['TOKEN'] = "AAAAAAAAAAAAAAAAAAAAAIdCcgEAAAAADxqlmxSiZLO05fKmfbrX7G3ckqQ%3DCCPSGoWTDF6uu4qdFsncsuOat5GFTTFv5blXPdA4ueK4YLu3gg"

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)

def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token   #params object received from create_url function
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "cleopatra lang:en"
start_time = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
start_time = start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')

end_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=10)
end_time = end_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
max_results = 30

url = create_url(keyword, start_time,end_time, max_results)
json_response = connect_to_endpoint(url[0], headers, url[1])

s = socket.socket()
host = "127.0.0.1"
port = 7777
s.bind((host, port))
print("Listening on port: %s" % str(port))
s.listen(5)
clientsocket, address = s.accept()
print("Received request from: " + str(address)," connection created.")

for data in json_response['data']:
    author_id = data['author_id']
    author_info = next(item for item in json_response['includes']['users'] if item['id'] == author_id)
    author_name = author_info['username']
    public_metrics = author_info['public_metrics']
    tweet = {"id": data['id'], "text": data['text'], "in_reply_to_user_id": data.get('in_reply_to_user_id', ''), "created_at": data['created_at'], "public_metrics": data['public_metrics'], "source": data.get('source', ''),"username":author_name,"user_metrics":public_metrics }
    json_tweet = json.dumps(tweet)
    json_tweet_with_separator = json_tweet + '\n' # Add separator
    clientsocket.send(json_tweet_with_separator.encode('utf-8'))
    time.sleep(2)
clientsocket.close()
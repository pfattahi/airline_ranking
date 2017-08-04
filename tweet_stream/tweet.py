import tweepy
import json
from tweepy import OAuthHandler
 
def process_or_store(tweet):
#    a=json.dumps(tweet)
#    print(json.dumps(tweet, sort_keys=True, indent=4))
	a=json.loads(json.dumps(tweet))
#	print a['text'].encode('ascii', 'ignore')
#    print "a:", a[0] 
#     line_object = json.loads(tweet)
#     print "Line:", line
	with open('python.json', 'a') as f:
                f.write(json.dumps(tweet))
                return True


consumer_key = 't5AGKRMAP9jxKLiHIBeIOWROx'
consumer_secret = 'j6IWkW3fEN0JkckZty9adylFMEFUsNvlKo2iCU7yX8gbbhoLdf'
access_token = '55853810-LmmIHxxlzD0AAT1sQ4c3mbAbZHXDoNqTyFp2EQQMY'
access_secret = 'ZRh5zEwJ8aID1cunNgkt4vnl18Kh6Q0gd5mZBrMklO8xN'
 
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
 
api = tweepy.API(auth)
#tweet=api.search(q="@VirginAmerica")

#print(json.dumps(tweet))
for tweet in tweepy.Cursor(api.search,q="@VirginAmerica").items():
#for tweet in tweepy.Cursor(api.search(q="@VirginAmerica")).items():
    process_or_store(tweet._json)
    # Process a single status
#    print(status.text)


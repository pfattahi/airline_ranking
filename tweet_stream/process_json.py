import json
 
with open('airlines_tweets/stream_out_tweets.json.json', 'r') as f:
    i=0
    for line in f: # read only the first tweet/line
        i=1+i
        if i>4:
            break
        tweet = json.loads(line) # load it as Python dict
        print(json.dumps(tweet, indent=4)) # pretty-print
        print "------------------------------------------------------------------------------------------------" 




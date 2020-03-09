from kafka import KafkaConsumer
import json
from textblob import TextBlob
from kafka import KafkaProducer


# consumer = KafkaConsumer('rams',group_id='my_favorite_group')

key='iphone'
consumer1 = KafkaConsumer(key,auto_offset_reset='earliest', bootstrap_servers=['ec2-3-81-83-213.compute-1.amazonaws.com:9092'])
favorite_count_min=100000000
favorite_count_max=0
favorite_count_sum=0
favorite_count_n=0
favorite_count_range=(100000000,0)
favorite_count_average=0
followers_count_min=100000000
followers_count_max=0
followers_count_sum=0
followers_count_n=0
followers_count_range=[100000000,0]
followers_count_average=0

producer = KafkaProducer(bootstrap_servers=['ec2-3-81-83-213.compute-1.amazonaws.com:9092'])
tweet_dict= {}

for msg in consumer1:
    tweet_text_json=json.loads(msg.value.decode('utf-8'))
    try:
        tweet_favorite_count = tweet_text_json['retweeted_status']['favorite_count']
        tweet_followers_count = tweet_text_json['retweeted_status']['user']['followers_count']

        favorite_count_sum=favorite_count_sum+tweet_favorite_count
        favorite_count_n=favorite_count_n+1
        favorite_count_average=favorite_count_sum/favorite_count_n

        followers_count_sum=followers_count_sum+tweet_followers_count
        followers_count_n=followers_count_n+1
        followers_count_average=followers_count_sum/followers_count_n

        if tweet_followers_count < followers_count_min:
            followers_count_min=tweet_followers_count
            followers_count_range=[followers_count_min,followers_count_range[1]]
        if tweet_followers_count > followers_count_max:
            followers_count_max=tweet_followers_count
            followers_count_range=[followers_count_range[0],followers_count_max]
            # sentiment = "negative"
        if tweet_favorite_count < favorite_count_min:
            # sentiment = "neutral"
            favorite_count_min=tweet_favorite_count
            favorite_count_range=[favorite_count_min,favorite_count_range[1]]
        if tweet_favorite_count > favorite_count_max:
            # sentiment = "neutral"
            favorite_count_max=tweet_favorite_count
            favorite_count_range=[favorite_count_range[0],favorite_count_max]
        
        tweet_dict['topic']=key
        tweet_dict['timestamp_ms']=tweet_text_json['timestamp_ms']
        tweet_dict['favorite_count_min']=favorite_count_min
        tweet_dict['favorite_count_max']=favorite_count_max
        tweet_dict['favorite_count_average']=favorite_count_average
        tweet_dict['favorite_count_range']=favorite_count_range
        tweet_dict['followers_count_min']=followers_count_min
        tweet_dict['followers_count_max']=followers_count_max
        tweet_dict['followers_count_average']=followers_count_average
        tweet_dict['followers_count_range']=followers_count_range
        producer.send(key+'_'+'stats', bytes(json.dumps(tweet_dict),'utf-8'))
        print(json.dumps(tweet_dict))
    except KeyError:
        print("No retweet status")
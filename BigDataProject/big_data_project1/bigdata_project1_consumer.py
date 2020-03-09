from kafka import KafkaConsumer
import json
from textblob import TextBlob
from kafka import KafkaProducer


# consumer = KafkaConsumer('rams',group_id='my_favorite_group')
key='iphone'
consumer1 = KafkaConsumer(key,group_id='sentiment_analysis_group', bootstrap_servers=['ec2-3-81-83-213.compute-1.amazonaws.com:9092'])
producer = KafkaProducer(bootstrap_servers=['ec2-3-81-83-213.compute-1.amazonaws.com:9092'])

for msg in consumer1:
    tweet_text_json=json.loads(msg.value.decode('utf-8'))
    tweet = TextBlob(tweet_text_json['text'])
    if tweet.sentiment.polarity < 0:
        producer.send(key+'_'+'negative', msg.value)
        tweet_text_json['sentiment'] ='negative'
        # sentiment = "negative"
    elif tweet.sentiment.polarity == 0:
        # sentiment = "neutral"
        producer.send(key+'_'+'neutral', msg.value)
        tweet_text_json['sentiment'] ='neutral'
    else:
        producer.send(key+'_'+'positive', msg.value)
        tweet_text_json['sentiment'] ='positive'
        # sentiment = "positive"
    tweet_text_json['sentiment_value'] =tweet.sentiment.polarity
    producer.send(key+'_'+'sentiment', bytes(json.dumps(tweet_text_json),'utf-8'))

    print(tweet_text_json['sentiment'])

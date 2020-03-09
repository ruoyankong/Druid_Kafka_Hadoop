from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from kafka.client import SimpleClient
from kafka.producer import SimpleProducer
from kafka import KafkaConsumer
import json
import os
import sys
# client = SimpleClient("127.0.0.1:9092")
# producer = SimpleProducer(client)

producer = KafkaProducer(bootstrap_servers=['ec2-3-81-83-213.compute-1.amazonaws.com:9092'])
consumer_key= 'tYCzKOoBOZft1oy0cC85l0mxU'
consumer_secret= 'tS2e6cMQv60ZtvoBaAXenLrkr26oSg8c22fyrcjSUvHZGNyIuS'
access_token= '4808614249-s758NEUKUDHjmspTSqqak1zSNkmgOI3mOyio0tv'
access_token_secret= 'JBW4qGXXs3MAXCgwplERYOqnCiJylN3uF8oKlPWn3r7wn'
class StdOutListener(StreamListener):
    key=''
    def set_keys(self,key):
        self.key = key
    def on_data(self, data):
        # if str(sys.argv[1]) in str(data):
        # if 'Rams' in str(data):
        if self.key in str(data):
            # producer.send(str(sys.argv[1]), bytes(data,'utf-8'))
            # producer.send('rams', bytes(data,'utf-8'))
            producer.send(self.key, bytes(data,'utf-8'))    
        print(data)
        return True
    def on_error(self, status):
        print(status)
if __name__ == '__main__':
    key='iphone'
    # os.system('kafka-topics.sh --zookeeper ec2-3-81-83-213.compute-1.amazonaws.com:2181 --create --topic'+' '+key+' --partitions 3 --replication-factor 2')
    l = StdOutListener()
    l.set_keys(key)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=[key],languages=["en"])
    # stream.filter(track=['Rams Seahawks'],languages=["en"])
    # stream.filter(track=[str(sys.argv[1]),str(sys.argv[2])],languages=["en"])
# if __name__ == '__main__':
#     print(type(sys.argv) is list)
   
       
    


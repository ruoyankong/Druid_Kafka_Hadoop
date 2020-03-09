from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from kafka.client import SimpleClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

client = SimpleClient("localhost:9092")
producer = SimpleProducer(client)
access_token = "972524746838458368-ttzXLBfz51JfEu8BNqzoNxi6oe2KX5a"
access_token_secret =  "FopdEcVsnvLHziYAxWyYwCPX6y556wPF28XMlvyqS4uuG"
consumer_key =  "0NWVlfYykqpOitJ2a0RRme0Dy"
consumer_secret =  "NDXbMlkbeuEsX5NfMV2lAUs98CZTDBR2gLZbmz2z6gwRxHjc1W"

class StdOutListener(StreamListener):
  def on_data(self, data):
    producer.send_messages('trump', str(data))
    print(data)
    return True

  def on_error(self, status):
    print(status)

if __name__ == '__main__':
	l = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, l)
	stream.filter(track=['trump'],languages=["en"])


export PYSPARK_PYTHON=/usr/bin/python3.6 
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.6
export SPARK_YARN_USER_ENV="PYSPARK_PYTHON=/usr/local/bin/python3.6"
import findspark
findspark.init()
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sys
import webbrowser
import codecs
import csv
from string import punctuation
import matplotlib.pyplot a plt 
import time
from kafka import SimpleProducer, KafkaClient
import tweepy
import configparser


#Producer
class tweetlistener(StreamListener):
	def .__init__(self.api):
		self.api=api
		super(tweepy.StreamListener,self).__init__()
		client = KafkaClient("localhost:9092")
		print(client) 

		self.producer=SimpleProducer(client,async=True,
			batch_send_every n = 1000,
			batch_send_every t = 10)
		print("after init producer")

	def on_status(self,status):
		global counter,Total_tweet_count,outfile,search_words_list,indiv,outfile
		counter += 1
		if(counter >= Total_tweet_count):
			search_words_list.pop(0)
			outfile.close()
			search_tweets()

		print("---------------NEW TWEET ARRIVED---------------")
		print("Tweet Text: %s " %status.text)
		outfile.write(status.text)
		outfile.write(str("\n"))
		print("Author's screen name: %s" %status.author.screen_name)
		print("Time of creation:%s" %status.created_at)
		print("Source of tweet: %s" %status.source)


		msg = status.text.encodde('utf-8')
		try:
			self.producer.send_message(b'kafkatwitterstream',msg)
		except Exception as e:
			print(e)
			return False
		return True



	def on_error(self,status):
		print("Too soon reconnected. Will terminate the program")
		print(status)
		sys.exit()
		print("----------------- End of stream ------------------")			



	def main():
		global Total_tweet_count, outfile, file, search_words_list, auth, labels, colors, all_figs
		search_words = str(raw_input("Enter the words to search:"))

		Total_tweet_count = int(raw_input("Number of tweets to be pulled for the search word:"))

		search_words_list = search_words.split(",")
		labels = ['Positive','Negetive']
		colors = ['yellowgreen','lightcoral']
		all_figs=[]
		search_tweets()

	def search_tweets():
		global search_words_list,counter,auth,indiv,outfile,file2,plt,access
		consumer_key = 'SYYmY5L3Q2XlHu80BGi8veVNt'
		consumer_secret = '02T0XxTCcuYd9vfmpnZBGP73nFgy859ShUyaxzq1otdGZdj2SP'
		access_token = '798942251862749184-hh5kzc5jXQB8eVWYPKSS72G5E5lpH8E'
		access_secret = 'l2hBVBuMYTiTFuvVdV706tylZx4ftGubhmczbPQIR9DP9'
		auth = OAuthHandler(consumer_key,consumer_secret)
		auth.set_access_token(access_token,access_secret)
		api = tweepy.API(auth)

		for indiv in search_words_list:
			print("Search word " + indiv + "is being processed")
			counter = 0


			file2 = "/home/maria_dev/tweettext"  + str(indiv[0]+ ".txt"
			outfile = codecs.open(file2,'w',"utf-8")
			twitterStream = Sitream(auth,tweetlistener(api))
			one_list = []
			one_list.append(indiv)
			print(one_list)
			twitterStream.filter(track = one_list,languages=["en"])

		sys.exit()



	main()





















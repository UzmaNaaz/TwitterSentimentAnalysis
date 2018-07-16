from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext 
from pyspark.streaming.kafka import kafkaUtils 
import operator 
import numpy as np
import matplotlib.pyplot as plt

#consumer

def main();
	global labels, colors, all_figs
	all_figs=[]
	conf = SparkConf().setMaster("local[2]").setAppName("SentimentAnalysis")
	sc = SparkContext(conf=conf)

	ssc = StreamingContext(sc,5)
	ssc.checkpoint("checkpoint")
	
	pwords = load_wordlist("/home/maria_dev/positive_words.txt")
	nwords = load_wordlist("/home/maria_dev/negetive_words.txt")

	counts = stream(ssc,pwords,nwords,60)
	print("Inside main function")
	print(counts)

def load_wordlist(filename):
	words=[]
	f=open(filename,'rU')
	text = f.read()
	text = text.split('\n')
	for line in text:
		words[line]=1

	f.close()
	return words


def update_function(newValues,runningCount):
	if(runningCount is None):
		rrunningCount = 0
	return sum(newValues,runningCount)



def stream(ssc,pwords,nwords,duration):
	global labels,colors,all_figs
	kstream = kafkaUtils.createDirectStream(ssc, topics = ['kafkatwitterstream'], 
		kafkaParams=("metadata.broker.list":'localhost:9092'))
	tweets = kstream.map(lambda x : x[1].encode("ascii","ignore"))
	words = tweets.flatMap(lambda line: line.split(" "))
	positive = words.map(lambda word: ('Positive',1) if word in pwords else('Positive,0'))
	negetive = words.map(lambda word: ('Negetive',1) if word in nwords else('Negetive,0'))
	allSentiments = positive.union(negetive)
	sentimentCounts = allSentiments.reduceByKey(lamba x,y: x + y)
	runningSentimentCounts = sentimentCounts.updateStateByKey(updateFunction)
	runningSentimentCounts.print()







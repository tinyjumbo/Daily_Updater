#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
this small program is to count the number of tweets of each company
update the number and score in tcount collection for each company
'''

import json
from datetime import datetime,timedelta
from pymongo import MongoClient
import pandas as pd
import datetime 
from senti_classifier import senti_classifier
import time
import os


#Mongo config
client = MongoClient()
client = MongoClient('mongodb://162.243.122.37:27017/')

#sample the tweets 1/sample
sample=100
timeslot=10



def Count_Update(company,score):
	query_collection= getattr(client.tinyjumbo,company)
	update_collection = client.tinyjumbo.tcount
	end = datetime.datetime.now()
	start = end - timedelta(1)

	#print query_collection.find_one()
	count = query_collection.find({'time': {'$gte': str(start), '$lt': str(end)}}).count()
	record={"company":company,"count":count,"date":datetime.datetime.now(),"score":score}
	result=update_collection.insert_one(record)
	#print result.inserted_id

def read(a):
    tweet=pd.read_csv(a)
    date=tweet['time']
    text=tweet['text']
    day=pd.to_datetime(date,format='%Y-%m-%d %H:%M:%S.%f')
    day=day.map(lambda x: x.strftime('%Y-%m-%d'))
    text = map(lambda x: [x], text)
    tweet=pd.DataFrame(text,index=day,columns=['text'])
    return tweet

def sentiment_score(dataset,sample):
	count=pos_sum=neg_sum=0
	for sentence in dataset:
		if count%sample==0:
			pos_score, neg_score = senti_classifier.polarity_scores([sentence])
			print "pos_score: " + str(pos_score) + "  neg_score" + str(neg_score)
			pos_sum+=pos_score
			neg_sum+=neg_score
		count+=1
	pos_score,neg_score=pos_sum/(pos_sum+neg_sum),neg_sum/(pos_sum+neg_sum)	
	return pos_score-neg_score


def query_and_process(company):
	query="mongoexport -h 162.243.122.37:27017 -d tinyjumbo -c "\
			 +company+ " --type=csv --fields time,text -q '{ \"time\": { $gt: \"2016-03\", $lt: \"2016-03-13 24:00\" } }' --out "\
			 +company+".csv"
	os.system(query)
	time.sleep(timeslot)
	tweet=read(company+".csv")
	sub=tweet.loc[str(datetime.datetime.now().date())]
	score=sentiment_score(sub['text'],sample)

	Count_Update(company,score)

	os.system("rm -f "+company+".csv")	


	return


#save daily tweets as csv
query_and_process("google")
query_and_process("amazon")
query_and_process("facebook")

print "finished"


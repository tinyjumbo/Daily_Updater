#!/usr/bin/env python
# -*- coding: utf-8 -*-


import pandas as pd
import datetime 
from senti_classifier import senti_classifier

import json
from datetime import datetime,timedelta
from pymongo import MongoClient
import pandas as pd
import datetime 
from senti_classifier import senti_classifier
import time
import os


#sample the tweets 1/sample
sample=5


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
	return pos_score,neg_score


def query_format(company):
	query="mongoexport -h 162.243.122.37:27017 -d tinyjumbo -c "\
			 +company+ " --type=csv --fields time,text -q '{ \"time\": { $gt: \"2016-03\", $lt: \"2016-03-13 24:00\" } }' --out "\
			 +company+".csv"
	return query

#save daily tweets
google_query=query_format("google")

os.system(google_query)
time.sleep(3)

tweet=read("google.csv")
sub=tweet.loc[str(datetime.datetime.now().date())]
pos_score,neg_score=sentiment_score(sub['text'],sample)
print pos_score,neg_score


















#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This small program is to count the number of tweets of each company
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

# Mongo config
client = MongoClient()
client = MongoClient('mongodb://162.243.122.37:27017/')

# Sample the tweets 1/sample
SAMPLE = 100
TIMESLOT = 10
CURRDATE = str(datetime.datetime.now().date())

# Update date and score infomation in MongoDB
def Count_Update(company, score):
    query_collection = getattr(client.tinyjumbo,company)
    update_collection = client.tinyjumbo.tcount
    end = CURRDATE + " 24" 
    start = CURRDATE
    count = query_collection.find({'time': {'$gte': start, '$lt': end}}).count()
    record = {"company":company,"count":count,"date":CURRDATE,"score":score}
    result = update_collection.insert_one(record)
    return


# Read csv and modify date for later process
def read(a):
    tweet = pd.read_csv(a)
    date = tweet['time']
    text = tweet['text']
    day = pd.to_datetime(date,format = '%Y-%m-%d %H:%M:%S.%f')
    day = day.map(lambda x: x.strftime('%Y-%m-%d'))
    text = map(lambda x: [x], text)
    tweet = pd.DataFrame(text, index = day, columns = ['text'])
    return tweet


# Caculate score by using senti_classifier
def sentiment_score(dataset, sample):
    count = pos_sum = neg_sum = 0
    for sentence in dataset:
        if count%sample==0:
            pos_score, neg_score = senti_classifier.polarity_scores([sentence])
            print "pos_score: " + str(pos_score) + "  neg_score" + str(neg_score)
            pos_sum += pos_score
            neg_sum += neg_score
        count += 1
    sum_val = pos_sum + neg_sum
    pos_score,neg_score = pos_sum/max(0.0000001,sum_val),neg_sum/max(0.0000001,sum_val)    
    return pos_score - neg_score


# Mian function
def query_and_process(company):
    # Query data
    query = "mongoexport -h 162.243.122.37:27017 -d tinyjumbo -c " + company + " --type=csv --fields time,text -q '{ \"time\": { $gt: \"" + CURRDATE + "\", $lt: \"" + CURRDATE + " 24:00\" } }' --out " + company + ".csv"
    os.system(query)
    time.sleep(TIMESLOT)
    # Read data
    tweet = read(company + ".csv")
    sub = tweet.loc[CURRDATE]
    # Get score
    score = sentiment_score(sub['text'],SAMPLE)
    # Update DB
    Count_Update(company,score)
    os.system("rm -f " + company + ".csv")    
    return


# Save daily tweets as csv
names = ["google", "amazon", "facebook"]
map(query_and_process, names)
print "finished"

''' if want to caculate mutiple days. Use following:
hard_code_time = '2016-03-20'
timearry = [hard_code_time[:-2]+str(int(hard_code_time[-2:])+i) for i in range(13) ]

for t in timearry:
    CURRDATE = t
    print CURRDATE
    map(query_and_process, names)
    print "finished"
'''

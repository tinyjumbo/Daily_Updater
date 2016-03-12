#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
this small program is to count the number of tweets of each company
update the number in tcount collection for each company
leave the score attribute as None for future development
'''

import json
from datetime import datetime,timedelta
from pymongo import MongoClient


#Mongo config
client = MongoClient()
client = MongoClient('mongodb://162.243.122.37:27017/')



def Count_Update(company):
	query_collection= getattr(client.tinyjumbo,company)
	update_collection = client.tinyjumbo.tcount
	end = datetime.now()
	start = end - timedelta(1)

	#print query_collection.find_one()
	count = query_collection.find({'time': {'$gte': str(start), '$lt': str(end)}}).count()
	record={"company":company,"count":count,"date":datetime.now(),"score":None}
	result=update_collection.insert_one(record)
	#print result.inserted_id


Count_Update("amazon")
Count_Update("facebook")
Count_Update("google")


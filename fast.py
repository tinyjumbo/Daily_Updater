
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 12 12:49:21 2016
@author: YI
"""

import pandas as pd


#read csv
def read(a):
    tweet=pd.read_csv(a)
    date=tweet['time']
    text=tweet['text']
    day=pd.to_datetime(date,format='%Y-%m-%d %H:%M:%S.%f')
    day=day.map(lambda x: x.strftime('%Y-%m-%d'))
    text = map(lambda x: [x], text)
    tweet=pd.DataFrame(text,index=day,columns=['Text'])
    return tweet
    
tweet=read('tweet.csv')

print tweet
#filter data based on date
sub226=tweet.loc[['2016-02-28']]
print sub226
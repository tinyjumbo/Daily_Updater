import pandas as pd
import datetime 


tweets=pd.read_csv("tweet.csv")



for i in xrange(0,len(tweets["time"])):
	print i
	tweets["time"][i] = datetime.datetime.strptime(tweets["time"][i], "%Y-%m-%d %H:%M:%S.%f").date()


sub=tweets.loc[tweets['time']==datetime.date(2016, 2, 15)]


print len(sub)

print tweets["time"][0]

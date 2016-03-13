import pandas as pd
import datetime 
from senti_classifier import senti_classifier


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



tweet=read("tweet.csv")
sub=tweet.loc['2016-02-26']
pos_score,neg_score=sentiment_score(sub['text'],100)
print pos_score,neg_score


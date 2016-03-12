import pandas as pd
import datetime 
from senti_classifier import senti_classifier

'''
sentences = ['The movie was the good movie', 'It was the worst acting by the actors']

pos_score, neg_score = senti_classifier.polarity_scores(sentences)
print "_____________________"
print "pos_score: " + str(pos_score) + "neg_score" + str(neg_score)
print "_____________________"
'''


tweets=pd.read_csv("tweet.csv")



#for i in xrange(0,len(tweets["time"])):
for i in xrange(0,10000):

	print i
	tweets["time"][i] = datetime.datetime.strptime(tweets["time"][i], "%Y-%m-%d %H:%M:%S.%f").date()


sub=tweets.loc[tweets['time']==datetime.date(2016, 2, 15)]
print sub['text']




for i in xrange(0,100):
	sentences = sub['text'][i]
	#print sentences
	pos_score, neg_score = senti_classifier.polarity_scores([sentences])
	print "pos_score: " + str(pos_score) + "  neg_score" + str(neg_score)
	

print sub[0]

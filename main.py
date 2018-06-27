import pandas as pd
import time
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob
import re

def main():

    # Authentification
    consumer_key = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    consumer_secret = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    access_token = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    access_secret = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    api = tweepy.API(auth)

    # Collect tweets
    macronTweets = tweepy.Cursor(api.search, q='EmmanuelMacron', count=10000)

    def clean_tweet(tweet):
        '''
        Utility function to clean tweet text by removing links, special characters using simple regex statements.
        '''
    return " ".join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet_text).split())



    def get_tweet_sentiment(tweet_text):
        '''
        Utility function to classify sentiment of passed tweet using textblob's sentiment method
        '''
        # create TextBlob object of passed tweet text
        analysis = TextBlob(clean_tweet(tweet_text))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'


    def create_dico_from_tweet(tweet):
        dico = {'tweet_id': tweet.id_str,
                "tweet_datetime": tweet.created_at,
                "tweet_lang": tweet.lang,
                "tweet_raw_text": tweet.text,
                "tweet_cleaned_text": clean_tweet(tweet.text),
                "tweet_sentiment": get_tweet_sentiment(tweet.text),
                "tweet_hashtags": [x['text'] for x in tweet.entities['hashtags']],
                "tweet_hashtags_indices": [x['indices'] for x in tweet.entities['hashtags']],
                "tweet_user_mentions_id": [x['id'] for x in tweet.entities['user_mentions']],
                "tweet_user_mentions_screen_name": [x['screen_name'] for x in tweet.entities['user_mentions']],
                "tweet_user_mentions_name": [x['name'] for x in tweet.entities['user_mentions']],
                "tweet_user_mentions_indices": [x['name'] for x in tweet.entities['user_mentions']],
                "tweet_symbols": tweet.entities['symbols'],
                "tweet_nb_retweets": tweet.retweet_count,
                "user_id": tweet.user.id_str,
                "user_id_verified":tweet.user.verified,
                "user_nb_followers": tweet.user.followers_count}
        return dico


    list_of_data = []
    while True:
        try:
            tweet = macronTweets.items(10).next()
            list_of_data.append(create_dico_from_tweet(tweet))
            # Insert into db
        except StopIteration:
            break
        except tweepy.TweepError:
            print("limit reached")
            break

    tweets_pd = pd.DataFrame(list_of_data, columns=create_dico_from_tweet(tweet).keys())

    tweets_pd.to_csv(path + "tweets_reduced.csv", sep=';', index=False)
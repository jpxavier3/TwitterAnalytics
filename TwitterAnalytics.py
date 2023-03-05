"""TwitterAnalytics, for automatic analysis on custom twitter's hashtags"""

from datetime import datetime, timedelta
import pandas as pd
import tweepy as ty
import plotly.express as px
from unidecode import unidecode
import warnings
warnings.filterwarnings('ignore')


class TwitterAnalytics:
    """
    TwitterAnalytics is a class that provides automatic analysis on custom
    hashtag searches.
    Twitter API is used for searching the tweets, storing it in a DataFrame.
    """

    def __init__(self, lang=None, tweets_df=None):

        # Possible languages
        all_langs = ['pt', 'en']

        if lang not in all_langs and lang is not None:
            raise KeyError(f'Language not avaible. Choose between: {all_langs}')

        self.lang = lang

        self.tweets_df = tweets_df

    def search(self, token, hashtag, n_tweets = 10, n_days = 1):
        """Function that uses Twitter API to searching the desired hashtag
        and organizes the data into a DataFrame.

        Args:
            hashtag (str): hashtag to be searched.
            n_tweets (int): amount of tweets to be searched.
            n_days (int): amount of days, decreasing from the current day, to
            gather tweets. Max of 6 days.

        Returns:
            DataFrame: tweets and their respective data - location,
            if it's from a verified user, date, name of the user.
        """

        # Validating token
        client = ty.Client(bearer_token=token)

        # Defining search
        if self.lang is None:
            search = f"#{hashtag}"

        else:
            search = f"#{hashtag} lang:{self.lang}"

        # Time period
        start_period = datetime.today() - timedelta(days = n_days)
        end_period = datetime.today()

        # Executing API search
        tweets = client.search_recent_tweets(
            query = search,
            start_time = start_period,
            end_time = end_period,
            tweet_fields = [
                "created_at", "text", "lang",
                "possibly_sensitive", "source", "public_metrics"
            ],
            user_fields = [
                "username", "location", "verified",  "description"
            ],
            max_results = n_tweets,
            expansions = 'author_id'
            )

        # Generating dataframe for tweet info
        tweet_info_twt = []

        for tweet, user in zip(tweets.data, tweets.includes['users']):
            tweet_info = {
                
                # Tweets info
                'created_at':tweet.created_at,
                'text':tweet.text,
                'lang':tweet.lang,
                'possibly_sensitive':tweet.possibly_sensitive,
                'source':tweet.source,
                'likes':tweet.public_metrics['like_count'],
                'retweets':tweet.public_metrics['retweet_count'],

                # User info
                'username':user.username,
                'location':user.location,
                'verified':user.verified,
                'description':user.description              
            }
            tweet_info_twt.append(tweet_info)
        tweets_df = pd.DataFrame(tweet_info_twt)
        tweets_df['created_at'] = tweets_df['created_at'].dt.date

        self.tweets_df = tweets_df

        return tweets_df

    def analyze(self):

        """Function that displays analyses from the gathered tweets,
        and also store all the information in dataframes.

        Returns:
            Dataframe: Top used words, tweets from verified users,
            quantity of tweets by location, quantity of tweets by user.
        """

        tweets_analyze = self.tweets_df

        # Splitting all the words from the gathered tweets
        all_text = tweets_analyze['text'].str.split(' ').explode()
        all_text = pd.DataFrame({'words':all_text})

        # Normalising text
        all_text['words'] = all_text['words'].apply(unidecode)
        all_text['words'] = all_text['words'].str.replace('\W', '', regex=True)
        all_text['words'] = all_text['words'].str.upper()

        # Removing stopwords based on the language selected
        if self.lang == 'pt':
            stopwords_pt = pd.read_csv(r'stopwords_pt.csv')
            all_text_filtered = all_text[~all_text['words'].isin(stopwords_pt['stopwords'])]

        else:
            stopwords_en = pd.read_csv(r'stopwords_en.csv')
            all_text_filtered = all_text[~all_text['words'].isin(stopwords_en['stopwords'])]

        # Removing leading and trailling spaces
        all_text_filtered['words'] = all_text_filtered['words'].str.strip()

        # Filtering blank a "RT" words
        all_text_filtered = all_text_filtered[~all_text_filtered['words'].isin(['', 'RT'])]
        all_text_filtered.reset_index(inplace=True, drop=True)

        # Counting and ordering by number of repetitions
        qtd = all_text_filtered.groupby('words')['words'].count()
        qtd = qtd.reset_index(name='Qtd.').sort_values('Qtd.', ascending=False)

        pd.set_option('display.colheader_justify','center')

        # Top 10 words used
        top_10 = qtd.iloc[0:10]

        top_10_fig = px.bar(top_10, x='words', y='Qtd.')
        print('Top 10 words used:')
        top_10_fig.show()

        # Tweets by location
        location = tweets_analyze.groupby('location')['location'].count()
        location = location.reset_index(name='Qtd.').sort_values('Qtd.', ascending = False)

        location_fig = px.bar(location, x='location', y='Qtd.')
        print('\nTweets by location:')
        location_fig.show()

        # Verified comments
        verified = tweets_analyze[tweets_analyze['verified'] == True]
        verified = verified[['username', 'text']]
        print('\nVerified comments:')
        display(verified)

        # Tweets by username
        by_user = tweets_analyze.groupby('username')['username'].count()
        by_user = by_user.reset_index(name='Qtd.').sort_values('Qtd.', ascending = False)
        print('\nTweets by username:')
        display(by_user)

        # Most liked tweet
        most_liked = tweets_analyze.copy()[tweets_analyze['likes']==tweets_analyze['likes'].max()]
        most_liked = most_liked[['text', 'username', 'likes', 'retweets']]
        print('\nMost liked:')
        display(most_liked)

        # Most retweets
        most_retweets = tweets_analyze.copy()[tweets_analyze['retweets']==tweets_analyze['retweets'].max()]
        most_retweets = most_retweets[['text', 'username', 'likes', 'retweets']]
        print('\nMost retweeted:')
        display(most_liked)

        return top_10, verified, location, by_user, most_liked


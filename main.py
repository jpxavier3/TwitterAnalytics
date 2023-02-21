"""TwitterAnalytics, for automatic analysis on custom twitter's hashtags"""

from datetime import datetime, timedelta
import yaml
import pandas as pd
import tweepy as ty
from unidecode import unidecode


class TwitterAnalytics:
    """
    TwitterAnalytics is a class that provides automatic analysis on custom
    hashtag searches.
    Twitter API is used for searching the tweets, storing it in a DataFrame.
    """

    def __init__(self):

        # Token
        # Add your API token in the token variable
        token = yaml.safe_load(open(r'config/config.yml'))['user']['token']
        self.client = ty.Client(bearer_token=token)

        # Time parameters
        self.today = datetime.today()

        # Possible languages
        self.all_langs = ['pt', 'en']

        # Stopwords to be removed
        self.stopwords_en = pd.read_csv(r'stopwords_en.csv')
        self.stopwords_pt = pd.read_csv(r'stopwords_pt.csv')

        self.tweet_df = pd.DataFrame()

    def search(self, hashtag, n_tweets = 10, n_days = 1, lang = None):
        """Function that uses Twitter API to searching the desired hashtag
        and organizes the data into a DataFrame.

        Args:
            hashtag (str): hashtag to be searched.
            n_tweets (int): amount of tweets to be searched.
            lang (str): language of the tweets.
            n_days (int): amount of days, decreasing from the current day, to
            gather tweets. Max of 6 days.

        Raises:
            KeyError: in case of the language selected not being avaible.

        Returns:
            DataFrame: tweets and their respective data - location,
            if it's from a verified user, date, name of the user.
        """

        if lang is None:
            search = f"#{hashtag}"

        else:
            # Input languages
            if lang not in self.all_langs:
                raise KeyError(f'Language not avaible. Choose between: {self.all_langs}')

            # Defining search
            search = f"#{hashtag} lang:{lang}"

        # Time period
        start_period = self.today - timedelta(days = n_days)
        end_period = self.today

        # Executing API search
        tweets = self.client.search_recent_tweets(
            query = search,
            start_time = start_period,
            end_time = end_period,
            tweet_fields = [
                "created_at", "text", "lang",
                "possibly_sensitive", "source"
                ],
            user_fields = [
                "username", "location", "verified",  "description"
                ],
            max_results = n_tweets,
            expansions = 'author_id'
            )

        # Generating dataframe
        tweet_info_ls = []

        for tweet, user in zip(tweets.data, tweets.includes['users']):
            tweet_info = {
                'created_at':tweet.created_at,
                'text':tweet.text,
                'lang':tweet.lang,
                'possibly_sensitive':tweet.possibly_sensitive,
                'source':tweet.source,
                'username':user.username,
                'location':user.location,
                'verified':user.verified,
                'description':user.description
                }
            tweet_info_ls.append(tweet_info)

        tweets_df = pd.DataFrame(tweet_info_ls)
        tweets_df['created_at'] = tweets_df['created_at'].dt.date

        self.tweet_df = tweets_df

        return tweets_df

    def analyze(self, df=None):

        if df is None:
            df = self.tweet_df

        all_text = df['text'].str.split(' ').explode()
        all_text = pd.DataFrame({'words':all_text})

        # Normalising text
        all_text['words'] = all_text['words'].apply(unidecode)
        all_text['words'] = all_text['words'].str.replace('\W', '', regex=True)
        all_text['words'] = all_text['words'].str.upper()

        # TODO conditional for language selected
        all_text_filtered = all_text[~all_text['words'].isin(self.stopwords_en['stopwords'])]
        all_text_filtered = all_text_filtered[~all_text_filtered['words'].isin(self.stopwords_pt['stopwords'])]

        all_text_filtered['words'] = all_text_filtered['words'].str.strip()
        all_text_filtered = all_text_filtered[all_text_filtered['words']!='']

        all_text_filtered.reset_index(inplace=True, drop=True)

        # TODO show tables as plotly
        # Couting and ordering
        qtd = all_text_filtered.groupby('words')['words'].count()
        qtd = qtd.reset_index(name='Qtd.').sort_values('Qtd.', ascending=False)

        # Top 10 words
        top_10 = qtd.iloc[0:10]

        # Verified comments
        certified = df[df['verified']==True]

        # Tweets by location
        location = df.groupby('location')['location'].count()
        location = location.reset_index(name='Qtd.').sort_values('Qtd.', ascending = False)

        # Tweets by username
        by_user = df.groupby('username')['username'].count()
        by_user = by_user.reset_index(name='Qtd.').sort_values('Qtd.', ascending = False)

        return top_10, certified, location, by_user

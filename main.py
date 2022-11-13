"""
TwitterAnalytics, for automatic analysis on custom twitter's hashtags
"""

from datetime import datetime, timedelta
import yaml
import pandas as pd
import tweepy as ty

class TwitterAnalytics:
    """
    TwitterAnalytics is a class that provides automatic analysis on custom
    hashtag searches.
    Twitter API is used for searching the tweets, storing it in a DataFrame.
    """

    def __init__(self):

        # Token
        # Add your API token in the token variable
        config = yaml.safe_load(open(r'config/config.yml'))
        token = config['user']['token']
        self.client = ty.Client(bearer_token=token)

        # Time parameters
        self.today = datetime.today()

        # Possible languages
        self.all_langs = ['pt', 'en']

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

        if not lang is None:

            # Input languages
            if lang not in self.all_langs:
                raise KeyError(f'Language not avaible. Choose between: {self.all_langs}')

            # Defining search
            search = f"#{hashtag} lang:{lang}"

        else:
            search = f"#{hashtag}" #-is:retweet"

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

        return tweets_df

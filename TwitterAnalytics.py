"""TwitterAnalytics, for automatic analysis on custom twitter's hashtags"""

import warnings
import re
from googletrans import Translator
from textblob import TextBlob
from datetime import datetime, timedelta
from unidecode import unidecode
import pandas as pd
import tweepy as ty
import plotly.express as px
from IPython.display import display

warnings.filterwarnings('ignore')


def sentiment_polarity(txt):
    """Function that takes text and applies textblob's sentiment analysis

    Args:
        txt (string): text to by analysed

    Returns:
        float: range [-1.0, 1.0] representing the sentiment analysis
    """

    blob = TextBlob(txt)
    sentiment = blob.sentiment.polarity

    return sentiment

def translate(txt, lang):
    """Function to translate text to english

    Args:
        txt (string): text to be translated
        lang (_type_): language from the text to be translated

    Returns:
        string: text translated to english
    """

    translator = Translator()
    translated_text = translator.translate(text=txt, src=lang, dest='en')

    return translated_text.text

def cleaning_txt(txt):
    """Function to normalize text for further analysis. It removes accentuation,
    special characters, links, retweets info generated automatically
    by Twitter, and converts text to upper case.

    Args:
        txt (string): text to be normalized

    Returns:
        string: normalized text
    """

    txt = txt.upper()

    # Removing retweet info
    if all(ext in txt for ext in ['RT', ':']):
        pos1 = re.search('RT', txt).span()[0]
        pos2 = re.search(':', txt).span()[0]

        txt = txt.replace(txt[pos1:(pos2+1)], '')

    # Removing links
    txt = re.sub('HTTP://\S+|HTTPS://\S+', '', txt)

    txt = unidecode(txt)
    txt = txt.replace('\W', '')
    txt = txt.replace('#', '')
    txt = txt.replace('\n', '')

    return txt

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

        return self.tweets_df

    def analyze(self):

        """Function that displays analyses from the gathered tweets,
        and also store all the information in dataframes.

        Returns:
            Dataframe: Top used words, tweets from verified users,
            quantity of tweets by location, quantity of tweets by user.
        """

        tweets_analyze = self.tweets_df[['text', 'username', 'location', 'likes', 'retweets', 'verified']]

        # Splitting all the words from the gathered tweets
        all_text = tweets_analyze['text'].str.split(' ').explode()
        all_text = pd.DataFrame({'words':all_text})

        # Normalising text
        all_text['words_cleaned'] = all_text['words'].apply(cleaning_txt)

        # Removing stopwords based on the language selected
        if self.lang == 'pt':
            stopwords_pt = pd.read_csv(r'Stopwords/stopwords_pt.csv')
            all_text_filtered = all_text[~all_text['words_cleaned'].isin(stopwords_pt['stopwords'])]

        else:
            stopwords_en = pd.read_csv(r'Stopwords/stopwords_en.csv')
            all_text_filtered = all_text[~all_text['words_cleaned'].isin(stopwords_en['stopwords'])]

        # Removing leading and trailling spaces
        all_text_filtered['words_cleaned'] = all_text_filtered['words_cleaned'].str.strip()

        # Filtering blank a "RT" words
        all_text_filtered = all_text_filtered[~all_text_filtered['words_cleaned'].isin(['', 'RT'])]
        all_text_filtered.reset_index(inplace=True, drop=True)

        # Counting and ordering by number of repetitions
        qtd = all_text_filtered.groupby('words_cleaned')['words_cleaned'].count()
        qtd = qtd.reset_index(name='Qtd.').sort_values('Qtd.', ascending=False)

        pd.set_option('display.colheader_justify','center')

        # Top 10 words used
        top_10 = qtd.iloc[0:10]

        top_10_fig = px.bar(top_10, x='words_cleaned', y='Qtd.')
        print('Top 10 words used:')
        top_10_fig.show()

        # Tweets by location
        location = tweets_analyze[tweets_analyze['location'].notnull()]
        location['location'] = location['location'].apply(cleaning_txt)
        location = location.groupby('location')['location'].count()
        location = location.reset_index(name='Qtd.').sort_values('Qtd.', ascending = False)

        location_fig = px.bar(location, x='location', y='Qtd.')
        print('\nTweets by location:')
        location_fig.show()

        # Verified comments
        verified = tweets_analyze[tweets_analyze['verified'] == True]
        print('\nVerified comments:')
        display(verified)

        # Most liked tweet
        most_liked = tweets_analyze[tweets_analyze['likes']==tweets_analyze['likes'].max()]
        most_liked = most_liked.iloc[0:1]
        print('\nMost liked:')
        display(most_liked)

        # Most retweets
        most_retweets = tweets_analyze[tweets_analyze['retweets']==tweets_analyze['retweets'].max()]
        most_retweets = most_retweets.iloc[0:1]
        print('\nMost retweeted:')
        display(most_retweets)

        return top_10, location, verified, most_liked, most_retweets

    def sentiment_analysis(self):
        """Function for returning sentiment analysis on the searched tweets

        Returns:
            DataFrame: adds a column with the sentiment analysis by tweet
            with the range [-1.0, 1.0]
        """

        # Normalising Text
        sentiment_df = self.tweets_df[['text', 'username']]
        sentiment_df['text_sentiment'] = sentiment_df['text'].apply(cleaning_txt)

        # Translating to english
        if self.lang != 'en':
            sentiment_df['text_sentiment'] = sentiment_df['text_sentiment'].apply(
                lambda x: translate(txt=x, lang=self.lang))
        else:
            sentiment_df['text_sentiment'] = sentiment_df['text_sentiment']

        # Creating columns with sentiment analysis
        sentiment = []
        for i, row in sentiment_df.iterrows():
            polarity = sentiment_polarity(row.text_sentiment)
            sentiment.append(polarity)
        sentiment_df['SENTIMENT'] = sentiment

        # Displaying results
        most_positive = sentiment_df.loc[sentiment_df['SENTIMENT']==sentiment_df['SENTIMENT'].max()]
        most_positive = most_positive.iloc[0:1]
        print("\nMost positive tweet:")
        display(most_positive)

        most_negative = sentiment_df.loc[sentiment_df['SENTIMENT']==sentiment_df['SENTIMENT'].min()]
        most_negative = most_negative.iloc[0:1]
        print("\nMost negative tweet:")
        display(most_negative)

        return sentiment_df

import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from collections import Counter
from glob import glob

import sys
sys.path.append('..')

from utilities import *
from tweetRetweetData import *


class TestTweetRetweetStats(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        print('setting up TestTweetRetweetStats')
        path = 'tests/unit/fixtures/lambda_func/'
        cls.path = path
        df_sentiments1 = pd.read_csv(glob(path + 'sentiments/*')[0])
        df_emotions1 = pd.read_csv(glob(path + 'emotions/*')[0])
        df_original1 = pd.read_json(glob(path + 'original/*')[0], orient='records', lines=True)
        df_retweet1 = pd.read_json(glob(path + 'retweet/*')[0], orient='records', lines=True)
        df_words1 = pd.read_json(glob(path + 'words/*')[0], orient='records', lines=True)
        base_timestamp = pd.Timestamp('2020-08-24-22')
        cls.stat_sentiments1 = calc_stat_sentiments(df_sentiments1)
        cls.stat_emotions1 = calc_stat_emotions(df_emotions1)
        tweet_retweet_data = TweetRetweetData(df_original1, df_retweet1, df_words1, now=base_timestamp)
        cls.stat_words1 = tweet_retweet_data.stat_words
        cls.top_tweets1 = tweet_retweet_data.top_tweets
        cls.top_users1 = tweet_retweet_data.top_users

    def test_stat_sentiments(self):
        expected = pd.read_csv(self.path + 'validation_objects/' + 'stat_sentiments1.csv')
        assert_frame_equal(self.stat_sentiments1, expected)

    def test_stat_sentiments(self):
        expected = pd.read_csv(self.path + 'validation_objects/' + 'stat_emotions1.csv')
        assert_frame_equal(self.stat_emotions1, expected)

    def test_stat_words(self):
        expected = pd.read_json(self.path + 'validation_objects/' + 'stat_words1.json', orient='records',lines=True)
        self.assertTrue(
            all(list(self.stat_words1.columns == expected.columns) + \
                [len(self.stat_words1) == len(expected)]))

    def test_top_tweets(self):
        expected = pd.read_json(self.path + 'validation_objects/' + 'top_tweets1.json', orient='records',lines=True)
        expected['RT_id'] = expected['RT_id'].astype(str)
        self.top_tweets1['followers_count'] = self.top_tweets1['followers_count'].astype(int)
        self.top_tweets1['retweet_timespan'] = self.top_tweets1['retweet_timespan'].astype(int)
        self.top_tweets1['retweet_total'] = self.top_tweets1['retweet_total'].astype(int)
        assert_frame_equal(self.top_tweets1, expected)

    def test_top_users(self):
        expected = pd.read_json(self.path + 'validation_objects/' + 'top_users1.json', orient='records',lines=True)
        expected['user_id'] = expected['user_id'].astype(str)
        expected['RT_id'] = expected['RT_id'].astype(str)
        self.top_users1['followers_count'] = self.top_users1['followers_count'].astype(int)
        self.top_users1['following_count'] = self.top_users1['following_count'].astype(int)
        self.top_users1['retweeted'] = self.top_users1['retweeted'].astype(int)
        assert_frame_equal(self.top_users1, expected)



if __name__=='__main__':
    unittest.main()



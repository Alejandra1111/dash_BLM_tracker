import unittest
import pandas as pd 
from glob import glob
import os
import shutil

import sys
sys.path.append('..')

from pandas.testing import assert_frame_equal

from process_new_data import *
from globals import read_json_args

path = 'tests/unit/'

# note on: test validation objects "2020_all_retweets.json" and other retweet datasets are 
#   initialized cumulative data plus the new data. 
#   For this testing, new data are assumed to be processed at '2020-07-08 23:59'. 


class TestCityDateData(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls):
        print('preparing for TestProcessNewData...')
        cls.data_dest = f'{path}fixtures/'
        copy_log_files(cls.data_dest)
        copy_retweet_files(cls.data_dest)
        remove_processed_data(cls.data_dest, [pd.to_datetime('2020-07-08 23:59')])

        process_new_data()

    def test_retweet_file_all_retweets(self):
        filename = f'{self.data_dest}data_cumulative/retweet/2020_all_retweets.json'
        result = pd.read_json(filename, **read_json_args)
        filenameV = f'{path}fixtures/validation_objects/2020_all_retweets.json'
        expected = pd.read_json(filenameV, **read_json_args)
        assert_frame_equal(result, expected)   

    def test_retweet_file_all_words(self):
        filename = f'{self.data_dest}data_cumulative/retweet/2020_all_words.json'
        result = pd.read_json(filename, **read_json_args)
        filenameV = f'{path}fixtures/validation_objects/2020_all_words.json'
        expected = pd.read_json(filenameV, **read_json_args)
        assert_frame_equal(result, expected)  

    def test_retweet_file_all_sentiments(self):
        filename = f'{self.data_dest}data_cumulative/retweet/2020_all_sentiments.csv'
        result = pd.read_csv(filename)
        filenameV = f'{path}fixtures/validation_objects/2020_all_sentiments.csv'
        expected = pd.read_csv(filenameV)
        assert_frame_equal(result, expected)  

    def test_retweet_file_all_emotions(self):
        filename = f'{self.data_dest}data_cumulative/retweet/2020_all_emotions.csv'
        result = pd.read_csv(filename)
        filenameV = f'{path}fixtures/validation_objects/2020_all_emotions.csv'
        expected = pd.read_csv(filenameV)
        assert_frame_equal(result, expected)

    def test_original(self):
        filename = f'{self.data_dest}data_cumulative/original/created_at_2020-07-08_23:59:00.json'
        result = pd.read_json(filename, **read_json_args)
        filenameV = f'{path}fixtures/validation_objects/original_created_at_2020-07-08_23:59:00.json'
        expected = pd.read_json(filenameV, **read_json_args)
        assert_frame_equal(result, expected) 

    def test_words(self):
        filename = f'{self.data_dest}data_cumulative/words/created_at_2020-07-08_23:59:00.json'
        result = pd.read_json(filename, **read_json_args)
        filenameV = f'{path}fixtures/validation_objects/words_created_at_2020-07-08_23:59:00.json'
        expected = pd.read_json(filenameV, **read_json_args)
        assert_frame_equal(result, expected) 

    def test_sentiments(self):
        filename = f'{self.data_dest}data_cumulative/sentiments/created_at_2020-07-08_23:59:00.csv'
        result = pd.read_csv(filename)
        filenameV = f'{path}fixtures/validation_objects/sentiments_created_at_2020-07-08_23:59:00.csv'
        expected = pd.read_csv(filenameV)
        assert_frame_equal(result, expected) 

    def test_emotions(self):
        filename = f'{self.data_dest}data_cumulative/emotions/created_at_2020-07-08_23:59:00.csv'
        result = pd.read_csv(filename)
        filenameV = f'{path}fixtures/validation_objects/emotions_created_at_2020-07-08_23:59:00.csv'
        expected = pd.read_csv(filenameV)
        assert_frame_equal(result, expected) 



def copy_log_files(data_dest):
    path = f'{data_dest}data_filenames/'
    shutil.copyfile(
        f'{path}files_read_BLM_tweet_original_init.csv', 
        f'{path}files_read_BLM_tweet_original.csv')
    shutil.copyfile(
        f'{path}files_read_BLM_tweet_retweet_init.csv', 
        f'{path}files_read_BLM_tweet_retweet.csv') 

def copy_retweet_files(data_dest):
    path = f'{data_dest}data_cumulative/retweet/'
    shutil.copyfile(
        f'{path}2020_all_retweets_init.json', f'{path}2020_all_retweets.json')
    shutil.copyfile(
        f'{path}2020_all_words_init.json', f'{path}2020_all_words.json')
    shutil.copyfile(
        f'{path}2020_all_sentiments_init.csv', f'{path}2020_all_sentiments.csv')
    shutil.copyfile(
        f'{path}2020_all_emotions_init.csv', f'{path}2020_all_emotions.csv')
    print('copied initializing retweet data.')

def remove_processed_data(data_dest, datetimes):
    for datetime in datetimes:
        parent_dir = f'{data_dest}data_cumulative/'
        datetime_str = str(datetime).replace(' ', '_')
        folders = ['original', 'words', 'sentiments', 'emotions']
        files = [f'created_at_{datetime_str}.json']*2 + [f'created_at_{datetime_str}.csv']*2

        for folder, file in zip(folders, files):
            filepath = f'{parent_dir}{folder}/{file}'
            try:
                os.remove(filepath)
                print('deleted:',filepath)
            except:
                pass


if __name__=='__main__':
    unittest.main()


import unittest
import pandas as pd 
from glob import glob
import os
import shutil

import sys
sys.path.append('..')

from pandas.testing import assert_frame_equal

from process_city_date_data import *
from globals import cities, cities_all

path = 'tests/unit/'

# note on: test validation objects "Minneapolis_.." 
#   are generated with '2020-07-08 23:59'.
#   Globals used are days_to_keep = 2, days_to_process = 2,  stat_days_short = 7, stat_days_long = 14
#   for which changes can cause test errors.

class TestCityDateData(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls):
        cls.data_dest = f'{path}fixtures/'
        cls.processing_date = '2020-07-08'
        print('preparing for TestCityDateData...')
        remove_city_data_files(cls.data_dest, ['2020-07-07', '2020-07-08'])
        copy_initial_retweet_file(cls.data_dest)
        copy_log_file(cls.data_dest)

        process_city_date_data()

    def test_retweet_data(self):
        filename = f'{self.data_dest}data_cumulative/city_date/Minneapolis/retweet/2020_all_retweets.json'
        result = pd.read_json(filename, orient='records', lines=True)
        filenameV = f'{path}fixtures/validation_objects/Minneapolis_2020_all_retweets.json'
        expected = pd.read_json(filenameV, orient='records', lines=True)
        assert_frame_equal(result, expected)

    def test_original_data(self):
        filename = f'{self.data_dest}data_cumulative/city_date/Minneapolis/original/records_{self.processing_date}.json'
        result = pd.read_json(filename, orient='records', lines=True)
        filenameV = f'{path}fixtures/validation_objects/Minneapolis_original_records_2020-07-08.json'
        expected = pd.read_json(filenameV, orient='records', lines=True)
        assert_frame_equal(result, expected)   

    def test_words_data(self):
        filename = f'{self.data_dest}data_cumulative/city_date/Minneapolis/words/records_{self.processing_date}.json'
        result = pd.read_json(filename, orient='records', lines=True)
        filenameV = f'{path}fixtures/validation_objects/Minneapolis_words_records_2020-07-08.json'
        expected = pd.read_json(filenameV, orient='records', lines=True)
        assert_frame_equal(result, expected)   

    def test_sentiments_data(self):
        filename = f'{self.data_dest}data_cumulative/city_date/Minneapolis/sentiments/records_{self.processing_date}.csv'
        result = pd.read_csv(filename)
        filenameV = f'{path}fixtures/validation_objects/Minneapolis_sentiments_records_2020-07-08.csv'
        expected = pd.read_csv(filenameV)
        assert_frame_equal(result, expected)   

    def test_emotions_data(self):
        filename = f'{self.data_dest}data_cumulative/city_date/Minneapolis/emotions/records_{self.processing_date}.csv'
        result = pd.read_csv(filename)
        filenameV = f'{path}fixtures/validation_objects/Minneapolis_emotions_records_2020-07-08.csv'
        expected = pd.read_csv(filenameV)
        assert_frame_equal(result, expected)   



def remove_city_data_files(data_dest, dates):
    for city in cities + cities_all :
        for date in dates:
            parent_dir = f'{data_dest}data_cumulative/city_date/{city}/'
            folders = ['original', 'retweet', 'sentiments', 'emotions', 'words']
            files = [f'records_{date}.json', '2020_all_retweets.json', 
                     f'records_{date}.csv', f'records_{date}.csv', f'records_{date}.json']

            for folder, file in zip(folders, files):
                filepath = f'{parent_dir}{folder}/{file}'
                try:
                    os.remove(filepath)
                    print('deleted:',filepath)
                except:
                    pass

def copy_initial_retweet_file(data_dest):
    for city in cities + cities_all :
        path = f'{data_dest}data_cumulative/city_date/{city}/retweet/'
        path0 = f'{path}2020_all_retweets_init.json'
        path1 = f'{path}2020_all_retweets.json'
        shutil.copyfile(path0, path1)
    
def copy_log_file(data_dest):
    path = f'{data_dest}data_filenames/'
    shutil.copyfile(f'{path}files_read_original_init.csv', f'{path}files_read_original.csv')
    shutil.copyfile(f'{path}files_read_sentiments_init.csv', f'{path}files_read_sentiments.csv')
    shutil.copyfile(f'{path}files_read_emotions_init.csv', f'{path}files_read_emotions.csv')
    shutil.copyfile(f'{path}files_read_words_init.csv', f'{path}files_read_words.csv')


if __name__=='__main__':
    unittest.main()


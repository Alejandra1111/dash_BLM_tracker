import unittest
import pandas as pd 
from glob import glob
import os
import shutil

import sys
sys.path.append('..')

from pandas.testing import assert_frame_equal

from process_city_date_stats import *
from logfile_utils import *
from globals import cities, cities_all

path = 'tests/unit/'

# note on: test validation objects "Minneapolis_stats_2020-07-08_v1.json", "Minneapolis_stats_2020-07-08_v2.json"
#   are generated with '2020-07-08 8:59' for v1 and '2020-07-08 20:59') for v2 (which was run after v1)
#   with globals days_to_keep = 2, days_to_process = 2,  stat_days_short = 7, stat_days_long = 14
#   changes in those parameters can cause test errors.

class TestCityDateStats(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls):
        cls.data_dest = f'{path}fixtures/'
        print('preparing for TestCityDateStats...')
        clean_up_stat_files(cls.data_dest)
        copy_initial_logfile(cls.data_dest)

        cls.current_time = pd.to_datetime('2020-07-08 08:59')
        cls.current_time_str = str(cls.current_time)
        cls.stats_datetime_candidates = get_same_day_hours_up_to_timestamp(cls.current_time)
        logfile_stats = LogFileUtilityCSV(f'{cls.data_dest}data_processing_log/', 'log_stats.csv', varname='datetime')     
        logfile_stats.add_candidate_records(cls.stats_datetime_candidates)
        logfile_stats.read_existing_records()
        logfile_stats.get_new_records()
        cls.logfile_stats = logfile_stats
        cls.process_hours = get_hour_value_from_timestamps(logfile_stats.new_records)
        
        data_dest_cd = cls.data_dest + "data_cumulative/city_date/"
        for city in cities + cities_all:
            create_or_update_city_date_stats(city, data_dest_cd, cls.current_time, cls.process_hours, verbose=False)
        cls.logfile_stats.append_new_records(f'{cls.data_dest}data_processing_log/log_stats.csv')

        # read the file just created 
        filename = f'{path}fixtures/data_cumulative/city_date/Minneapolis/stats/stats_2020-07-08.json'
        cls.stat_sentiments, cls.stat_emotions, cls.stat_words, cls.top_tweets, cls.top_users = read_stat_file(filename)

        # read a validating object
        filenameV = f'{path}fixtures/validation_objects/Minneapolis_stats_2020-07-08_v1.json'
        cls.stat_sentimentsV, cls.stat_emotionsV, cls.stat_wordsV, cls.top_tweetsV, cls.top_usersV = read_stat_file(filenameV)


    def test_get_same_day_hours_up_to_timestamp(self):
        expected = [f'2020-07-08 0{str(h)}:00:00' for h in range(9)]
        self.assertEqual(self.stats_datetime_candidates, expected)

    def test_logfile_stats_new_records(self):
        expected = [f'2020-07-08 0{str(h)}:00:00' for h in range(9)]
        self.assertEqual(self.logfile_stats.new_records, expected)

    def test_stat_sentiments(self):
        assert_frame_equal(self.stat_sentiments, self.stat_sentimentsV)

    def test_stat_emotions(self):
        assert_frame_equal(self.stat_emotions, self.stat_emotionsV)

    # def test_stat_words(self):
    #     assert_frame_equal(self.stat_words, self.stat_wordsV)

    def test_top_tweets(self):
        assert_frame_equal(self.top_tweets, self.top_tweetsV)

    def test_top_users(self):
        assert_frame_equal(self.top_users, self.top_usersV)



class TestCityDateStats2(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls):
        cls.data_dest = f'{path}fixtures/'
        cls.current_time = pd.to_datetime('2020-07-08 20:59') # second time run at later time 
        cls.current_time_str = str(cls.current_time)
        cls.stats_datetime_candidates = get_same_day_hours_up_to_timestamp(cls.current_time)
        logfile_stats = LogFileUtilityCSV(f'{cls.data_dest}data_processing_log/', 'log_stats.csv', varname='datetime')     
        logfile_stats.add_candidate_records(cls.stats_datetime_candidates)
        logfile_stats.read_existing_records()
        logfile_stats.get_new_records()
        cls.logfile_stats = logfile_stats
        cls.process_hours = get_hour_value_from_timestamps(logfile_stats.new_records)
        
        data_dest_cd = cls.data_dest + "data_cumulative/city_date/"
        for city in cities + cities_all:
            create_or_update_city_date_stats(city, data_dest_cd, cls.current_time, cls.process_hours, verbose=False)
        cls.logfile_stats.append_new_records(f'{cls.data_dest}data_processing_log/log_stats.csv')

        # read the file just created 
        filename = f'{path}fixtures/data_cumulative/city_date/Minneapolis/stats/stats_2020-07-08.json'
        cls.stat_sentiments, cls.stat_emotions, cls.stat_words, cls.top_tweets, cls.top_users = read_stat_file(filename)

        # read a validating object
        filenameV = f'{path}fixtures/validation_objects/Minneapolis_stats_2020-07-08_v2.json' # version 2
        cls.stat_sentimentsV, cls.stat_emotionsV, cls.stat_wordsV, cls.top_tweetsV, cls.top_usersV = read_stat_file(filenameV)


    def test_get_same_day_hours_up_to_timestamp(self):
        expected = [f'2020-07-08 0{str(h)}:00:00' for h in range(10)] + [f'2020-07-08 {str(h)}:00:00' for h in range(10,21)]
        self.assertEqual(self.stats_datetime_candidates, expected)

    def test_logfile_stats_new_records(self):
        expected = [f'2020-07-08 0{str(h)}:00:00' for h in range(9,10)] + [f'2020-07-08 {str(h)}:00:00' for h in range(10,21)]
        self.assertEqual(sorted(self.logfile_stats.new_records), sorted(expected))

    def test_stat_sentiments(self):
        assert_frame_equal(self.stat_sentiments, self.stat_sentimentsV)

    def test_stat_emotions(self):
        assert_frame_equal(self.stat_emotions, self.stat_emotionsV)

    # def test_stat_words(self):
    #     assert_frame_equal(self.stat_words, self.stat_wordsV)

    def test_top_tweets(self):
        assert_frame_equal(self.top_tweets, self.top_tweetsV)

    def test_top_users(self):
        assert_frame_equal(self.top_users, self.top_usersV)




def read_stat_file(filename):
    with open(filename) as f:
        stats = json.load(f) 
    return [pd.read_json(stats[stat], **json_split) 
            for stat in ['stat_sentiments', 'stat_emotions', 'stat_words', 'top_tweets', 'top_users']]
      
def clean_up_stat_files(data_dest):
    data_dest_cd = data_dest + "data_cumulative/city_date/"
    for city in cities + cities_all:
        for date in ['2020-07-08']:
            city_date_stats_path = f'{data_dest_cd}{city}/stats/'
            filename = f'{city_date_stats_path}stats_{date}.json'
            try: 
                os.remove(filename)
                print('deleted:', filename) 
            except:
                pass


def copy_initial_logfile(data_dest):
    path0 = f'{data_dest}data_processing_log/log_stats_init.csv'
    path1 = f'{data_dest}data_processing_log/log_stats.csv'
    try:
        shutil.copyfile(path0, path1)
    except:
        pass


if __name__=='__main__':
    unittest.main()



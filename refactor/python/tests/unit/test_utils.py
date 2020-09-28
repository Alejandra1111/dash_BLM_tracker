import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

import sys
sys.path.append('..')

from utilities  import *



class TestUtilities(unittest.TestCase):

    def test_str_between_with_string(self):
        str1 = 'this_is_a_test_string'
        self.assertEqual(str_between(str1, '_is_', '_string'), 'a_test', "should be 'a_test' ")

    def test_str_between_with_bad_request(self):
        str1 = 'this_is_a_test_string'
        with self.assertRaises(ValueError):
            attempt = str_between(str1, '_is_', 'bad')


    def test_extract_timestamp_from_filename_with_filename_string(self):
        filename1 = 'c://home/document/somefolder/target_file_2020-09-08.txt'
        self.assertEqual(
            extract_timestamp_from_filename(filename1, 'file_'), 
            pd.Timestamp('2020-09-08'), "should be '2020-09-08'")

    def test_extract_timestamp_from_filename_with_filename_string2(self):
        filename1 = 'c://home/document/somefolder/target_file_2020-09-08-xyz.txt'
        self.assertEqual(
            extract_timestamp_from_filename(filename1, 'file_', '-xyz'), 
            pd.Timestamp('2020-09-08'), "should be '2020-09-08'")

    def test_extract_timestamp_from_filename_with_filename_string_with_no_dot(self):
        filename1 = 'c://home/document/somefolder/target_file_2020-09-08-xyz_txt'
        self.assertEqual(
            extract_timestamp_from_filename(filename1, 'file_', '-xyz'), 
            pd.Timestamp('2020-09-08'), "should be '2020-09-08'")


    def test_keep_files_newer_or_within_x_days_old(self): 
        files = [ f'C://home/document/somefolder/target_file_2020-09-{num_to_str2d(day)}.txt' for day in range(1,31)]
        base_timestamp = pd.Timestamp('2020-09-25')
        result = keep_files_newer_or_within_x_days_old(
            files, base_timestamp, days = 7, 
            date_prefix = '_file_', date_suffix ='.')
        expected = [ f'C://home/document/somefolder/target_file_2020-09-{num_to_str2d(day)}.txt' for day in range(18, 31)]
        self.assertEqual(result, expected, f"should be: {expected}")


    def test_keep_files_within_x_days_old(self): 
        files = [ f'C://home/document/somefolder/target_file_2020-09-{num_to_str2d(day)}.txt' for day in range(1,31)]
        base_timestamp = pd.Timestamp('2020-09-25')
        result = keep_files_within_x_days_old(
            files, base_timestamp, days = 7, 
            date_prefix = '_file_', date_suffix ='.')
        expected = [ f'C://home/document/somefolder/target_file_2020-09-{num_to_str2d(day)}.txt' for day in range(18, 26)]
        self.assertEqual(result, expected, f"should be: {expected}")



    def test_convert_floats(self):
        df = pd.DataFrame.from_dict(
            {'x1':[1,2,3], 'x2':['a','b','c'], 'x3':[4.11,7.241,8.009], 'x4':[4.0,7.1,8.0]})
        convert_floats(df, target_dtype='float32', include=['float64'])
        df2 = pd.DataFrame.from_dict(
            {'x1':[1,2,3], 'x2':['a','b','c'], 'x3':[4.11,7.241,8.009], 'x4':[4.0,7.1,8.0]})
        df2['x3'] = df2.x3.astype('float32')
        df2['x4'] = df2.x4.astype('float32')
        self.assertTrue(all(a==b for a,b in zip(df.dtypes, df2.dtypes)), f'should be: {df2.dtypes}')


    def test_filter_df_by_id(self):
        df = pd.DataFrame.from_dict(
            {'x1':['a','b','a'], 'x2':[1,2,3]})
        result = filter_df_by_id(df, 'x1', ['a'])
        expected = pd.DataFrame.from_dict(
            {'x1':['a','a'], 'x2':[1,3]})
        assert_frame_equal(result, expected, f'should be:{expected}') 

    def test_remove_list_items_ending_with(self):
        list1 = ['app_data/data_cumulative/city_date/all_v1/original/.DS_Store',
                 'app_data/data_cumulative/city_date/all_v1/original/records_2020-05-27.json', 
                 'app_data/data_cumulative/city_date/all_v1/original/records_2020-05-30.json']
        result = remove_list_items_ending_with(list1, 'DS_Store')
        expected = list1[1:]
        self.assertEqual(result, expected, f"should be: {expected}")

    def test_convert_vars_to_numeric(self):
        df = pd.DataFrame.from_dict(
            {'x1':[1,2,3], 'x2':['a','b','c'], 'x3':['1','0','1'], 'x4':['4.0','7.1','8.0']})
        convert_vars_to_numeric(df, ['x3', 'x4']) 
        expected = pd.DataFrame.from_dict(
            {'x1':[1,2,3], 'x2':['a','b','c'], 'x3':[1, 0, 1], 'x4':[4.0,7.1,8.0]})
        assert_frame_equal(df, expected, f'should be:{expected}') 

    def test_convert_vars_to_str(self):
        df = pd.DataFrame.from_dict(
            {'x1':[1,2,3], 'x2':['a','b','c'], 'x3':[1, 0, 1], 'x4':[4.0,7.1,8.0]})
        convert_vars_to_str(df, ['x3', 'x4']) 
        expected = pd.DataFrame.from_dict(
            {'x1':[1,2,3], 'x2':['a','b','c'], 'x3':['1','0','1'], 'x4':['4.0','7.1','8.0']})
        assert_frame_equal(df, expected, f'should be:{expected}') 

    def test_drop_duplicates_index(self):
        df = pd.DataFrame.from_dict(
            {'x1':[1,2,3], 'x2':['a','b','a'], 'x3':[1, 0, 1], 'x4':[4.0,7.1,8.0]}).set_index('x2')
        result = drop_duplicates_index(df, keep='first')
        expected = pd.DataFrame.from_dict(
            {'x1':[1,2], 'x2':['a','b'], 'x3':[1, 0], 'x4':[4.0,7.1]}).set_index('x2')
        assert_frame_equal(result, expected, f'should be:{expected}') 



if __name__=='__main__':
    unittest.main()


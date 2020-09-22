import unittest
import pandas as pd 
from glob import glob
from copy import deepcopy

import sys
sys.path.append('..')

from datedFiles import *

class TestDatedFiles(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        files = [
         'app_data/data_cumulative/city_date/all_v1/stats/stats_2020-05-27.json',
         'app_data/data_cumulative/city_date/all_v1/stats/stats_2020-05-30.json',
         'app_data/data_cumulative/city_date/all_v1/stats/stats_2020-06-03.json']
        cls.datedfiles = DatedFiles(files, date_prefix='stats_')


    def test_timestamps(self):
        timestamps = [pd.Timestamp(f'2020-{m_d} 00:00:00') for m_d in ('05-27','05-30','06-03')]
        self.assertEqual(self.datedfiles.timestamps, timestamps, 'should be:' + str(timestamps))

    def test_dates(self):
        dates = ['2020-05-27', '2020-05-30', '2020-06-03']
        self.assertEqual(self.datedfiles.dates, dates, 'should be:' + str(dates))


class TestDatedDataFilesAndFilters(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        base_timestamp = pd.Timestamp('2020-08-25')
        files_original = glob('tests/unit/fixtures/lambda_func/original/*')
        files_wordindex = glob('tests/unit/fixtures/lambda_func/wordindex/*')
        cls.dateddatafiles = DatedDataFiles(files_original, id_varname = 'id', date_prefix='records_')
        cls.datedfilenamefilter = DatedFilenameFilter(base_timestamp, days=2, no_newer=True)
        cls.datedfilenamefilter_withnewer = DatedFilenameFilter(base_timestamp, days=2, no_newer=False)  
        cls.datedwordindexfilter = DatedWordindexFilter(files_wordindex, 'protest')

    def test_filekeys(self):
        dates = [f'2020-08-{d}' for d in range(20,29)]
        self.assertEqual(sorted(self.dateddatafiles.filekeys), dates, 'should be:' + str(dates))

    def test_filename_filter(self):
        filtered_files = [f'tests/unit/fixtures/lambda_func/original/records_2020-08-{d}.json' for d in range(23,26)]
        copy_dateddatafiles = deepcopy(self.dateddatafiles)
        copy_dateddatafiles.apply_file_filter(self.datedfilenamefilter)
        self.assertEqual(sorted(copy_dateddatafiles.files), filtered_files, 'should be:' + str(filtered_files))

    def test_filename_filter_with_newer(self):
        filtered_files = [f'tests/unit/fixtures/lambda_func/original/records_2020-08-{d}.json' for d in range(23,29)]
        copy_dateddatafiles = deepcopy(self.dateddatafiles)
        copy_dateddatafiles.apply_file_filter(self.datedfilenamefilter_withnewer)
        self.assertEqual(sorted(copy_dateddatafiles.files), filtered_files, 'should be:' + str(filtered_files))

    def test_get_filtered_id(self):
        result = self.datedwordindexfilter.get_filtered_ids(self.dateddatafiles)
        self.assertEqual( (len(result.get('2020-08-25')), len(result.get('2020-08-28'))), (1240, 883),
            'should be:' + str((1240, 883)))

    def test_id_filter(self):
        self.dateddatafiles.apply_id_filter(self.datedwordindexfilter)
        result = self.dateddatafiles.filtered_ids
        self.assertEqual( (len(result.get('2020-08-25',[])),len(result.get('2020-08-28',[]))), (1240, 883),
            'should be:' + str((1240, 883)))



if __name__=='__main__':
    unittest.main()


import unittest
import pandas as pd 
from glob import glob

import sys
sys.path.append('..')

from localDataAccess import *
from datedFiles import *
from dataLoaderMethods import *
from dataLoader import DataLoader

path = 'tests/unit/'


class TestDataLoaderMethods(unittest.TestCase):
        
    @classmethod
    def setUpClass(cls):
        files_original = glob(f'{path}fixtures/lambda_func/original/*')
        files_wordindex = glob(f'{path}fixtures/lambda_func/wordindex/*')
        cls.localdata = LocalDataAccess('python/tests/unit/fixtures/lambda_func/original')
        cls.data1 =  pd.read_json(files_original[0], orient='records', lines=True)
        cls.dateddatafiles = DatedDataFiles(files_original, id_varname = 'id', date_prefix='records_')
        cls.datedwordindexfilter = DatedWordindexFilter(files_wordindex, 'protest')
        cls.dateddatafiles.apply_id_filter(cls.datedwordindexfilter)
        cls.dataloaderjson = DataLoaderJson(orient='records', lines=True, float_dtype='float64')
        cls.dataloaderjson.load_data(cls.dateddatafiles, cls.localdata)
        cls.dataloadercsv = DataLoaderCSV(float_dtype='float64')

        files_sentiments = glob(f'{path}fixtures/lambda_func/sentiments/*')
        cls.localdata2 = LocalDataAccess('python/tests/unit/fixtures/lambda_func/sentiments')
        cls.data2 =  pd.read_csv(files_sentiments[0])
        cls.dateddatafiles2 = DatedDataFiles(files_sentiments, id_varname = 'id', date_prefix='records_')
        cls.dateddatafiles2.apply_id_filter(cls.datedwordindexfilter)
        cls.dataloadercsv.load_data(cls.dateddatafiles2, cls.localdata2)


    def test_DataLoaderJson_dtypes(self):
        dtypes = self.data1.dtypes
        self.assertTrue(
            all([self.dataloaderjson.df.dtypes[var] == dtypes[var] for var in self.data1.columns]),
            'data types should be:' + str(dtypes))

    def test_DataLoaderJson_len(self):
        self.assertEqual(len(self.dataloaderjson.df), 210122, 'len() should be' + str(210122))


    def test_DataLoaderCSV_dtypes(self):
        dtypes = self.data2.dtypes
        self.assertTrue(
            all([self.dataloadercsv.df.dtypes[var] == dtypes[var] for var in self.data2.columns]),
            'data types should be:' + str(dtypes))

    def test_DataLoaderCSV_len(self):
        self.assertEqual(len(self.dataloadercsv.df), 309546, 'len() should be' + str(309546))


class TestDataLoader(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        files_original = glob(f'{path}fixtures/lambda_func/original/*')
        files_wordindex = glob(f'{path}fixtures/lambda_func/wordindex/*')
        localdata = LocalDataAccess(f'{path}fixtures/lambda_func/original')
        cls.data1 =  pd.read_json(files_original[0], orient='records', lines=True)
        cls.dateddatafiles = DatedDataFiles(files_original, id_varname = 'id', date_prefix='records_')
        base_timestamp = pd.Timestamp('2020-08-25')
        cls.datedfilenamefilter = DatedFilenameFilter(base_timestamp, days=2, no_newer=True)
        cls.datedwordindexfilter = DatedWordindexFilter(files_wordindex, 'protest')
        cls.dataloader = DataLoader(localdata)
        cls.df = cls.dataloader.load(cls.dateddatafiles, cls.datedfilenamefilter, cls.datedwordindexfilter,
            orient='records', lines=True, float_dtype='float64')


    def test_DataLoader_dtypes(self):
        dtypes = self.data1.dtypes
        self.assertTrue(
            all([self.df.dtypes[var] == dtypes[var] for var in self.data1.columns]),
            'data types should be:' + str(dtypes))
    
    def test_DataLoader_files(self):
        expected = [
            f'{path}fixtures/lambda_func/original/records_2020-08-23.json',
            f'{path}fixtures/lambda_func/original/records_2020-08-24.json',
            f'{path}fixtures/lambda_func/original/records_2020-08-25.json'
        ]
        self.assertEqual(self.dateddatafiles.files, expected)

    def test_DataLoader_len(self):
        self.assertEqual(len(self.df), 94220, 'len() should be' + str(94220))




if __name__=='__main__':
    unittest.main()

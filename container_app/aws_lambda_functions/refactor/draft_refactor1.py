from glob import glob
from io import BytesIO
import boto3
import pandas as pd

path = '/Users/kotaminegishi/big_data_training/python/dash_BLM/new_data/app_data/data_cumulative/city_date/all_v1/'
file_words =  glob(path + 'words/*')


bucket_name = 'kotasstorage1'
session = boto3.Session()
s3_client = session.client("s3")
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(bucket_name)


def str_between(x, str_before, str_after):
    return x.split(str_before)[1].split(str_after)[0]

def extract_timestamp_from_filename(filename, prefix, suffix='.'):
    return pd.Timestamp(str_between(filename, prefix, suffix).replace('_',' '))

def str_date(x):
    return str(x.date())

def keep_recent_files(
    files, base_timestamp, days = 14, no_newer=False, date_prefix = 'created_at_', date_suffix ='.'):
    timestamps = [extract_timestamp_from_filename(file, date_prefix, date_suffix) for file in files ]
    keep_idx1 = [base_timestamp - timestamp <= pd.Timedelta(days, unit='d') for timestamp in timestamps]
    if no_newer: 
        keep_idx2 = [base_timestamp - timestamp >= pd.Timedelta(0, unit='d') for timestamp in timestamps]
        keep_idx1 = np.multiply(keep_idx1, keep_idx2)
    return list(itertools.compress(files, keep_idx1))


def convert_floats(df, target_dtype='float32', include=['float64']):
    floats = df.select_dtypes(include=include).columns.tolist()
    df[floats] = df[floats].astype(target_dtype)
    return df


class S3DataAccess:
    def __init__(self, bucket_name, s3_client, s3_resource):
        self.bucket_name = bucket_name
        self.s3_client = s3_client
        self.s3_resource = s3_resource
        self.bucket = s3_resource.Bucket(bucket_name)

    def get_data(self, filename):
        f = BytesIO()
        self.s3_client.download_fileobj(self.bucket_name, filename, f)
        return f.getvalue()

    def get_files(self, prefix):
        return [object.key for object in self.bucket.objects.filter(Prefix=prefix)]



class DatedFile:
    def __init__(self, date_prefix='records_', date_suffix='.'):
        self.date_prefix = date_prefix
        self.date_suffix = date_suffix

    def get_timestamps(self, files):
        return [extract_timestamp_from_filename(file, self.date_prefix, self.date_suffix) for file in files ]

    def get_dates(self):
        return [str_date(timestamp) for timestamp in self.timestamps]


class DatedDataFiles(DatedFile):
    def __init__(self, files, id, filekey='date', date_prefix='records_', date_suffix='.'):
        super().__init__(date_prefix, date_suffix)
        self.files = files
        self.id = id 
        self.file_format = files[0].split('.')[1]
        self.timestamps = get_timestamps(files)
        self.dates = get_dates()
        self.filtered_ids = None

    @property
    def filekeys(self):
        return self.dates if self.filekey=='date' else self.timestamps
    
    def apply_file_filter(self, file_filter):
        self.files = file_filter.filter_files(self.files)

    def add_filtered_ids(self, index_filter):
        self.filtered_ids = index_filter.filter_ids(self.filekeys)


class DatedFilenameFilter(DatedFile):
    def __init__(self, base_timestamp, days = 7, no_newer=True, date_prefix='records_', date_suffix='.'):
        super().__init__(date_prefix, date_suffix)
        self.base_timestamp = base_timestamp
        self.days = days
        self.no_newer = no_newer

    def filter_files(self, files):
        return keep_recent_files(
            files=files, base_timestamp=self.base_timestamp, days=self.days, 
            no_newer=self.no_newer, prefix = self.date_prefix, suffix = self.date_suffix)


class DatedWordindexFilter(DatedFile):
    def __init__(self, files_wordindex, filter_keyword, date_prefix='records_', date_suffix='.'):
        super().__init__(date_prefix, date_suffix)
        self.files_wordindex = files_wordindex
        self.filter_keyword = filter_keyword

    @property
    def filtered_ids(self):
        dict_id = {}
        for file in self.files_wordindex:
            datetime = str_between(file, self.date_prefix, self.date_suffix)
            with open(file) as f:
                data = json.load(f)
                dict_id[datetime] = data[self.filter_keyword]
        return dict_id

    def filter_ids(self, filekeys):
        return { key: value for key, value in self.filtered_ids.items() if key in filekeys }


class DataLoaderBase:
    def load_data(self, DataFiles, DataAccess, get_data, read_data):
        df = []
        for file, filekey in zip(DataFiles.files, DataFiles.filekeys):
            filtered_id = DataFiles.filtered_ids[filekey]
            data = read_data(get_data(DataAccess, file))
            if not filtered_id:
                df_filtered_id = pd.DataFrame(filtered_id).set_index(0)
                data = data.set_index(DataFiles.id).join(df_filtered_id, how='inner').reset_index()
            df.append(data)  
        self.df = pd.concat(df, ignore_index=True) 

    def check_convert_floats(self, float_dtype):
        if not float_dtype: 
            self.df = convert_floats(self.df, target_dtype=float_dtype)  
  

class DataLoaderJson(DataLoaderBase):
    def __init__(self, orient, lines=True, float_dtype='float32'):
        self.orient = orient
        self.lines = lines
        self.float_dtype = float_dtype

    def get_data(self, DataAccess, file):
        return DataAccess.get_data(file)

    def read_data(self, DataFiles, data):
        return pd.read_json(data, orient=self.orient, lines=self.lines)

    def load_data(self, DataFiles, DataAccess):
        super().load_data(DataFiles, DataAccess, get_data, read_data)
        check_convert_floats(float_dtype)


class DataLoaderCSV(DataLoaderBase):
    def __init__(self, encoding='latin-1', float_dtype='float32'):
        self.float_dtype = float_dtype
        self.encoding = encoding

    def get_data(self, DataAccess, file):
        return BytesIO(Data.Access.get_data(file))

    def read_data(self, DataFiles, data):
        return pd.read_csv(data, encoding=self.encoding)

    def load_data(self, DataFiles, DataAccess):
        super().load_data(DataFiles, DataAccess, get_data, read_data)
        check_convert_floats(float_dtype)


class Factory():
    def __init__(self):
        self._tools = {}

    def register_tool(self, key, tool):
        self._tools[key] = tool

    def get_tool(self, key):
        tool = self._tools.get(key)
        if not tool:
            raise ValueError(key)
        return creator() 

data_loader_factory = Factory()
data_loader_factory.register_tool('json', DataLoaderJson)
data_loader_factory.register_tool('csv', DataLoaderCSV)


class DataLoader():
    """ """
    def load(self, DataFiles, FileFilter, IndexFilter, DataAccess):
        data_loader = data_loader_factory.get_tool(DataFiles.file_format)
        if not FileFilter: DataFiles.apply_file_filter(FileFilter)
        if not IndexFilter: DataFiles.filter_ids(IndexFilter)
        data_loader.load_data(DataFiles, DataAccess)
        return data_loader.df


# Use case:
# s3_data_access = S3DataAccess(bucket_name, s3_client, s3_resource)
# filename_filter_7d = DatedFilenameFilter(base_timestamp, days = 7, no_newer=True, date_prefix='records_')
# filename_filter14d = DatedFilenameFilter(base_timestamp, days = 14, no_newer=True, date_prefix='records_')
# all_files_wordindex = s3_data_access.get_data('wordindex_')
# datafiles_wordindex = DatedDateFiles(all_files_wordindex, ...)
# datafiles_wordindex.apply_file_filter(dated_filename_filter_7d)
# filter_keyword = 'protest'
# wordindex_filter = DatedWordindexFilter(datafiles_wordindex, filter_keyword, date_prefix='records_')

# files_original = DatedDataFiles(...)
# files_retweet = DatedDataFiles(...)
# files_sentiments = DatedDataFiles(...)
# files_emotions =  DatedDataFiles(...)
# files_words =  DatedDataFiles(...)
# loader = DataLoader()
# data_original = loader.load(files_original, dated_filename_filter_7d, dated_wordindex_filter, s3_data_access)
# data_retweet = loader.load(files_retweet, None, dated_wordindex_filter, s3_data_access)
# data_words = loader.load(files_words, dated_filename_filter_7d, dated_wordindex_filter, s3_data_access)
# data_sentiments = loader.load(files_sentiments, dated_filename_filter_14d, dated_wordindex_filter, s3_data_access)
# data_emotions = loader.load(files_emotions, dated_filename_filter_14d, dated_wordindex_filter, s3_data_access)


# sentiments = stat_calculator(AppData('sentiments',data_sentiments)).stat








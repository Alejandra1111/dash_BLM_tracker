import json

from utilities import extract_timestamp_from_filename, str_date, \
     keep_files_newer_or_within_x_days_old, \
     keep_files_within_x_days_old, \
     str_between, remove_list_items_ending_with


class DatedFiles:  
    def __init__(self, files=None, ignore_files_ends_with='', date_prefix='records_', date_suffix='.'):
        if files is None:
            self.files = []
        else:
            if ignore_files_ends_with:
                files = remove_list_items_ending_with(files, ignore_files_ends_with)
            self.files = sorted(list(files))
        self.date_prefix = date_prefix
        self.date_suffix = date_suffix

    @property 
    def timestamps(self):
        if self.files:
            return [extract_timestamp_from_filename(file, self.date_prefix, self.date_suffix) for file in self.files ]
        else:
            raise ValueError('found no files')

    @property
    def dates(self):
        if self.files:
            return [str_date(timestamp) for timestamp in self.timestamps]
        else:
            raise ValueError('found no files')


class DatedDataFiles(DatedFiles):
    def __init__(self, files, ignore_files_ends_with='', id_varname='', filekey_unit='date', date_prefix='records_', date_suffix='.'):
        super().__init__(files, ignore_files_ends_with, date_prefix, date_suffix)
        self.id_varname = id_varname
        self.filekey_unit = filekey_unit
        self.file_format = self.files[0].split('.')[1]
        self.filtered_ids = {}

    @property
    def filekeys(self):
        if self.filekey_unit=='date': 
            return sorted(self.dates)
        else:
            raise NotImplementedError
    
    def apply_file_filter(self, file_filter):
        self.files = file_filter.filter_files(self)

    def apply_id_filter(self, index_filter, DataLoader=None, DataAccess=None):
        if not self.id_varname: raise ValueError('please set "id_varname".')
        self.filtered_ids = index_filter.filter_ids(self, DataLoader, DataAccess)


class DatedFilenameFilter:
    def __init__(self, base_timestamp, days = 7, no_newer=True):
        self.base_timestamp = base_timestamp
        self.days = days
        self.no_newer = no_newer

    def filter_files(self, DatedDataFiles):
        if self.no_newer:
            func = keep_files_within_x_days_old
        else: 
            func = keep_files_newer_or_within_x_days_old
        return func(
                files=DatedDataFiles.files, base_timestamp=self.base_timestamp, days=self.days, 
                date_prefix = DatedDataFiles.date_prefix, date_suffix = DatedDataFiles.date_suffix)



class DatedWordindexFilter:
    def __init__(self, files, filter_keyword, ignore_files_ends_with=''):
        if ignore_files_ends_with:
            files = remove_list_items_ending_with(files, ignore_files_ends_with)
        self.files = list(files)
        self.file_format = self.files[0].split('.')[1]
        self.filter_keyword = filter_keyword

    def get_filtered_ids(self, DatedDataFiles, DataLoader, DataAccess):
        dict_id = {}
        for file in self.files:
            datetime = str_between(file, DatedDataFiles.date_prefix, DatedDataFiles.date_suffix)
            try:
                with open(file) as f:
                    data = json.load(f)
            except Exception:
                data = DataLoader.load_data_from_single_file(DataAccess, file)
            except ValueError:
                print('DataAccess is missing get_data() method')
            dict_id[datetime] = data[self.filter_keyword]
        return dict_id

    def filter_ids(self, DatedDataFiles, DataLoader=None, DataAccess=None):
        filtered_ids = self.get_filtered_ids(DatedDataFiles, DataLoader, DataAccess)
        return { key: value for key, value in filtered_ids.items() if key in DatedDataFiles.filekeys }




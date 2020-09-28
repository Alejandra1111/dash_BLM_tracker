from glob import glob
from pandas import read_csv

from utilities import path_ends_with_slash, append_a_list_to_csv_file, pass_obj_and_args_to_fun, \
    keep_files_within_x_days_old


class FilenameGatherer:
    def __init__(self, new_file_location, new_file_prefix, 
                 existing_file_location, existing_filenames_file, 
                 varname='name'):
        self.new_file_location = path_ends_with_slash(new_file_location)
        self.new_file_prefix = new_file_prefix
        self.existing_file_location = path_ends_with_slash(existing_file_location)
        self.existing_filenames_file = existing_filenames_file
        self.varname=varname

    def read_all_filenames_glob(self):
        self.all_filenames = glob(self.new_file_location + self.new_file_prefix)
    
    def register_filename_reader(self, filename_reader, *args, **kwargs):
        self.filename_reader = pass_obj_and_args_to_fun(self, filename_reader, *args, **kwargs)

    def register_filename_filter(self, filename_filter, *args, **kwargs):
        self.filename_filter = pass_obj_and_args_to_fun(self, filename_filter, *args, **kwargs)
                                        
    def read_existing_filenames(self):
        if not hasattr(self, 'filename_reader'): raise ValueError('Please register filename reader.')
        self.existing_filenames = self.filename_reader(self)

    def apply_filename_filter(self):
        if hasattr(self, 'filename_filter'):
           self.all_filenames = self.filename_filter(self)

    def get_new_filenames(self):
        self.new_filenames = [file for file in self.all_filenames 
                              if file.split(self.new_file_location)[1] not in self.existing_filenames]

    def get_new_filenames_without_loc(self):
        self.new_filenames_without_loc = [file.split(self.new_file_location)[1] for file in self.new_filenames] 

    def gather_filenames(self):
        self.read_all_filenames_glob()
        self.apply_filename_filter()
        self.read_existing_filenames()
        self.get_new_filenames()
        self.get_new_filenames_without_loc()

    def append_new_filenames(self, alt_filename=None):
        file = f'{self.existing_file_location}{self.existing_filenames_file}' if alt_filename is None else alt_filename
        append_a_list_to_csv_file(self.new_filenames_without_loc, self.varname, file)



def read_existing_filenames_csv(FilenameGatherer):
    return list(read_csv(FilenameGatherer.existing_file_location + 
            FilenameGatherer.existing_filenames_file)[FilenameGatherer.varname])


def filter_by_keeping_files_within_x_days_old(
    FilenameGatherer, current_time, days_to_keep, date_prefix, date_suffix):
    return keep_files_within_x_days_old(
        FilenameGatherer.all_filenames, current_time, days_to_keep, 
        date_prefix, date_suffix)



from utilities import path_ends_with_slash, append_a_list_to_csv_file
from pandas import read_csv

class LogFileUtilityCSV:
    def __init__(self, file_location, filename, 
                 varname='name'):
        self.file_location = path_ends_with_slash(file_location)
        self.filename = filename
        self.path_and_filename = f'{self.file_location}{self.filename}'
        self.varname=varname
    
    def add_candidate_records(self, records):
        self.candidate_records = list(records)
    
    def read_existing_records(self):
        self.existing_records = list(read_csv(self.path_and_filename)[self.varname])
    
    def get_new_records(self):
        if not hasattr(self, 'candidate_records'): raise ValueError('Please add candiate records.')
        self.new_records = [record for record in self.candidate_records if record not in self.existing_records]
    
    def append_new_records(self, alt_filename=None):
        file = self.path_and_filename if alt_filename is None else alt_filename
        append_a_list_to_csv_file(self.new_records, self.varname, file)

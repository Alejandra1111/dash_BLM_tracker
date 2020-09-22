from glob import glob

from utilities import path_ends_with_slash


class LocalDataAccess:
    def __init__(self, path):
        self.path = path_ends_with_slash(path)
        self.filenames = sorted(glob(self.path + '*'))

    def get_data(self, filename):
        return filename

    def get_files(self, prefix=''):
        return [filename for filename in self.filenames if filename.startswith(path + prefix)]



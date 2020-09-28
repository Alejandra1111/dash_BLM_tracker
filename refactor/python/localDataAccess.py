from glob import glob

from utilities import path_ends_with_slash


class LocalDataAccess:
    def __init__(self, path):
        self.path = path_ends_with_slash(path)
        self.filenames = sorted(glob(f'{self.path}**/*', recursive=True))

    def get_data(self, filename):
        return filename

    def get_files(self, prefix=''):
        return [filename for filename in self.filenames if filename.startswith(f'{self.path}{prefix}')]

    def get_files_dict_for_folders(self, folders):
        files_dict = {}
        for folder in folders:
            files_dict[folder] = self.get_files(f'{folder}/')
        return files_dict

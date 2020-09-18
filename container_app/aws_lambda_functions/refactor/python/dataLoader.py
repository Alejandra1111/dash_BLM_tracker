
from factory import Factory
from dataLoaderMethods import *

data_loader_factory = Factory()
data_loader_factory.register_tool('json', DataLoaderJson)
data_loader_factory.register_tool('csv', DataLoaderCSV)


class DataLoader:

    def __init__(self, DataAccess):
        self.DataAccess = DataAccess

    def load(self, DataFiles, FileFilter=None, IndexFilter=None):
        data_loader = data_loader_factory.get_tool(DataFiles.file_format)
        if FileFilter: DataFiles.apply_file_filter(FileFilter)
        if IndexFilter: 
            indexdata_loader = data_loader_factory.get_tool(IndexFilter.file_format)
            DataFiles.apply_id_filter(IndexFilter, indexdata_loader, self.DataAccess)
        data_loader.load_data(DataFiles, self.DataAccess)
        return data_loader.df
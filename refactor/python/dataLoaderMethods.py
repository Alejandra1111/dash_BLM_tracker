import pandas as pd
from io import BytesIO

from utilities  import convert_floats, filter_df_by_id, isBytes
from datedFiles import *



class DataLoaderBase:
    def load_data(self, DataFiles, DataAccess):
        df = []
        for file, filekey in zip(DataFiles.files, DataFiles.filekeys):
            filtered_ids = DataFiles.filtered_ids.get(filekey, [])
            data = self.read_data(self.get_data(DataAccess, file))
            if len(filtered_ids):
                data = filter_df_by_id(data, DataFiles.id_varname, filtered_ids)   
            if len(data):
                df.append(data)  
        self.df = pd.concat(df, ignore_index=True) 

    def load_data_from_single_file(self, DataAccess, file):
        return self.read_data(self.get_data(DataAccess, file))

    def convert_floats(self, *args, **kwargs):
        self.df = convert_floats(self.df, args, kwargs)  

    def get_data(self): pass

    def read_data(self): pass


class DataLoaderJson(DataLoaderBase):
    def __init__(self, orient='records', lines=True, float_dtype='float64'):
        self.orient = orient
        self.lines = lines
        self.float_dtype = float_dtype

    def get_data(self, DataAccess, file):
        return DataAccess.get_data(file)

    def read_data(self, data):
        return pd.read_json(data, orient=self.orient, lines=self.lines)

    def load_data(self, DataFiles, DataAccess):
        super().load_data(DataFiles, DataAccess)
        convert_floats(self.df, target_dtype=self.float_dtype, 
                       exceptions=DataFiles.id_varname)


class DataLoaderCSV(DataLoaderBase):
    def __init__(self, encoding='latin-1', float_dtype='float64'):
        self.float_dtype = float_dtype
        self.encoding = encoding

    def get_data(self, DataAccess, file):
        data = DataAccess.get_data(file)
        if isBytes(data):
            return BytesIO(data)
        else:
            return data

    def read_data(self, data):
        return pd.read_csv(data, encoding=self.encoding)

    def load_data(self, DataFiles, DataAccess):
        super().load_data(DataFiles, DataAccess)
        convert_floats(self.df, target_dtype=self.float_dtype, 
                       exceptions=DataFiles.id_varname)
        



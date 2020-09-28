from pandas import to_datetime
from utilities import time_now_pandas


data_source = 'tests/unit/fixtures/'
data_dest = 'tests/unit/fixtures/'

current_time = time_now_pandas()
# !! overwrite time during code dev
current_time = to_datetime('2020-07-08 23:59')
current_time_str = str(current_time)

def reset_time():
    global current_time, current_time_str
    current_time = time_now_pandas()
    # !! overwrite time during code dev
    current_time = to_datetime('2020-07-08 23:59')
    current_time_str = str(current_time)
    print('current_time:', current_time_str)


to_json_args = dict(orient='records', lines=True, date_format='iso')
read_json_args = dict(orient='records', lines=True)
json_split = dict(orient='split')
data_types = dict(float_dtype='float32')

cities = ['Minneapolis','LosAngeles','Denver','Miami','Memphis',
          'NewYork','Louisville','Columbus','Atlanta','Washington',
          'Chicago','Boston','Oakland','StLouis','Portland',
          'Seattle','Houston','SanFrancisco','Philadelphia','Baltimore']

city_filterwords = {
    'Minneapolis': ['Minneapolis','mlps', ['St', 'Paul']],
    'LosAngeles':['LosAngeles','LA', ['Los', 'Angeles']],
    'Denver': ['Denver', 'DEN'],
    'Miami': ['Miami'],
    'Memphis': ['Memphis'],
    'NewYork': ['NewYork',['New','York'], 'NY','NYC','manhattahn'],
    'Louisville': ['Louisville'],
    'Columbus': ['Columbus'],
    'Atlanta': ['Atlanta'],
    'Washington': ['Washington','DC','WashingtonDC'],
    'Chicago': ['Chicago'],
    'Boston': ['Boston'],
    'Oakland': ['Oakland'],
    'StLouis': ['StLouis',['St','Loius']],
    'Portland': ['Portland'],
    'Seattle': ['Seattle'],
    'Houston': ['Houston'],
    'SanFrancisco': ['SanFrancisco','SF',['San','Francisco']],
    'Philadelphia': ['Philadelphia'],
    'Baltimore': ['Baltimore']
}

cities_all = ['all_v1','all_v2','all_v3','all_v4','all_v5']

days_to_keep = 2 # files to read within x days
days_to_process = 2 # original tweet ids to match within x days 

stat_days_short = 7 # used in calculating past "7 day" stats
stat_days_long = 14  # used in calculating past "14 day" stats






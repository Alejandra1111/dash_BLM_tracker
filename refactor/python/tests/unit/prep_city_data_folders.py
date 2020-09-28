# import sys
# sys.path.append('python/tests/unit/')

import os
from glob import glob

data_source = 'fixtures/'
data_dest = 'fixtures/'

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


print(glob('*'))

for city in cities + cities_all :

    # Parent Directory path 
    parent_dir = data_dest + "data_cumulative/city_date/"

    # Path 
    path = os.path.join(parent_dir, city) 

    dir_exist = os.path.isdir(path)

    if not dir_exist:
        # Create the directory 
        os.mkdir(path) 
        os.mkdir(path + '/original') 
        os.mkdir(path + '/retweet') 
        os.mkdir(path + '/sentiments') 
        os.mkdir(path + '/emotions')
        os.mkdir(path + '/words') 
        print("City_date directory for '%s' created" %city) 



import shutil 

abs_path = '/Users/kotaminegishi/big_data_training/python/dash_BLM/'
for city in cities + cities_all :

    path0 = f'{abs_path}data_cumulative/city_date/{city}/retweet/2020_all_retweets.json'
    path1 = f'{data_dest}data_cumulative/city_date/{city}/retweet/2020_all_retweets.json'

    shutil.copyfile(path0, path1)


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

#data_dest = '/Users/kotaminegishi/big_data_training/python/dash_demo1/'
# data_dest = '/data/app_data/'

current_time = datetime.utcnow() + pd.DateOffset(hours=-6)
current_time_s = current_time.strftime('%Y-%m-%d %H:%M:%S')
current_time_s = pd.to_datetime(current_time_s)
current_time_s

data_dest_files = data_dest + 'data_cumulative/'
days_to_keep = 2 # files to read within x days
days_to_process = 2 # original tweet ids to match within x days 

# current_time = datetime.utcnow() + pd.DateOffset(hours=-6)
# current_time_s = current_time.strftime('%Y-%m-%d %H:%M:%S')

# current_time_s = pd.to_datetime(current_time_s)
base_timestamp = current_time_s
#base_timestamp =  pd.to_datetime(datetime(2020,7,27))

print('Going to process city_date data: ', base_timestamp)   


for city in cities:

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


files_retweet = [data_dest + 'data_cumulative/retweet/2020_all_retweets.json']


files_all_original = keep_recent_files(
    glob(data_dest_files + 'original/*'), 
    base_timestamp, 
    file_type= '.json', 
    days = days_to_keep, no_newer=True)

files_all_sentiments = keep_recent_files(
    glob(data_dest_files + 'sentiments/*'), 
    base_timestamp, 
    file_type= '.csv', 
    days = days_to_keep, no_newer=True)

files_all_emotions = keep_recent_files(
    glob(data_dest_files + 'emotions/*'), 
    base_timestamp, 
    file_type= '.csv', 
    days = days_to_keep, no_newer=True)

files_all_words = keep_recent_files(
    glob(data_dest_files + 'words/*'), 
    base_timestamp, 
    file_type= '.json', 
    days = days_to_keep, no_newer=True)

try:
    # read previous filenames
    files_existing_original = pd.read_csv(data_dest + 'data_filenames/files_read_original.csv')
    files_existing_sentiments = pd.read_csv(data_dest + 'data_filenames/files_read_sentiments.csv')
    files_existing_emotions = pd.read_csv(data_dest + 'data_filenames/files_read_emotions.csv')
    files_existing_words = pd.read_csv(data_dest + 'data_filenames/files_read_words.csv')

    # get new file names 
    files_original = [file for file in files_all_original if file.split(data_dest_files)[1] not in np.array(files_existing_original.name)]
    files_sentiments = [file for file in files_all_sentiments if file.split(data_dest_files)[1] not in np.array(files_existing_sentiments.name)]
    files_emotions = [file for file in files_all_emotions if file.split(data_dest_files)[1] not in np.array(files_existing_emotions.name)]
    files_words = [file for file in files_all_words if file.split(data_dest_files)[1] not in np.array(files_existing_words.name)]

    mode = 'a'
    header = False

except:
    # initialize filenames 
    files_original = files_all_original
    files_sentiments = files_all_sentiments
    files_emotions = files_all_emotions
    files_words = files_all_words
    mode = 'w'
    header = True


city = 'Minneapolis'


data_dest = '/data/app_data/'
data_dest = '/Users/kotaminegishi/big_data_training/python/dash_demo1/'

data_dest_files = data_dest + 'data_cumulative/'
#data_dest_files + 'city_date/' + city + '/sentiments'


for city in cities + cities_all:
    files  = glob(data_dest_files + 'city_date/' + city + '/sentiments/*')
    for file in files:
        #print(file)
        df = pd.read_csv(file)
        idx1 = df['created_at_h']!='created_at_h'
        #idx1[False==idx1]
        if sum(idx1)<len(df):
            print('rewrite file:', file)
            df[idx1].to_csv(file, mode='w', header=True, index=False)





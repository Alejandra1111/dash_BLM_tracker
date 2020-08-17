import numpy as np
import pandas as pd
import re
import string
import nltk
#nltk.download('vader_lexicon') 
#nltk.download('stopwords')
#nltk.download('averaged_perceptron_tagger')
#nltk.download('wordnet')
#nltk.download('punkt')
from nltk.tag import pos_tag
from nltk.stem.wordnet import WordNetLemmatizer
from nltk import tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import sys
from nrclex import NRCLex
from collections import Counter
import itertools 
from glob import glob



def tw_data_files_to_df_csv(files):
    '''append and concat data files into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_csv(file)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    return df

def tw_data_files_to_df_csv2(files, frac=0.05, float_dtype=None):
    '''append and concat a sample of data into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_csv(file, low_memory=True)
        .sample(frac=frac, replace=True)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    if float_dtype is None: return df
    return convert_floats(df, float_dtype)

def convert_floats(df, float_dtype='float32'):
    floats = df.select_dtypes(include=['float64']).columns.tolist()
    df[floats] = df[floats].astype(float_dtype)
    return df


def tw_data_files_to_df_json(files, lines=False):
    '''append and concat data files into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_json(file, orient='records', lines=lines)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    return df

def tw_data_files_to_df_json3(files, lines=False, frac=0.05, float_dtype=None):
    '''append and concat a sample of data into a pandas.DataFrame'''
    df = []
    [ df.append(pd.read_json(file, orient='records', lines=lines)
                 .sample(frac=frac, replace=True)) for file in files ]
    df = pd.concat(df, ignore_index=True)
    if float_dtype is None: return df
    return convert_floats(df, float_dtype)


def tw_data_format_created_at(df):
    # assumes variable 'created_at' exists
    # uses CST as timestamp
    df['created_at'] = pd.to_datetime(df.created_at, unit='s') + pd.DateOffset(hours=-6) # CST
    df['created_at_h'] =  df['created_at'].dt.floor("h")
    return df


def lemmatize_sentence(tokens):
    lemmatizer = WordNetLemmatizer()
    lemmatized_sentence = []
    for word, tag in pos_tag(tokens):
        if tag.startswith('NN'):
            pos = 'n'
        elif tag.startswith('VB'):
            pos = 'v'
        else:
            pos = 'a'
        lemmatized_sentence.append(lemmatizer.lemmatize(word, pos))
    return lemmatized_sentence

def remove_noise(tweet_tokens, stop_words = ()):

    cleaned_tokens = []

    for token, tag in pos_tag(tweet_tokens):
        token = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|'\
                       '(?:%[0-9a-fA-F][0-9a-fA-F]))+','', token)
        token = re.sub("(@[A-Za-z0-9_]+)","", token)

        if tag.startswith("NN"):
            pos = 'n'
        elif tag.startswith('VB'):
            pos = 'v'
        else:
            pos = 'a'

        lemmatizer = WordNetLemmatizer()
        token = lemmatizer.lemmatize(token, pos)

        if len(token) > 0 and token not in string.punctuation and token.lower() not in stop_words:
            cleaned_tokens.append(token.lower())
    return cleaned_tokens

def clean_tokens(tweet_texts):
    mytokens = [tw.split() for tw in tweet_texts]

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords.extend(['#blacklivesmatter', '&amp;', 'please','retweet'])

    cleaned_tokens_list = []

    for tokens in mytokens:
        cleaned_tokens_list.append(remove_noise(tokens, stopwords))
    return cleaned_tokens_list


def join_token(token):
    return " ".join(str(word) for word in token)



def get_df_and_ids(self):
    '''used in assign_sentiments() and assign_emotions'''
    if (self.type=='original'):
        df = self.df[[ len(token) > 0 for token in self.df.tokens] ].reset_index() 
        ids = df.id
    elif (self.type=='retweet'):
        df = self.df
        ids = df.RT_id
    return df, ids



class new_tw_data():
    
    def __init__(self, new_data_files, type):
        if (type in ['original', 'retweet']):
            self.type = type 
            if self.type=='original': self.id = 'id'
            else: self.id = 'RT_id'
        
        else:
            print('value error: type must be either "original" or "retweet".')
            sys.exit()
            
        if len(new_data_files)>0:
            # define new data of original tweets and retweets
            df = tw_data_files_to_df_json(new_data_files, lines=True)
            df = tw_data_format_created_at(df)
            if hasattr(df, 'quoted_text'):
                df['tokens'] = clean_tokens(df.text + df.quoted_text)
            else:
                df['tokens'] = clean_tokens(df.text)
            self.df = df.drop_duplicates(subset = [self.id])
        else:
            self.df = None
     
    
    def assign_sentiments(self):
        if self.df is None:
            self.df_sentiments = None
            return
        
        df, ids = get_df_and_ids(self)
        if len(df)==0: 
            self.df_sentiments = None
            return
    
        idx_ini = 0
        idx_end = len(ids) # 50
        try:
            tweets = df.tokens[idx_ini:idx_end]
            ids = ids[idx_ini:idx_end]
            created_at_h = df.created_at_h[idx_ini:idx_end]

            sid = SentimentIntensityAnalyzer()
            score_1 = sid.polarity_scores(join_token(tweets[0]))

            sentiments = []

            for i, tweet in enumerate(tweets):
                    score = sid.polarity_scores(join_token(tweet))
                    sentiments.append([score[key] for key in score])

            df_sentiments = pd.DataFrame(data = sentiments, columns=score_1.keys())
            df_sentiments = df_sentiments.set_index([pd.Index(ids), pd.Index(created_at_h)])
        
        except Exception as e:
            print(e.__doc__)
            
        self.df_sentiments = df_sentiments
        
        
    def assign_emotions(self):
        if self.df is None: 
            self.df_emotions = None
            return
        df, ids = get_df_and_ids(self)
        if len(df)==0: 
            self.df_emotions = None
            return

        idx_ini = 0
        idx_end = len(ids) # 50
        try:
            tweets = df.tokens[idx_ini:idx_end]
            ids = ids[idx_ini:idx_end]
            created_at_h = df.created_at_h[idx_ini:idx_end]

            nrc_1 = NRCLex(join_token(tweets[0]))
            emo_labels = nrc_1.affect_frequencies.keys()
            top_emotions = []

            for i, tweet in enumerate(tweets):
                nrc = NRCLex(join_token(tweet))
                emos = [ i[0] for i in nrc.top_emotions]
                top_emotions.append([ i in emos for i in emo_labels])

            df_top_emotions = pd.DataFrame(data = top_emotions, 
             columns = emo_labels)

            df_top_emotions = df_top_emotions.set_index([pd.Index(ids), pd.Index(created_at_h)])
        
        except Exception as e:
            print(e.__doc__)
        
        self.df_top_emotions = df_top_emotions


    def count_words(self):
        if self.df is None: 
            self.df_words = None
            return
        df = self.df
        try:
            df['token_counter'] = [ Counter(token) for token in df['tokens'] ]
            if self.type == 'original':
                df = df.set_index(['id', 'created_at_h'])
            elif self.type == 'retweet':
                df = df.set_index(['RT_id', 'created_at_h'])
        
        except Exception as e:
            print(e.__doc__)
        
        self.df_words = df[['token_counter']]
        self.df = self.df.drop(columns = ['token_counter']) 




def keep_recent_files(files, base_timestamp, file_type= '.json', days = 14, hours=0,
                      no_newer=False, no_newer_h=0, prefix = 'created_at_'):
    timestamps = [pd.Timestamp(file.split(prefix,1)[1]
                               .replace(file_type,'').replace('_',' ')) for file in files ]
    if no_newer: 
        keep_idx1 = [(base_timestamp - timestamp <= pd.Timedelta(days, unit='d') + pd.Timedelta(hours, unit='h')) & 
                     (base_timestamp - timestamp >= -pd.Timedelta(no_newer_h, unit='h')) for timestamp in timestamps]
    else: 
        keep_idx1 = [base_timestamp - timestamp <= pd.Timedelta(days, unit='d') + + pd.Timedelta(hours, unit='h') for timestamp in timestamps]
    return(list(itertools.compress(files,keep_idx1)))




def fix_datetime(df, timevar='created_at_h'):
    df[timevar] = pd.to_datetime(df[timevar])

def fix_token_counter(df):
    df.token_counter = df.token_counter.apply(lambda x: Counter(x))  

def fix_RT_id(df):
    df.RT_id = df.RT_id.astype(str) 


def merge_datasets(or_df, or_data, ref_data):
    if or_data is None: return()
    
    col_data = [*or_data.columns]
    
    ref_data['RT_id'] = ref_data['RT_id'].astype(str) 
    
    or_non_empty = or_df[or_df.RT_id != '']
    
    if len(or_non_empty)>0:
        retweeted_data = (or_non_empty[['id','RT_id','created_at_h']]
             .join(ref_data.set_index('RT_id'), 
                   on='RT_id', rsuffix='_rt')
            )

        df_merged = (
            or_data.reset_index()[['id','created_at_h', *col_data]] 
            .append(retweeted_data[['id','created_at_h', *col_data]])
            )
        
        print('Num rows = {} + {} = {}'
          .format(len(or_data), len(retweeted_data), len(df_merged)))
        return df_merged
   
    else: 
        print('Num rows = {}'.format(len(or_data)))
        return or_data.reset_index()[['id','created_at_h', *col_data]] 




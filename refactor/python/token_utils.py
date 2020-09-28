import nltk
from nltk.tag import pos_tag
from nltk.stem.wordnet import WordNetLemmatizer
from nltk import tokenize
import re
import string


def get_clean_lemmatized_tokens(texts):
    stopwords = nltk.corpus.stopwords.words('english')
    stopwords.extend(['#blacklivesmatter', '&amp;', 'please','retweet'])
    
    cleaned_tokens_list = []
    for text in texts:
        tokens = remove_noise(text.lower()).split()
        clean_tokens = remove_stopwords(tokens, stopwords)
        cleaned_tokens_list.append( 
            lemmatize_sentence(
              clean_tokens
            )
        )
    return cleaned_tokens_list

def remove_noise(token):
    token = re.sub('(https?://.*)|(www[.].*)','', token)
    token = re.sub("(@[A-Za-z0-9_]+)","", token)
    token = re.sub("[.,!?]"," ", token)
    return token

def remove_stopwords(tokens, stopwords):
    return [word for word in tokens if word not in stopwords]

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


def join_token(token):
    return " ".join(str(word) for word in token)

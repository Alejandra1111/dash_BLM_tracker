import time

from process_new_data import *


if __name__=="__main__":

    import nltk
    nltk.download('vader_lexicon') 
    nltk.download('stopwords')
    nltk.download('averaged_perceptron_tagger')
    nltk.download('wordnet')
    nltk.download('punkt')

    while True: # run continuously until stopped
       
        set_globals()
        process_new_data()

        
        
        time.sleep(3600) # 3600 seconds of sleep 







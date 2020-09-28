import time

from globals import reset_time
from process_new_data import process_new_data
from process_city_date_data import process_city_date_data
from process_city_date_stats import process_city_date_stats


if __name__=="__main__":

    import nltk
    nltk.download('vader_lexicon') 
    nltk.download('stopwords')
    nltk.download('averaged_perceptron_tagger')
    nltk.download('wordnet')
    nltk.download('punkt')


    while True: # run continuously until stopped

        # reset_time()
        process_new_data()
        # process_city_date_data()
        # process_city_date_stats()

        time.sleep(3600) # 3600 seconds of sleep 







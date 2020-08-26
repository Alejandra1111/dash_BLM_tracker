
## About this app

This site is a web application of **Twitter** data gathering, storage, and analysis on hashtag **BlackLivesMatter**. It is powered by **Twitter API**, **AWS Kinesis**, **Python**, and **Dash**. [Source code](https://github.com/kotamine/dash_BLM_tracker). 

Users of this app can **browse through BLM statistics** for **selected city and time** to observe **trends in sentiments and emotions** and discover **top tweets and influencers**. One can also apply **a keyword filter** to narrow the focus. 

Please note that "city" data are gathered based on mentioning of city's name in tweets as opposed to users' location information, which is rarely available.  



### Current Project Status 

The app is currently using **the data collected during the development stage**. There have been some disruptions in the data collection process, so **the data may be incomplete for some dates and hours.**  

**The data before 7/1 are retroactively gathered** through [Twitter's full archive API](https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/quick-start/premium-full-archive). It is *expensive* to collect millions of tweets data in this way, and hence the data was gathered only at a limited scope (that is, the data were searched for a two-minute interval for each hour on Wednesdays and Saturdays with the maximum of 500 tweets for each of those two-minute intervals). Therefore, whenever a date before 7/1 is selected, the app directs the user to a nearby data-collection date, and the data is only available for "All Cities Version 1".   


---

### App Components

#### Dashboard

The dashboard provides a snapshot of what's going on across different cities in the form of sentiment statistics, ranging from -1 (negative sentiments) to 1 (positive sentiments). The sentiment of a given city is compared with the current overall sentiment across all cities and with the past sentiments of the city.  


#### Dataset Selection

The app initially loads summary statistics of data for **all cities**.  **All cities** data are based on **a 2% random sample** of the whole data collected, and hence the app lets you choose a version of the random sample. When you **select a particular city**, the app loads a pre-processed dataset filtered by the city's name in tweet text.  

Initially, the date and time are set at the current time in CST. When you **select a date**, the app loads archival data, recalculate summary statistics, and updates figures and tables. When you **select an hour**, the app updates statistics for the last hour under the Top Words, Top Tweets, and Top Users tabs  (for example, the previous hour at hour = 20:00 uses the data recorded between 20:00 to 20:59).  

You can further **filter data by entering a filtering keyword**. The app will go back to the archival raw data, apply the filter in tweet text, and recaulculate statistics, which involves somewhat intensive data processing and hence takes time. Filtering can be used to target your search. Note that depending on the size of the dataset, filtering may result in much fewer data points or no data at all. 


#### Sentiments

**Sentiment** score is calculated for each tweet by the "polarity_scores" of the **nltk** package (Natural Language Toolkit), ranging from -1 (negative) to +1 (positive) sentiment. The relevant tweets' scores are then averaged for each hour. 

**Emotions** are calculated for each tweet by the top emotions the **nrclex** package (NRC Lexicon). Each tweet is assessed for exhibiting which of the 7 selected emotions and index for 0 (absent of a given emotion) or 1 (present). Multiple emotions are indexed 1 in the case of a tie. The relevant tweets' indices are averaged for each hour.   


#### Top Words

The app calculates the **most frequently occuring words** for a selected dataset and displays them as word cloud and bar chart.  If the selected dataset is large, the app uses a random sample of 10,000 tweets, 



#### Top Tweets

The app identifies **the most retweeted tweets** in **a selected timespan** (out of last hour, today, yesterday, past 7 days). 


#### Top Users


The app identifies **the most retweeted Twitter users** in **a selected timespan** (out of last hour, today, yesterday, past 7 days). 


#### Daily Summary Subscription

A user can sign up to receive a daily summary via email. All the information we collect is email address. The daily email is scheduled sometime around 4 to 5 pm CST.  Unsubscribing is done by re-entering the email address, which the app recognizes and changes the subscribe-button into an unsubscribe-button. 


--- 
### Data Collection and Processing
#### Data Flow Chart

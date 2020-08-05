
* **AWS EC2 (t2.micro #1)**: runs python code for Twitter API listeners on data streams using **#BlackLivesMatter** as a filter (via *tweepy*) and sending data of interests to *AWS Kinesis streams* (via *AWS boto3*). It separately collects data of original tweets and those of retweeted tweets that have been retweeted over a threshold. Many tweets are retweets, so that omitting popular retweet texts from the dataset of original tweets reduces the redundancy of repeated text data. The data collected on the popular retweets also include tweet's user description and URL, so that we can trace back key tweets and key Twitter users.     
* **AWS Kinesis**: *Kinesis Streams* accept streaming data inflows and relay them to *Kinesis Firehose* programs, which sink the data into a *AWS S3* data storage as batch files. 
* **AWS S3**: Stores all data, or initially sinked data and subsequently processed data. S3 storage can be volume-mounted in EC2 instances, which makes it easy to share data across containers.   
* **AWS EC2 (t2.small)**: processes the data into different data files such as original tweets, detailed retweets, sentiment analysis scores, and cleaned-up word tokens, and data summaries such as mean statistics of sentiment analyses, the most common word tokens, and the most retweeted tweets and associated Twitter users etc. The processing is first done for all data and are then stored by *city* and *date* for a list of 20 cities by picking up city names as keywords in tweet text. In addition, 5 sets of 10% sample data from all data from all cities are stored.    
* **AWS EC2 (t2.micro #2)**: hosts the *Dash* app that you are seeing.  



#### Current Status 

The app currently mostly uses the data collected during the development stage. There have been some disruptions in the data collection process, so the data are incomplete for certain dates and hours.  

The data can be updated once the data archives are collected and organized (work in progress).  


---

### App Components

#### Sentiments


#### To Words


#### Top Tweets


#### Top Users


---

Created by: **Kota Minegishi** 

Last Modified: **7/24/2020**

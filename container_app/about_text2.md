
* **AWS EC2 (t2.micro)**: runs python code for Twitter API listeners on data streams using **#BlackLivesMatter** as a filter (via **tweepy**) and sending data of interests to **AWS Kinesis streams** (via **AWS boto3**). It separately collects data of original tweets and those of retweeted tweets that have been retweeted over a threshold. Many tweets are retweets, so that omitting popular retweet texts from the dataset of original tweets reduces the redundancy of repeated text data. The data collected on the popular retweets also include tweet's user description and URL, so that we can trace back key tweets and key Twitter users.     
* **AWS Kinesis**: **Kinesis Streams** accept streaming data inflows and relay them to **Kinesis Firehose** programs, which sink the data into a **AWS S3** data storage as batch files. 
* **AWS S3**: Stores all data, or initially sinked data and subsequently processed data. S3 storage can be volume-mounted in EC2 instances, which makes it easy to share data across containers.   
* **AWS EC2 (t2.small)**: processes the data into different data files such as original tweets, detailed retweets, sentiment analysis scores, and cleaned-up word tokens, and data summaries such as mean statistics of sentiment analyses, the most common word tokens, and the most retweeted tweets and associated Twitter users etc. The processing is first done for all data and are then stored by **city** and **date** for a list of 20 cities by picking up city names as keywords in tweet text. In addition, 5 sets of 2% sample data from all data from all cities are stored. The city-date level statistics data and raw data are stored separately where the latter is used for user-specified data filtering process in the app.    
* **Heroku Dyno**: hosts the **Dash** app that you are seeing. To reduce the computational burden and memory usage in the server, much of the data processing task is done through **AWS Lambda functions** (in particular, **user-specified filtering of raw data and re-calculating statistics** involves quite intensive data-processing task). Using the Labmda functions makes it much easier to handle the scale with only small incremental workload added to the host server. The current app should be able to host concurrent sessions of a fair number of users (I'm not exactly sure what the current limit is). To scale, dyno can be upgraded and more worker nodes be added.   



---

Created by: **Kota Minegishi** 

Last Modified: **8/26/2020**

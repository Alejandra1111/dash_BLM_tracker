from datetime import datetime as dt
import pandas as pd
import sqlite3
import yagmail
import chart_studio
import chart_studio.plotly as py

import conf_credentials as conf

from parameters import daily_template
from helpers_light import fig_dashboard, fix_user_id, fig_top_users, formatInt


# sqlite db
db = "blm_daily.db"

# plotly chart studio clinent
chart_studio.tools.set_credentials_file(
    username= conf.chart_studio['username'], 
    api_key= conf.chart_studio['api_key'])

# yagmail client
yag = yagmail.SMTP(
  user=conf.yag['user'],
  password=conf.yag['password'])


def get_time():
    return (dt.utcnow() + pd.DateOffset(hours=-6)).strftime('%Y-%m-%d %H:%M:%S')

def get_str_date():
    return str(get_time())[:10]

def get_daily_dashboard(current_city_stats, latest_datatime):
    url = py.plot(fig_dashboard(current_city_stats, latest_datatime), 
        auto_open=False, 
        filename='dashboard_{date}'.format(date= str(latest_datatime)[:10]))
    return url


def get_daily_top_users(current_data_cities, latest_datatime):
    df = pd.read_json(current_data_cities['all_v1']['top_users'], orient='split') 
    fix_user_id(df)
    url = py.plot(fig_top_users(df, 'today'),
        auto_open=False, 
        filename='top_users_{date}'.format(date= str(latest_datatime)[:10]))
    return url


def get_daily_top_tweets(current_data_cities):
    top_tweets = pd.read_json(current_data_cities['all_v1']['top_tweets'], orient='split')
    formatInt(top_tweets, ['followers_count', 'retweet_timespan', 'retweet_total'])

    tt1 = (top_tweets[top_tweets.subset=='today'][['user_name','followers_count','text','t_co',
                                              'retweet_timespan','retweet_total']]
       .rename(columns = {'user_name': 'User Name', 'followers_count':'Followers',
                         'text':'Tweets', 't_co':'Link', 'retweet_timespan':'Retweets in DB', 
                          'retweet_total':'Retweets Total'})
      )

    tt1_html = (tt1.to_html(index=False)
             .replace('\n',' ')
             .replace('<table border="1" class="dataframe">',
                '<table align="center" width="600" border="1" rules="rows" bordercolor="#e0e2e2" cellspacing="0" cellpadding="2" bgcolor="#ffffff">')
             .replace('<tr style="text-align: right;">', 
                     '<tr style="text-align: center; background-color: #b8c9ca;">')   
            )
    return tt1_html


def email_invalid(email):
    return str(email).find('@')==-1 or str(email).find('.')==-1


def email_exists(email):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("SELECT * from blm_daily_email where email =='{email}'".format(email=str(email)))
    result = c.fetchall()
    conn.close()
    return result


def email_already_sent_today(date):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("select datetime from blm_daily_log")
    result = c.fetchall()
    conn.close()
    previous_email = result.pop()[0]
    print(previous_email)
    return previous_email[:10] == date


def get_email_addresses():
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("SELECT * from blm_daily_email")
    results = c.fetchall()
    conn.close()
    return [result['email'] for result in results]


def log_sent_emails(emails, time):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("INSERT INTO blm_daily_log VALUES (?, ?)", (str(emails), time))
    conn.commit()
    conn.close()


def add_email_address(email, time):
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("INSERT INTO blm_daily_email VALUES (?, ?)", (str(email), time))
    conn.commit()
    conn.close()


class SubscriptionEmail:
    def __init__(self, current_data_cities, latest_datatime, current_city_stats):
        self.current_data_cities = current_data_cities
        self.latest_datatime = latest_datatime
        self.current_city_stats = current_city_stats
        self.date = get_str_date()

    def compose_email(self, note=''):
        return daily_template.format(
            note = '',
            date = self.date, 
            url_dashboard = get_daily_dashboard(self.current_city_stats, self.latest_datatime),
            url_top_users = get_daily_top_users(self.current_data_cities, self.latest_datatime),
            html_top_tweets = get_daily_top_tweets(self.current_data_cities)
            ) 

    def send_daily_summary_email(self):
        email_body = self.compose_email() 
        emails = get_email_addresses()
        print('Sending daily emails to:', emails)
        for email in emails:
            try: 
                yag.send(str(email), 
                     'BLM Tracker Daily Summary - {date}'.format(date = self.date), 
                     email_body)
                print('daily email sent to:', str(email))
            except:
                print('failed: daily email to:', str(email))
        current_time = get_time()
        log_sent_emails(emails, current_time)
     


    def send_subscription_email(self, email):       
        current_time = get_time()
        add_email_address(email, current_time)
        date = current_time[:10]
        email_body = self.compose_email( 
            note = 'This email confirms that you are subscribed to BLM Tracker Daily Summary.' +
            ' The following is a sample of daily summary.') 
        try: 
            yag.send(str(email), 
                 'BLM Tracker Subscription Confirmation - {date}'.format(date = self.date), 
                 email_body)
        except:
            pass


    def send_unsubscription_email(self, email):
        conn = sqlite3.connect(db)
        c = conn.cursor()
        try:
            c.execute("DELETE from blm_daily_email where email =='{email}'".format(email=str(email)))
            conn.commit()
        except:
            pass
        conn.close()
        try:
            yag.send(str(email), 
                     'BLM Tracker Unsubscription Confirmation', 
                     'This email confirms that you are unsubscribed from BLM Tracker Daily Summary.')
        except:
            pass



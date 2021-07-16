import os
import pandas as pd

adf = pd.read_csv('tweet_tables.csv')
news_table = 'febnewsprocessed'
for i in range(len(adf)):
    tweet_table = adf['table_name'].iloc[i]
    news_date = adf['news_date'].iloc[i]
    os.system('python run_mesos.py --news-table {0} --news-date {1} --tweet-table {2} -r'.format(news_table, news_date, tweet_table))
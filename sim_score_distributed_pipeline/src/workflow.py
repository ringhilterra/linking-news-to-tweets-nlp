import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk.stem
import sqlalchemy as sq
import io

english_stemmer = nltk.stem.SnowballStemmer('english')
analyzer = TfidfVectorizer().build_analyzer()

PG_CONN_STRING = 'postgresql://*****'
#PG_CONN_STRING = '******'


def stemmed_words(doc):
    return (english_stemmer.stem(w) for w in analyzer(doc))


def get_tfidf():
    tfidf = TfidfVectorizer(stop_words='english', smooth_idf=True, analyzer=stemmed_words)
    return tfidf
 

def sim_scores(tweet, news_tfidf_vector, vocab):
    tf_sum = np.zeros((news_tfidf_vector.shape[0], 1))
    
    # iterate through the words in tweet
    for w in tweet.split():
        #w = w.lower()
        if w in vocab:
            idx = vocab[w] # save idx
            tf_sum += news_tfidf_vector[:, idx]
            
    output_df = pd.DataFrame(tf_sum)[0]

    return output_df


def full_workflow(tweet, tweet_id, news_vector, tfidf, topN, news_dic, news_df):
    news_scores = sim_scores(tweet, news_vector, tfidf.vocabulary_) 

    # want news_id and news_scores associated and then want to max topN values
    news_df['score'] = news_scores
    
    sorted_news = news_df.sort_values('score', ascending=False)
            
    for i in range(topN):
        result_row = sorted_news.iloc[i] 
        score = result_row['score']
        if score > 0.3:
            news_id = result_row['newsid']
            #print(news_id)
            top_tweet_list = news_dic[news_id]
            top_tweet_list.append((tweet_id, score))
            news_dic[news_id] = top_tweet_list
        
    return news_dic


def get_tweet_df_pg(tweet_table):
    engine = sq.create_engine(PG_CONN_STRING)
    TWEET_QUERY = """select * from ryan_test.{tweet_table}""".format(tweet_table=tweet_table)
    tweet_df = pd.read_sql_query(TWEET_QUERY, engine)
    tweet_df.columns = ['id', 'tweet_text']
    tweet_df['tweet_text'] = tweet_df['tweet_text'].astype('str') 
    return tweet_df


def get_news_df_pg(news_table, date):
    engine = sq.create_engine(PG_CONN_STRING)
    NEWS_QUERY = """select * from ryan_test.{news_table}""".format(news_table=news_table)
    news_df = pd.read_sql_query(NEWS_QUERY, engine)
    news_df = news_df[news_df.publishdate == date].reset_index(drop=True)
    return news_df
    

def get_results(news_df, tweet_df, news_vector, tfidf):
    news_ids = list(news_df.newsid.values)
    news_dic = {}
    for news_id in news_ids:
        news_dic[news_id] = []
    # for now hardcoded only first 100 tweets but need to replace with len(tweet_df)
    for i in range(len(tweet_df)):
        #print(i)
        tweet_row = tweet_df.iloc[i]
        tweet = tweet_row['tweet_text']
        tweet_id = tweet_row['id']
        topN = 10
        # want to get top 10 news articles associated witht each tweet
        news_dic = full_workflow(tweet, tweet_id, news_vector, tfidf, topN, news_dic, news_df)

    result_series = pd.Series(news_dic, name='DateValue')
    result_df = pd.DataFrame(result_series).reset_index()
    result_df.columns = ['newsid', 'sim_tweets']
    return result_df


def run_main(news_table, news_date, tweet_table):
    feb_news = get_news_df_pg(news_table, news_date)
    tfidf = get_tfidf()
    news_vector = tfidf.fit_transform(feb_news['text'])
    tweet_df = get_tweet_df_pg(tweet_table)
    result_df = get_results(feb_news, tweet_df, news_vector, tfidf)
    result_df['news_date'] = news_date
    result_df['tweet_file'] = '-'.join(tweet_table.replace('tweetsprocessed', '').split('_'))
    write_results_pg(result_df, tweet_table)
    
    return result_df


def write_results_pg(result_df, tweet_table):
    engine = sq.create_engine(PG_CONN_STRING)
    
    schema = 'ryan_test'
    result_table = 'final_sim_results'
    
    #result_df.head(0).to_sql(result_table, con=engine, schema=schema, index=False, if_exists='replace') #truncates the table
    conn = engine.raw_connection()
    cur = conn.cursor()
    cur.execute("SET search_path TO '{schema}'".format(schema=schema))
    output = io.StringIO()
    result_df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table=result_table, null="") # null values become ''
    conn.commit()
    print('completed write to db')
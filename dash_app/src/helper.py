import  pandas as pd
import numpy as np
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer


def get_top_news(keywords, day):
    if day[:6] == '2018-2':
        print('feb')
        feb_news = pd.read_csv('news_files/FebNewsProcessed.csv')
        vocab_file = open('pkl_files/feb_vocab.pkl', 'rb')
        vocab = pickle.load(vocab_file)
        tfidf_file = open('pkl_files/feb_news_tfidf.pkl', 'rb')
        tfidf_news = pickle.load(tfidf_file)
        top_articles = get_top_n_highest(keywords, tfidf_news, vocab, day, feb_news, 10, 0.3)
    else:
        print('mar')
        mar_news = pd.read_csv('news_files/MarNewsProcessed.csv')
        vocab_file = open('pkl_files/mar_vocab.pkl', 'rb')
        vocab = pickle.load(vocab_file)
        tfidf_file = open('pkl_files/mar_news_tfidf.pkl', 'rb')
        tfidf_news = pickle.load(tfidf_file)
        top_articles = get_top_n_highest(keywords, tfidf_news, vocab, day, mar_news, 10, 0.3)
        
    return top_articles


def get_top_n_highest(input_string, tfidf, vocab, publish_date, news_df, topN, threshold):
    tf_sum = np.zeros((tfidf.shape[0], 1))

    for w in input_string.split():
        w = w.lower()
        if w in vocab:
            idx = vocab[w]
            tf_sum += tfidf[:, idx]

    output_df = pd.DataFrame(tf_sum)[0]
    output_df = output_df[output_df > threshold]
    
    news_df['score'] = output_df
    news_df = news_df.fillna(0)
    news_df = news_df[news_df['score'] > threshold]
    top_news = news_df[news_df.publishdate == publish_date].sort_values('score', ascending=False).iloc[:topN]

    return top_news
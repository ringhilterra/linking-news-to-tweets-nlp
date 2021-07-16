import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import scipy.sparse
import pickle
import pkg_resources


def process_news():
    # Load News
    print('Loading News Articles')
    news = pd.read_csv(pkg_resources.resource_filename('tweetsnews.data', 'news-feb-01.csv'), header=None)
    article_text = news[0]
    article_text = article_text.fillna('')

    # Run TFIDF
    print('Running TFIDF')
    vectorizer = TfidfVectorizer(stop_words='english')
    X = vectorizer.fit_transform(article_text)

    # Save Components
    print('Saving Outputs')
    scipy.sparse.save_npz(pkg_resources.resource_filename('tweetsnews.data', 'tfidf_news'), X)
    pickle.dump(vectorizer.vocabulary_, open(pkg_resources.resource_filename('tweetsnews.data', 'news_vocab.p'), 'wb'))
    news.to_hdf(pkg_resources.resource_filename('tweetsnews.data', 'news.hdf'), key='df')

if __name__ == '__main__':
    process_news()
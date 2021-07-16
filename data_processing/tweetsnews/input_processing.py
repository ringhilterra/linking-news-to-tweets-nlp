import pickle
import pkg_resources
import scipy.sparse
import numpy as np
import pandas as pd


def load_tfidf_vectors():
    return scipy.sparse.load_npz(pkg_resources.resource_filename('tweetsnews.data', 'tfidf_news.npz'))


def load_vocab():
    return pickle.load(open(pkg_resources.resource_filename('tweetsnews.data', 'news_vocab.p'), 'rb'))


def load_news():
    return pd.read_hdf(pkg_resources.resource_filename('tweetsnews.data', 'news.hdf'))


def get_top_n_highest(input_string, tfidf, vocab, n=10, threshold=.5):
    tf_sum = np.zeros((tfidf.shape[0], 1))

    for w in input_string.split():
        w = w.lower()
        if w in vocab:
            idx = vocab[w]
            tf_sum += tfidf[:, idx]

    output_df = pd.DataFrame(tf_sum)[0]
    output_df = output_df[output_df > threshold]

    return output_df.sort_values(ascending=False)[:n]

if __name__ == '__main__':
    tfidf = load_tfidf_vectors()
    vocab = load_vocab()

    input_string = 'san francisco tech google scandal'
    print(get_top_n_highest(input_string, tfidf, vocab, n=50))

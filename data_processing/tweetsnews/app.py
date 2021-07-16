from flask import Flask, render_template, jsonify, request
from project.tweetsnews.input_processing import get_top_n_highest, load_vocab, load_tfidf_vectors, load_news

app = Flask(__name__)

vocab = load_vocab()
tfidf = load_tfidf_vectors()
news = load_news()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/lookup', methods=['POST'])
def lookup():
    input_string = request.form['keywords']
    output = get_top_n_highest(input_string, tfidf, vocab)
    print(output)

    result_list = []
    for i in range(len(output)):
        news_index = output.index[i]
        score = output.iloc[i]
        headline = news.loc[news_index][2]

        result_list.append({
            'news_index': int(news_index),
            'score': score,
            'headline': headline
        })

    print(result_list)
    return jsonify(result_list)

if __name__ == '__main__':
    app.run()
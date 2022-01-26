import json
import operator
import string
from collections import defaultdict
from pathlib import Path
from matplotlib import pyplot as plt
from sklearn.manifold import TSNE

import seaborn as sn
import pandas as pd
import numpy as np

# Special thanks to Jan Scholich (janscho@student.ethz.ch) for significantly contributing to the implementation of
# the Skip Gram Model as outlined below.

# Skip Gram Model Evaluation from trained model

def cosine_similarity(e_x, e_y):
    """
    Cosine similarity calculation
    :param e_x:
    :param e_y:
    :return:
    """
    vec_dot = np.dot(e_x, e_y)
    norm = np.linalg.norm(e_x) * np.linalg.norm(e_y)
    return vec_dot / norm


def preprocessing(hashtag):
    # remove punctuation
    x = hashtag.strip(string.punctuation)
    # make hashtag lowercase
    x = hashtag.lower()
    if x:
        return x

class SkipGramModelEvaluation:

    def __init__(self, embedding_size, frequencies, epochs, max_freq, min_freq, lr, test_data=None):
        self.base_path = Path(__file__).parent
        self.N = embedding_size
        self.epochs = epochs
        self.W = None
        self.hashtag_to_index = None
        self.index_to_hashtag = None
        # appears max times
        self.max_freq = max_freq
        # appears min times
        self.min_freq = min_freq
        # filter hashtags
        file_fyp_hashtags = (self.base_path / "hashtags_to_ignore.json").resolve()
        f = open(file_fyp_hashtags, )
        self.filter_hashtags = list(json.load(f).values())
        self.lr = lr
        self.posts_no_embedding = {}
        self.test_data = test_data
        print("***** Starting evaluation of SGM *****")
        self.import_model_results()
        self.example_analysis_embeddings()
        self.visualizing_hashtag_embeddings(frequencies)
        if self.test_data is not None:
            self.evaluate_test_data()
        print("***** Posts for which 0 hashtags have a pretrained embedding: ", self.posts_no_embedding)
        print("***** Evaluation of SGM completed *****")

    def import_model_results(self):
        """
        Import hashtag embedding weights, hashtag_to_index, and index_to_hashtag
        :return:
        """
        # import embedding weights
        file_embedding_weights_csv = (self.base_path / f"sgm_resources/embedding_weights_epochs{self.epochs}_lr{self.lr}.csv").resolve()
        self.W = np.genfromtxt(file_embedding_weights_csv, delimiter=',')
        np.save('embedding.npy', self.W)

        # import hashtags_to_index
        file_hashtag_to_index = (self.base_path / f"sgm_resources/sgm_hashtag_to_index_epochs{self.epochs}_lr{self.lr}.json").resolve()
        self.hashtag_to_index = json.load(open(file_hashtag_to_index, ))

        # import index_to_hashtags
        file_index_to_hashtag = (self.base_path / f"sgm_resources/sgm_index_to_hashtag_epochs{self.epochs}_lr{self.lr}.json").resolve()
        self.index_to_hashtag = json.load(open(file_index_to_hashtag, ))

    def example_analysis_embeddings(self):
        """
        Evaluating model performance based on analysis of example words.
        :return:
        """
        pairs = [
            ("cooking", "chocolate"),
            ("apple", "iphone"),
            ("covid19", "coronavirus"),
            ("beerpong", "drink"),
            ("bike", "ride"),
            ("neymar", "messi"),
        ]
        print("| x      | y       | sim(x,y)     | ")
        print("|--------|---------|--------------|")
        for x, y in pairs:
            e_x = self.W[self.hashtag_to_index[x]]
            e_y = self.W[self.hashtag_to_index[y]]
            sim = cosine_similarity(e_x, e_y)
            print("|", x, "|", y, "|", sim, "|")

        example_words = ["love", "car", "president", "monday", "green", "money", "health", "faith", "book", "france",
                         "swiss", "spring",
                         "food", "home", "law", "america"]

        print("| x      | y       | sim(x,y)     | ")
        print("|--------|---------|--------------|")

        for x in example_words:
            e_x = self.W[self.hashtag_to_index[x]]
            W_sim = np.apply_along_axis(lambda y: cosine_similarity(e_x, y), 1, self.W)
            W_sim[self.hashtag_to_index[x]] = 0
            y = self.index_to_hashtag[np.argmax(W_sim)]
            print("|", x, "|", y, "|", np.max(W_sim), "|")

    def visualizing_hashtag_embeddings(self, frequencies):
        """
        Plotting for different frequencies the hashtag embeddings resulted from the Skip-Gram model.
        :return:
        """
        labels = []
        tokens = []

        for i in range(len(self.index_to_hashtag)):
            tokens.append(self.W[i, :])
            labels.append(self.index_to_hashtag[i])

        tsne_model = TSNE(perplexity=40, n_components=2, init='pca', n_iter=2500, random_state=23)
        new_values = tsne_model.fit_transform(tokens)
        print(new_values[:3])

        for frequency in frequencies:
            # plots the 100 most frequent hashtags in 2D
            x = np.transpose(new_values[:frequency])[0]
            y = np.transpose(new_values[:frequency])[1]
            n = self.index_to_hashtag[:frequency]

            fig, ax = plt.subplots(figsize=(24, 16))
            ax.scatter(x, y)
            ax.title.set_text(f"{frequency} Most Frequent Hashtags")

            for i, txt in enumerate(n):
                ax.annotate(txt, (x[i], y[i]))
            plt.savefig(
                self.base_path / f"sgm_resources/{frequency}_mostfrequenthashtags_epochs{self.epochs}_lr{self.lr}.png",
                bbox_inches='tight')
            print(f"Chart visualizing {frequency} Most Frequent Hashtags stored for {self.epochs}.")

    def post_avg_embedding(self, post_hashtags):
        """
        calculates the average of the post's hashtags' embeddings
        expects list of hashtags
        :param post:
        :return:
        """
        in_vocab = 0
        avg_vec = np.zeros(self.N)
        for hashtag in post_hashtags:
            if hashtag in self.index_to_hashtag:
                ind_hashtag = self.hashtag_to_index[hashtag]
                avg_vec += self.W[ind_hashtag]
                in_vocab += 1
            # Todo get embedding for "unk" hashtag if hashtag not in self.index_to_hashtag
            else:  # retrieve synonym 'unk' for hashtags appearing less than min_freq
                ind_hashtag = self.hashtag_to_index['unk']
                avg_vec += self.W[ind_hashtag]
                in_vocab += 1
        if in_vocab == 0:
            # if post_id not in self.posts_no_embedding.keys():
            #     self.posts_no_embedding[post_id] = post_hashtags
            raise Exception('Post has 0 hashtags that have pretrained embeddings.')
        else:
            return avg_vec / in_vocab

    def post_sim(self, post1, post2):
        """
        calculates similarity between posts, expects two lists of hashtags
        :return:
        """
        vec1 = self.post_avg_embedding(post1)
        vec2 = self.post_avg_embedding(post2)
        return cosine_similarity(vec1, vec2)

    def feed_sim(self, feed):
        """
        calculates the average similarity between all post in the feed, expects list of lists of hashtags
        :param feed:
        :return:
        """
        pairs = 0
        avg_sim = 0
        for i in range(len(feed)):
            post1 = feed[i]
            for j in range(i + 1, len(feed)):
                post2 = feed[j]
                avg_sim += self.post_sim(post1, post2)
                pairs += 1
        return avg_sim / pairs

    def check_valid_hashtag(self, pair):
        """
        filtering hashtags
        :param pair:
        :return:
        """
        (hashtag, count) = pair
        if hashtag in self.filter_hashtags:
            return False
        elif count < self.min_freq:
            return False
        elif count > self.max_freq:
            return False
        else:
            return True

    def clean_hashtags(self, posts):
        preprocessed_posts = {}
        for post in posts.keys():
            for hashtag in posts.get(post):
                prepro_pos = preprocessing(hashtag)
                if post not in preprocessed_posts.keys():
                    preprocessed_posts[post] = [prepro_pos]
                else:
                    preprocessed_posts[post].append(prepro_pos)

        # count how often hashtags appear over all posts
        count = defaultdict(int)
        for post in list(preprocessed_posts.values()):
            for hashtag in post:
                count[hashtag] += 1

        # sort hashtags by appearance frequency
        sorted_counts = sorted(count.items(), key=operator.itemgetter(1), reverse=True)

        # filter hashtags
        filtered_hashtags = list(filter(self.check_valid_hashtag, sorted_counts))
        filtered_hashtags = [hashtag[0] for hashtag in filtered_hashtags]

        filtered_posts = {}
        for post in preprocessed_posts.keys():
            for hashtag in preprocessed_posts.get(post):
                if hashtag in filtered_hashtags:
                    if post not in filtered_posts.keys():
                        filtered_posts[post] = [hashtag]
                    else:
                        filtered_posts[post].append(hashtag)
        return filtered_posts

    def check_too_frequent_hashtags(self, hashtag):
        if hashtag in self.filter_hashtags:
            return False
        else:
            return True

    def remove_too_frequent_hashtags(self, posts):
        hashtags = []
        for post in posts.keys():
            for hashtag in posts.get(post):
                if hashtag not in hashtags:
                    hashtags.append(hashtag)

        filtered_hashtags = list(filter(self.check_too_frequent_hashtags, hashtags))

        filtered_posts = {}
        for post in posts.keys():
            for hashtag in posts.get(post):
                if hashtag in filtered_hashtags:
                    if post not in filtered_posts.keys():
                        filtered_posts[post] = [hashtag]
                    else:
                        filtered_posts[post].append(hashtag)
        return filtered_posts

    def evaluate_test_data(self):
        """
        Import and preprocess test data to then evaluate it.
        :return:
        """
        # file_path = (self.base_path / "test_data_set.csv").resolve()
        # posts = np.genfromtxt(file_path, delimiter=',', dtype=np.dtype(str), usecols=1)
        # post_ids = np.genfromtxt(file_path, delimiter=',', dtype=np.dtype(str), usecols=0)
        #
        # posts = list(self.test_data.values())
        # post_ids = list(self.test_data.keys())

        # splits the string of hashtags
        # posts = np.char.split(posts)
        # print(posts)

        filtered_posts = self.clean_hashtags(posts=self.test_data)
        posts = list(filtered_posts.values())
        post_ids = list(filtered_posts.keys())

        # Matrix of post similarities
        sim = []
        span = range(20)
        for i in span:
            sim_int = []
            for j in span:
                sim_int.append(self.post_sim(posts[i], posts[j]))
            sim.append(sim_int)
        df_cm = pd.DataFrame(sim, index=post_ids[0:len(span)],
                             columns=post_ids[0:len(span)])
        plt.figure(figsize=(24, 16))
        sn.heatmap(df_cm, annot=True)
        plt.savefig(self.base_path / f"sgm_resources/heatmap_firsttestdata_epochs{self.epochs}_lr{self.lr}.png",
                    bbox_inches='tight')
        print(f"Chart visualizing first 30 posts in heatmap stored for {self.epochs} and lr {self.lr}.")

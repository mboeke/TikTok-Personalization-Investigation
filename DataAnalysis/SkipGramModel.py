from pathlib import Path
from collections import defaultdict
from tqdm.notebook import trange, tqdm
from sklearn.preprocessing import normalize
import matplotlib.pyplot as plt
import numpy as np
import json
import operator
import string
import random

# Special thanks to Jan Scholich (janscho@student.ethz.ch) for significantly contributing to the implementation of
# the Skip Gram Model as outlined below.

def sigmoid(x):
    """
    Helper function sigmoid.
    """
    return 1 / (1 + np.exp(-x))


def preprocessing(posts):
    training_data = []
    for i in range(len(posts)):
        post = posts[i]
        # remove punctuation
        x = [hashtag.strip(string.punctuation) for hashtag in post]
        # make all hashtag lowercase
        x = [hashtag.lower() for hashtag in x]
        if x:
          training_data.append(x)
    return training_data


class SkipGramModel:

    def __init__(self, max_freq, min_freq, embedding_size, neg_sample_size, lr, epochs, training_data):
        self.base_path = Path(__file__).parent
        self.training_data = training_data
        ### set hyperparameters for creating data set ###
        # only considering hashtags that appear between 2 and 1000 times
        # appears max times
        self.max_freq = max_freq
        # appears min times
        self.min_freq = min_freq
        # filter hashtags
        file_fyp_hashtags = (self.base_path / "hashtags_to_ignore.json").resolve()
        f = open(file_fyp_hashtags, )
        self.filter_hashtags = list(json.load(f).values())
        # embedding size
        self.N = embedding_size
        # number of negative samples per positive pairs (wt,wi)
        self.K = neg_sample_size
        self.W = None
        self.W_prime = None
        self.hashtag_to_index = {}
        self.index_to_hashtag = []

        ### data loading and preprocessing ###
        print("***** Loading and preprocessing data *****")
        self.posts = None
        self.data_loading_preprocessing()
        
        ### prep data for training ###
        print("***** Preparing data for training *****")
        self.training_samples_incl_neg = None
        self.vocabulary = None
        self.prep_data_for_training()

        ### training ###
        # hyperparameters for training
        print("***** Starting training *****")
        self.lr = lr
        self.epochs = epochs
        self.epoch_losses = []
        self.step_losses = []
        self.training()
        print("Training completed")

        ### plot training loss performance ###
        print("***** Plotting training loss performance *****")
        self.plot_training_loss_performance()

        ### store model results ###
        print("***** Storing model results *****")
        self.store_data()

        print("***** Training of skip-gram model completed. *****")

    def data_loading_preprocessing(self):
        """
        Data loading and preprocessing
        :return:
        """
        # file_path = (self.base_path / "training_data_set.csv").resolve()
        # posts = np.genfromtxt(file_path, delimiter=',', dtype=np.dtype(str), usecols=1)
        # # splits the string of hashtags
        # posts = np.char.split(posts)
        # print(posts)

        self.posts = preprocessing(self.training_data)
        print("Set of hashtags for first three posts: \n", self.posts[:3])
        print("Number of posts:", len(self.posts))

    def filter_common_hashtags(self, pair):
        (hashtag, count) = pair
        if hashtag in self.filter_hashtags:
            return False
        else:
            return True

    def prep_data_for_training(self):
        """
        Prepare data for training:
        - extract vocabulary of hashtags V
        - convert corpus into indices
        - extract pair (hashtag, context (i.e. hashtags that are co-occurring with hashtag))
        - negative sampling
        :return:
        """

        # count how often hashtags appear over all posts
        count = defaultdict(int)
        for post in self.posts:
            for hashtag in post:
                count[hashtag] += 1
        # sort hashtags by appearance frequency
        sorted_counts = sorted(count.items(), key=operator.itemgetter(1), reverse=True)

        # filter hashtags
        posts_filter_for_common_hashtags = list(filter(self.filter_common_hashtags, sorted_counts))


        replaced_hashtags = []
        for pair in posts_filter_for_common_hashtags:
            (hashtag, count) = pair
            if count < self.min_freq:
                replaced_hashtags.append('unk')
            elif count > self.max_freq:
                replaced_hashtags.append('unk')
            else:
                replaced_hashtags.append(hashtag)

        self.vocabulary = replaced_hashtags

        # Assign ids and create lookup tables
        for idx, hashtag in enumerate(self.vocabulary, 0):
            self.hashtag_to_index[hashtag] = idx
            if hashtag not in self.index_to_hashtag:
                self.index_to_hashtag.append(hashtag)

        assert len(self.index_to_hashtag) == len(self.hashtag_to_index)
        print("Number of hashtag (unfiltered):", len(sorted_counts))
        print("Number of hashtag (filtered):", len(self.index_to_hashtag))

        # transforming dataset by replacing the words with their index.
        posts_index = []
        for post in self.posts:
            ids = []
            for hashtag in post:
                # only add hashtags that are in the vocabulary!!! (all others dropped)
                if hashtag in self.hashtag_to_index:
                    ids.append(self.hashtag_to_index[hashtag])
            posts_index.append(ids)
        print("First three posts represented by the indices of their hashtags:")
        print(posts_index[:3])
        print("Number of posts (after indexing them):", len(posts_index))
        print("Number of hashtags (including duplications)", sum([len(x) for x in posts_index]))

        ## Extract pair (hashtag, context)
        # initializing the training samples (an array containing one array per hashtag in the vocabulary with all context-word-pairs)
        training_samples = [[]] * len(self.index_to_hashtag)

        # used for descriptive statistics (to check that it works)
        count = 0
        counts = []

        # iterate through all posts
        for post in tqdm(posts_index):
            interim_count = 0
            # iterate through all hashtags of a post
            for i in range(len(post)):
                hashtag = post[i]
                # iterate through the context of that hashtag
                for j in range(0, len(post)):
                    if j != i:
                        interim_count += 1
                        context_hashtag = post[j]

                        # add context-hashtag-pair to the training samples
                        if len(training_samples[hashtag]) == 0:
                            training_samples[hashtag] = [(hashtag, context_hashtag)]
                        else:
                            training_samples[hashtag].append((hashtag, context_hashtag))
            count += interim_count
            counts.append(interim_count)
        # displays the number of context-hashtag-pairs per post as histogram
        fig = plt.figure()
        plt.hist(counts)
        fig.suptitle("Histogram of the number of training samples/context-hashtag-pairs per post:")
        plt.xlabel('Number of training samples')
        plt.ylabel('Number of posts')
        plt.show()

        # Total number of context-word-pairs (training samples)
        print("Number of training samples/context-hashtag-pairs:", sum([len(x) for x in training_samples]))
        print("Manual count of training samples to validate:", count)

        # Negative Sampling
        # initialize array to capture training samples
        self.training_samples_incl_neg = [[]] * len(self.index_to_hashtag)

        # filter out all hashtags from the posts that are not in the vocabulary to get the frequency of all hashtags appearing in the corpus
        all_hashtag_rep = list(filter(lambda x: x in self.index_to_hashtag, [inner for outer in self.posts for inner in outer]))

        # iterate through the array of arrays with the context-hashtag-pairs (training samples)
        for hashtag_samples_ind in tqdm(range(len(training_samples))):
            hashtag_pairs_and_neg = []
            # iterate through the array with the context-hashtag-pairs (done for each word in the vocabulary)
            for sample in training_samples[hashtag_samples_ind]:
                neg_samples = []
                # repeat for K negative samples
                for i in range(self.K):
                    same_as_context = True
                    # while the randomly chosen sample (by choosing a random hashtag in the filtered set of all posts) is equal to the context hashtag,
                    # we choose a new one, else we add it to the list of negative samples.
                    while same_as_context:
                        neg = all_hashtag_rep[random.randint(0, len(all_hashtag_rep) - 1)]
                        neg_ind = self.hashtag_to_index[neg]
                        same_as_context = neg_ind == sample[1]
                    neg_samples.append(self.hashtag_to_index[neg])
                # create a tuple (w_i, w_t, C) where C = [(w_0^-, ..., w_20^-)] for every context-hashtag-pair
                hashtag_pairs_and_neg.append(sample + (neg_samples,))
            self.training_samples_incl_neg[hashtag_samples_ind] = hashtag_pairs_and_neg

    def training(self):
        """
        Training the model based on extracted and preprocessed training data and defined parameters.
        # Learning: calculate gradient, set training parameters, train
        Plot training performance.
        :return:
        """
        # training
        np.random.seed(42)
        random.seed(42)
        
        # vectorization of the training samples
        vectorized_training_samples = [inner for outer in self.training_samples_incl_neg for inner in outer]
        
        # initialization of weights to be between -0.8 and 0.8
        self.W = np.random.rand(len(self.vocabulary), self.N).astype(np.float128)
        self.W_prime = np.random.rand(self.N, len(self.vocabulary)).astype(np.float128)
        self.W = (2 * self.W - 1) * 0.8
        self.W_prime = (2 * self.W_prime - 1) * 0.8

        # normalize vectors to mitigate difference of vector length and only have difference of vector angle
        self.W = normalize(self.W, axis=1, norm='l2')
        
        # iterate through the number of epochs
        for i in range(self.epochs):
            print("Epoch", i + 1)
            epoch_loss = 0
            count = 0

            # shuffle training samples to make model more robust
            random.shuffle(vectorized_training_samples)

            t = tqdm(vectorized_training_samples, desc="loss: {:.4f}".format(epoch_loss))

            # iterate through all samples of the training set
            for sample in t:
                wi = sample[0]
                wt = sample[1]
                C_minus = sample[2]
        
                # get the embedding of the hashtag and the context hashtags
                e_wi = self.W[wi]
                e_wt = self.W_prime[:, wt]
        
                # temporary variable to sum up the product between the embedding of the hashtag
                # and the sigmoid of the dot product of the embedding of the context hashtags and the hashtag
                s = 0
                # temporary variable to sum up the step loss
                step_loss = 0
        
                # iterate through negative samples
                for wm in C_minus:
                    # get embedding of the negative sample
                    e_wm = self.W_prime[:, wm]
                    # update the weight of the context matrix for the (negative) sampled hashtag using GD
                    # TODO remove "(i+1)", dividing by to reduce loss even stronger which may result that loss diverges again
                    # TODO check how it influences loss using / not using it
                    self.W_prime[:, wm] = e_wm - self.lr/(i+1) * sigmoid(np.dot(e_wi, e_wm)) * e_wi
                    # add to the temporary variable as described above
                    s += sigmoid(np.dot(e_wi, e_wm)) * e_wm
                    # add to the step loss
                    t_step_loss = 1 - sigmoid(np.dot(e_wi, e_wm))
                    # case distinction for numerical stability
                    if t_step_loss <= 0:
                        step_loss -= np.log(10 ** -10)
                    else:
                        step_loss -= np.log(t_step_loss)
        
                # update weights of the hashtag embeddings
                self.W[wi] = e_wi - self.lr/(i+1) * ((sigmoid(np.dot(e_wi, e_wt)) - 1) * e_wt + s)
        
                # update weights of the context hashtag
                self.W_prime[:, wt] = e_wt - self.lr/(i+1) * (sigmoid(np.dot(e_wi, e_wt)) - 1) * e_wi
        
                # add to step loss
                step_loss -= np.log(sigmoid(np.dot(e_wi, e_wt)))
                epoch_loss += step_loss
                self.step_losses.append(step_loss)
        
                # for bookkeeping and updating loss
                count += 1
                if epoch_loss / count == np.inf:
                    print(count)
                if count % 1000 == 0:
                    t.set_description("loss: {:.8f}".format(epoch_loss / count))
                    t.refresh()

            # normalize updated weights
            self.W = normalize(self.W, axis=1, norm='l2')

            epoch_loss = epoch_loss / len(vectorized_training_samples)
            print("Loss", epoch_loss)
            self.epoch_losses.append(epoch_loss)
        
    def plot_training_loss_performance(self):
        """
        Visualizing the loss performance of the current training session. 
        :return: 
        """
        # Plot loss during the epochs
        fig = plt.figure()
        plt.plot(range(self.epochs), self.epoch_losses)
        fig.suptitle("Loss progression")
        plt.xlabel('Epochs')
        plt.ylabel('Loss')
        plt.savefig(self.base_path / f"sgm_resources/lossprogressionepochs_epochs{self.epochs}_lr{self.lr}.png",
                    bbox_inches='tight')

        # plot losses during the individual context-word-negative-sample-tuples
        fig = plt.figure()
        plt.plot(range(len(self.step_losses)), self.step_losses)
        fig.suptitle("Loss progression")
        plt.xlabel('Training steps')
        plt.ylabel('Loss')
        plt.show()
        
        # plot loss averaged over 1000 successive context-word-negative-sample-tuples
        step_losses_thousands = []
        sum = 0
        for i in range(len(self.step_losses)):
            sum += self.step_losses[i]
            if i % 1000 == 0:
                step_losses_thousands.append(sum / 1000)
                sum = 0
        
        fig = plt.figure()
        plt.plot(range(len(step_losses_thousands)), step_losses_thousands)
        fig.suptitle("Loss progression")
        plt.xlabel('Training steps in 1000s')
        plt.ylabel('Loss')
        plt.show()

    def store_data(self):
        """
        Store results from model training.
        :param W:
        :param self.hashtag_to_index:
        :param self.index_to_hashtag:
        :return:
        """

        # saving embedding weights in csv file
        print(self.W)
        file_embedding_weights_csv = (self.base_path / f"sgm_resources/embedding_weights_epochs{self.epochs}_lr{self.lr}.csv").resolve()
        np.savetxt(file_embedding_weights_csv, self.W, delimiter=',')
    
        # self.hashtag_to_index
        file_hashtag_to_index = (self.base_path / f"sgm_resources/sgm_hashtag_to_index_epochs{self.epochs}_lr{self.lr}.json").resolve()
        with open(file_hashtag_to_index, 'w') as f:
            json.dump(self.hashtag_to_index, f)

        # self.index_to_hashtag
        file_index_to_hashtag = (self.base_path / f"sgm_resources/sgm_index_to_hashtag_epochs{self.epochs}_lr{self.lr}.json").resolve()
        with open(file_index_to_hashtag, 'w') as f:
            json.dump(self.index_to_hashtag, f)

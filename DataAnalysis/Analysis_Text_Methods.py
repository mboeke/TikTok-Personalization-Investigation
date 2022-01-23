import csv

from gensim.models.doc2vec import Doc2Vec, TaggedDocument

from Analysis_Methods import *
from SkipGramModelEvaluation import *



base_path = Path(__file__).parent


def instance_db():
    file_path = (base_path / "../utilities/db_credentials.json").resolve()
    with open(file_path) as file:
        db_credentials = json.load(file)

    conn = psycopg2.connect(
        host=db_credentials.get('host'),
        database=db_credentials.get('database'),
        user=db_credentials.get('user'),
        password=db_credentials.get('password'))
    cur = conn.cursor()
    return conn, cur


conn, cur = instance_db()


def compute_similarity_within_feed(text_set):
    """
    Compute the similarity of texts from posts of the same feed.
    source:
    https://towardsdatascience.com/calculating-string-similarity-in-python-276e18a7d33a
    https://rare-technologies.com/word2vec-tutorial/
    https://towardsdatascience.com/calculating-document-similarities-using-bert-and-other-models-b2c1a29c9630
    :param text_set: dictionary of structure {"post_id": {text, language}}
    :return:
    """
    pd.set_option('display.max_colwidth', 0)
    pd.set_option('display.max_columns', 0)

    text_corpus_df = pd.DataFrame(columns=['post_id', 'text_corpus', 'text_corpus_cleaned'])
    text_corpus_df['post_id'] = text_set.keys()
    text_corpus_df['text_corpus'] = [text_set[i]['desc'] for i in text_set.keys()]

    # cleaning data: removing special characters & emojis
    text_corpus_df['text_corpus_cleaned'] = [clean_string(text_set[key]['desc'], text_set[key]['lang'],
                                                          text_set[key]['already_translated'])
                                             for key in text_set.keys()]

    # delete value from text list if after cleaning empty text remains
    nan = float("NaN")
    text_corpus_df.replace("", nan, inplace=True)
    text_corpus_df.dropna(subset=['text_corpus_cleaned'], inplace=True)

    # computing similarities using Doc2Vec cosine similarity and differences using Doc2Vec euclidean distance
    tf_idf_vectoriser = TfidfVectorizer()
    tf_idf_vectoriser.fit(text_corpus_df.text_corpus_cleaned)
    tf_idf_vectors = tf_idf_vectoriser.transform(text_corpus_df.text_corpus_cleaned)

    # download_package('punkt')

    tagged_data = [TaggedDocument(words=word_tokenize(doc), tags=[i]) for i, doc in
                   enumerate(text_corpus_df.text_corpus_cleaned)]
    model_d2v = Doc2Vec(vector_size=100, alpha=0.025, min_count=1)

    model_d2v.build_vocab(tagged_data)

    for epoch in range(100):
        model_d2v.train(tagged_data,
                        total_examples=model_d2v.corpus_count,
                        epochs=model_d2v.epochs)

    document_embeddings = np.zeros((text_corpus_df.shape[0], 100))

    for i in range(len(document_embeddings)):
        document_embeddings[i] = model_d2v.docvecs[i]

    pairwise_similarities = cosine_similarity(document_embeddings)
    pairwise_differences = euclidean_distances(document_embeddings)

    np.set_printoptions(threshold=sys.maxsize)

    return text_corpus_df, pairwise_similarities, pairwise_differences


def generate_similarities_differences(test_user_pair, test_runs, action_type, hashtags=False, description=False,
                                      within_feed=False, within_test_run=False):
    """
    Retrieve relevant data to compute similarity:
    - for posts within a feed itself
    - of posts from two feeds
    :param test_user_pair:
    :param test_runs:
    :param within_feed:
    :param within_test_run:
    :return:
    """
    relevant_data = {}
    text_source = ''
    scope = ''
    if description:
        relevant_data = retrieve_description(test_user_pair, test_runs)
        text_source = 'descriptions'
    if hashtags:
        relevant_data = retrieve_hashtags(test_user_pair, test_runs)
        text_source = 'hashtags'

    print(f"*** DATA RETRIEVED FOR TEXT SOURCE: {text_source}")
    print(relevant_data)

    test_run_feed_similarities_differences = {}

    # Compute the similarity & difference of hashtags of posts within the same feed
    if within_feed:
        for user in test_user_pair:
            test_run_feed_similarities_differences[user] = {}
            for run in test_runs:
                # compute similarities and differences
                text_corpus_df, pairwise_similarities, pairwise_differences = \
                    compute_similarity_within_feed(relevant_data[user][run])

                # structure results from similarity & difference computation
                post_similarities_differences, feed_similarity_differences = \
                    structuring_similarities_differences(text_corpus_df, pairwise_similarities, pairwise_differences)
                test_run_feed_similarities_differences[user][run] = {
                    'feed(s)_similarity': feed_similarity_differences.get('avg_sim_entire_feed'),
                    'feed(s)_difference': feed_similarity_differences.get('avg_diff_entire_feed')
                }
        print("*** COMPUTED SIMILARITIES & DIFFERENCES WITHIN FEED")
        print(test_run_feed_similarities_differences)
        scope = 'within_feed'
        plot_similarities_differences(test_run_feed_similarities_differences, text_source, scope, action_type)

    # Compute the similarity & difference of hashtags of posts from a feed across multiple testruns
    test_run_two_feed_similarities_differences = {}
    if len(test_user_pair) == 2 and within_test_run:
        for run in test_runs:
            text_sets_similarities_differences = compute_similarity_between_two_feeds(
                relevant_data[test_user_pair[0]][run], relevant_data[test_user_pair[1]][run])
            all_sim = [text_sets_similarities_differences.get(item).get('feed_similarity_to_other_feed') for item in
                       text_sets_similarities_differences.keys()]
            all_diff = [text_sets_similarities_differences.get(item).get('feed_difference_to_other_feed') for item in
                        text_sets_similarities_differences.keys()]
            test_run_two_feed_similarities_differences[run] = {
                'feed(s)_similarity': sum(all_sim) / len(all_sim),
                'feed(s)_difference': sum(all_diff) / len(all_diff)
            }
        scope = 'within_test_run'
        print("*** COMPUTED SIMILARITIES & DIFFERENCES BETWEEN FEEDS ACROSS ALL TEST RUNS")
        print(test_run_two_feed_similarities_differences)
        plot_similarities_differences(test_run_two_feed_similarities_differences, text_source, scope, action_type)


def compute_similarity_between_two_feeds(text_set_1, text_set_2):
    """
    - either twice the same set of text's as similarity of posts within the same feed shall be computed
    - or different set of text's from two different users as similarity of both users' feeds shall be evaluated
    :return:
    """
    different_text_sets = {}
    text_sets = {'text_set_1': text_set_1, 'text_set_2': text_set_2}
    text_sets_similarities_differences = {}

    for text_set in text_sets.keys():
        different_text_sets[text_set] = {}
        temp_post_sim_diff_to_other_feed = {}
        for post_user_1 in text_sets.get(text_set).keys():
            different_text_sets[text_set][post_user_1] = {
                f"{post_user_1}": text_sets.get(text_set).get(post_user_1),
            }
            other_text_set = [text_sets.get(x) for x in text_sets.keys() if x != text_set][0]
            for post_from_text_2 in other_text_set.keys():
                different_text_sets[text_set][post_user_1][f"Compared_To_{post_from_text_2}"] = {
                    'desc': other_text_set.get(post_from_text_2).get('desc'),
                    'lang': other_text_set.get(post_from_text_2).get('lang'),
                    'already_translated': other_text_set.get(post_from_text_2).get('already_translated')
                }
            text_corpus_df, pairwise_similarities, pairwise_differences = \
                compute_similarity_within_feed(different_text_sets[text_set][post_user_1])
            post_similarities_differences, feed_similarity_differences = \
                structuring_similarities_differences(text_corpus_df, pairwise_similarities, pairwise_differences)
            temp_post_sim_diff_to_other_feed[post_user_1] = {
                'feed_similarity': feed_similarity_differences.get('avg_sim_entire_feed'),
                'feed_difference': feed_similarity_differences.get('avg_diff_entire_feed')
            }
        all_sim = [temp_post_sim_diff_to_other_feed.get(item).get('feed_similarity') for item in temp_post_sim_diff_to_other_feed.keys()]
        all_diff = [temp_post_sim_diff_to_other_feed.get(item).get('feed_difference') for item in temp_post_sim_diff_to_other_feed.keys()]
        text_sets_similarities_differences[text_set] = {
            'feed_similarity_to_other_feed': sum(all_sim) / len(all_sim),
            'feed_difference_to_other_feed': sum(all_diff) / len(all_diff)
        }

    return text_sets_similarities_differences


def get_training_data(test_hashtags):
    """
    Create list of list of hashtags for all posts that shall be used in the training_data set
    :return:
    """
    sql_hashtags_training = """select distinct p.id, phr.translation_english
        from (select phr1.postid, h.id, h.translation_english
                from d1rpgcvqcran0q.public.post_hashtag_relation phr1 join 
                    d1rpgcvqcran0q.public.hashtags h on phr1.hashtagid = h.id) phr join
        (select id from d1rpgcvqcran0q.public.posts where testuserid not in (53, 54, 91, 92, 123, 124)) p on p.id = phr.postid"""

    # retrieve training hashtag data
    training_hashtags_dict = {}
    # for training data set
    cur.execute(sql_hashtags_training, ())
    results_training = cur.fetchall()
    training_hashtags = []
    for item in results_training:
        post_id = item[0]
        cur_hashtag = item[1].strip()
        if cur_hashtag != '' and post_id not in test_hashtags.keys():
            if post_id in training_hashtags_dict.keys() and cur_hashtag not in training_hashtags_dict.get(post_id):
                training_hashtags_dict[post_id].append(cur_hashtag)
            else:
                training_hashtags_dict[post_id] = [cur_hashtag]
            if cur_hashtag not in training_hashtags:
                training_hashtags.append(cur_hashtag)

    for post in list(test_hashtags.keys()):
        for hashtag in list(test_hashtags.get(post)):
            if hashtag not in training_hashtags:
                if post not in training_hashtags_dict.keys():
                    training_hashtags_dict[post] = [hashtag]
                else:
                    training_hashtags_dict[post].append(hashtag)
        if post in training_hashtags_dict.keys():
            del test_hashtags[post]

    # store training hashtag data
    file_training_data_set = (base_path / "training_data_set.csv").resolve()
    with open(file_training_data_set, 'w') as f:
        w = csv.writer(f)
        for row in training_hashtags_dict.items():
            w.writerow(row)

    return list(training_hashtags_dict.values())


def get_test_data(test_users):
    sql_hashtags_test = """select distinct p.id, phr.translation_english
        from (select phr1.postid, h.id, h.translation_english
                from d1rpgcvqcran0q.public.post_hashtag_relation phr1 join 
                    d1rpgcvqcran0q.public.hashtags h on phr1.hashtagid = h.id) phr join
        (select id from d1rpgcvqcran0q.public.posts where testuserid = %s) p on p.id = phr.postid"""

    # retrieve test hashtag data from database
    test_hashtags = {}
    for user in test_users:
        # form test data set
        cur.execute(sql_hashtags_test, (user,))
        results_test = cur.fetchall()
        for item in results_test:
            post_id = item[0]
            cur_hashtag = item[1].strip()
            if cur_hashtag != '':
                if post_id in test_hashtags.keys() and cur_hashtag not in test_hashtags.get(post_id):
                    test_hashtags[post_id].append(cur_hashtag)
                else:
                    test_hashtags[post_id] = [cur_hashtag]

    # store test hashtag data
    file_test_data_set = (base_path / "test_data_set.csv").resolve()
    with open(file_test_data_set, 'w') as f:
        w = csv.writer(f)
        for row in test_hashtags.items():
            w.writerow(row)

    return list(test_hashtags.values()), test_hashtags


def adjust_data_structure(dict):
    # restructure relevant data
    adjusted_data = {}
    for item in dict.keys():
        adjusted_data[item] = dict.get(item).get('desc')
    return adjusted_data


def visualize_similarities(test_user_pair, test_runs, action_type, skipgrammodelevaluation, epochs, lr,
                           within_feed=False, thesis_chart=False):
    """
    Visualize the similarities of the feeds of each user for every test run, both graphs in one subplot
    Visualize in another subplot the similarities of two feeds for every test run
    :return:
    """
    # use function feed_sim() from SkipGramModelEvaluation to compute similarity of specific list of hashtags
    # this list either contains only hashtags from one feed --> measuring similarity within a feed
    # or hashtags from two feeds --> measuring similarity between two feeds
    # perhaps shuffle list of hashtags before computing similarity

    description_data, hashtag_data = retrieve_hashtags(test_user_pair, test_runs)
    text_source = 'hashtags'

    print(f"*** DATA RETRIEVED FOR TEXT SOURCE: {text_source}")
    # print(hashtag_data)

    # Compute the similarity & difference of hashtags of posts within the same feed
    user_feed_similarities = {}
    for user in test_user_pair:
        user_feed_similarities[user] = {}
        for run in test_runs:
            filtered_posts = skipgrammodelevaluation.remove_too_frequent_hashtags(
                adjust_data_structure(hashtag_data[user][run]))
            posts = list(filtered_posts.values())
            post_ids = list(filtered_posts.keys())
            # compute similarities using SkipGramModel
            user_feed_similarities[user][run] = round(skipgrammodelevaluation.feed_sim(posts), 4)
    print("*** COMPUTED SIMILARITIES WITHIN FEED")
    print(user_feed_similarities)

    users_similarities = {}
    for run in test_runs:
        user_1_hashtags = adjust_data_structure(hashtag_data[test_user_pair[0]][run])
        user_2_hashtags = adjust_data_structure(hashtag_data[test_user_pair[1]][run])
        user_1_filtered_hashtags = skipgrammodelevaluation.remove_too_frequent_hashtags(user_1_hashtags)
        user_2_filtered_hashtags = skipgrammodelevaluation.remove_too_frequent_hashtags(user_2_hashtags)
        posts = list(user_1_filtered_hashtags.values()) + list(user_2_filtered_hashtags.values())
        # compute similarities using SkipGramModel
        users_similarities[run] = round(skipgrammodelevaluation.feed_sim(posts), 4)
    print("*** COMPUTED SIMILARITIES BETWEEN TWO FEEDS")
    print(users_similarities)

    plot_similarities_differences(user_feed_similarities, users_similarities, text_source, action_type, epochs, lr,
                                  within_feed, thesis_chart)

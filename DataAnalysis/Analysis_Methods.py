import itertools
import sys
from random import randint

import requests
from distinctipy import distinctipy
from langdetect import detect
from matplotlib import cm
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics.pairwise import euclidean_distances
from nltk.tokenize import word_tokenize
from pathlib import Path
from nltk.corpus import stopwords
from textblob import TextBlob

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import numdifftools as nd
import nltk
import ssl
import re
import psycopg2
import json
import urllib

base_path = Path(__file__).parent

# Defining the hashtags to exclude from analysis as they appear to frequently and thus loose their meaning
file_fyp_hashtags = (Path(__file__).parent / "hashtags_to_ignore.json").resolve()
f = open(file_fyp_hashtags,)
too_frequent_hashtags = json.load(f)

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


def post_overlap_analysis_4_users(test_run_id, test_user_ids):
    sql1 = """select count(id) from d1rpgcvqcran0q.public.posts where testrunid = %s and testuserid = %s"""
    print("Total posts for test user: \n")
    for user in test_user_ids:
        cur.execute(sql1, (test_run_id, user,))
        print(f"For user id {user}, total posts {cur.fetchall()[0][0]}\n")

    sql2 = """with interesting_posts as (
        select * from d1rpgcvqcran0q.public.posts where testrunid = %s
        ), testuser4 as (
        select * from interesting_posts where testuserid = %s
        ),
         testuser5 as (
             select * from interesting_posts where testuserid = %s
         ),
         testuser8 as (
             select * from interesting_posts where testuserid = %s
         ),
         testuser9 as (
             select * from interesting_posts where testuserid = %s
         ),
         t8t9 as (
             select distinct t9.id
             from testuser9 t9
                      join testuser8 t8 on t9.id = t8.id
             where t9.testuserid != t8.testuserid
         ),
         t4t5 as (
             select distinct t4.id
             from testuser4 t4
                      join testuser5 t5 on t4.id = t5.id
             where t4.testuserid != t5.testuserid
         )
    select count(t4t5.id) from t4t5 join t8t9 on t4t5.id = t8t9.id"""
    cur.execute(sql2, (test_run_id, test_user_ids[0], test_user_ids[1], test_user_ids[2], test_user_ids[3],))
    # print(f"Common posts for {test_run_id}: {cur.fetchall()[0][0]}\n")


def post_overlap_analysis_2_users(test_run_id, test_user_ids):
    """
    Retrieves the number of total posts viewed per user, and the overlapping posts for a test run.
    :param test_run_id:
    :param test_user_ids:
    :return: total_posts_user = {user: total_posts_viewed}, overlapping_posts = number_of_overlapping_posts
    """
    sql1 = """select count(id) from d1rpgcvqcran0q.public.posts where testrunid = %s and testuserid = %s"""
    # print("Total posts for test user: \n")
    total_posts_user = {}
    for user in test_user_ids:
        cur.execute(sql1, (test_run_id, user,))
        total_posts_result = cur.fetchall()[0][0]
        total_posts_user[user] = total_posts_result
        # print(f"For user id {user}, total posts {total_posts_result}\n")

    sql2 = """with interesting_posts as (
    select * from d1rpgcvqcran0q.public.posts where testrunid = %s), 
    testuser1 as (select * from interesting_posts where testuserid = %s),
    testuser2 as (select * from interesting_posts where testuserid = %s),
    t1t2 as (
         select distinct t1.id
         from testuser1 t1 join testuser2 t2 on t1.id = t2.id
         where t1.testuserid != t2.testuserid
     ) select count(*) from t1t2;"""
    cur.execute(sql2, (test_run_id, test_user_ids[0], test_user_ids[1],))
    overlapping_posts = cur.fetchall()[0][0]
    # print(f"Common posts for {test_run_id}: {overlapping_posts}\n")

    return total_posts_user, overlapping_posts


def liked_posts(test_user_ids, test_run_id):
    sql_liked_posts = """select count(distinct post_id), testuser_id, testrun_id from liked_post 
    where testrun_id = %s and liked = True group by testuser_id, testrun_id"""
    cur.execute(sql_liked_posts, (test_run_id,))
    sql_liked_posts_result = cur.fetchall()
    result_dic = {}
    for item in sql_liked_posts_result:
        result_dic[item[1]] = item[0]
    user_liked_posts = {}
    for user in test_user_ids:
        if user in list(result_dic.keys()):  # if user liked
            user_liked_posts[user] = result_dic.get(user)
        else:  # if user did not like
            user_liked_posts[user] = 0
    return user_liked_posts


def get_number_of_content_creators_followed(test_user_ids, test_run_id):
    """
    Retrieve the number of content creators each test user involved in the test run has started to follow.
    :param test_run_id:
    :return:
    """
    sql_content_creators_followed = """select count(distinct author_id), test_user_id, test_run_id from author_followed
    where test_run_id = %s and followed = True group by test_user_id, test_run_id"""
    cur.execute(sql_content_creators_followed, (test_run_id,))
    sql_follow_posts_result = cur.fetchall()
    result_dic = {}
    for item in sql_follow_posts_result:
        result_dic[item[1]] = item[0]
    user_following = {}
    for user in test_user_ids:
        if user in list(result_dic.keys()):  # if user followed
            user_following[user] = result_dic.get(user)
        else:  # if user did not follow
            user_following[user] = 0
    return user_following


def get_number_of_posts_watched_longer(test_user_ids, test_run_id):
    """
    Retrieve the number of posts each test user has watched longer than other posts.
    :param test_user_ids:
    :param test_run_id:
    :return:
    """
    sql_posts_watched_longer = """select count(distinct post_id), test_user_id, test_run_id from longer_watched_posts 
    where test_run_id = %s group by test_user_id, test_run_id"""
    cur.execute(sql_posts_watched_longer, (test_run_id,))
    sql_posts_watched_longer_result = cur.fetchall()
    result_dic = {}
    for item in sql_posts_watched_longer_result:
        result_dic[item[1]] = item[0]
    user_watched_post_longer = {}
    for user in test_user_ids:
        if user in list(result_dic.keys()):  # if user watched posts longer than others
            user_watched_post_longer[user] = result_dic.get(user)
        else:  # if user did not watch posts longer than others
            user_watched_post_longer[user] = 0
    return user_watched_post_longer


def test_run_time_per_user(test_run_id):
    """
    Retrieves the time in minutes it took each user of a test run to complete the test.
    :param test_run_id:
    :return: test_run_time_per_user = {user: test_run_time_for_that_user}
    """
    sql4 = """select testuserid, duration from d1rpgcvqcran0q.public.testrun where id = %s;"""
    cur.execute(sql4, (test_run_id,))
    durations = cur.fetchall()
    test_run_time_per_user = {}
    for duration in durations:
        # print(f"Test run with user {duration[0]} took {str(duration[1])} minutes.")
        test_run_time_per_user[duration[0]] = duration[1]
    return test_run_time_per_user


def overlapping_posts_per_batch(test_run_id, test_user_ids):
    sql_max_batch = """select batch_position from d1rpgcvqcran0q.public.posts where testrunid = %s order by batch_position desc limit 1"""
    cur.execute(sql_max_batch, (test_run_id,))
    sql_max_batch_result = cur.fetchall()[0][0]

    sql_batch_overlap = """select count(t1.id)
    from (select * from d1rpgcvqcran0q.public.posts where testrunid = %s and testuserid = %s) t1 join
     (select * from d1rpgcvqcran0q.public.posts where testrunid = %s and testuserid = %s) t2 
     on t1.id = t2.id and t1.testuserid != t2.testuserid
     where t1.batch_position = (%s)"""

    for user in test_user_ids:
        results = []
        user1 = user
        if test_user_ids.index(user) == 0:
            user2 = test_user_ids[1]
        else:
            user2 = test_user_ids[0]
        for batch in range(0, sql_max_batch_result + 1):
            cur.execute(sql_batch_overlap, (test_run_id, user1, test_run_id, user2, batch,))
            results.append(cur.fetchall()[0][0])
        # print(f"In {test_run_id} for user {user1} the batches contained overlaps: {results}")


def check_if_all_posts_in_db_stored(file_path, test_run_id, test_user_id):
    with open(file_path, "r") as file:
        results = json.load(file)

    # check if number of posts mentioned in console log equal to number of posts stored in db
    number_of_posts_console_log = len(results.keys())
    sql6 = """select testuserid, count(id) from d1rpgcvqcran0q.public.posts where testrunid = %s and testuserid = %s group by testuserid"""
    cur.execute(sql6, (test_run_id, test_user_id,))
    result_sql6 = cur.fetchall()
    number_posts_collected = result_sql6[0][1]
    number_of_posts_console_log += 1
    if number_of_posts_console_log != number_posts_collected:
        missing_posts = []
        for post in results:
            sql7 = """select id from d1rpgcvqcran0q.public.posts where id = %s and testrunid = %s and testuserid = %s"""
            cur.execute(sql7, (post, test_run_id, test_user_id))
            result_sql7 = cur.fetchall()
            if len(result_sql7) == 0:
                missing_posts.append(post)
        # print(f"For test user {test_user_id} the following posts were collected in test run {test_run_id} but not"
        #       f" stored (total {len(missing_posts)} posts): {missing_posts}")
    # else:
    # print(f"For test user {test_user_id} {number_posts_collected} posts were collected as stated by the console "
    #       f"log: {number_of_posts_console_log}.")


def last_proxy_access(test_run_id):
    """
    Retrieves the proxy addresses used in a test run and identifies when those proxies were used the
    last time.
    :param test_run_id:
    :return: executed_on = {user: sql_last_proxy_access_result}
    """
    sql_get_proxies = """select ip_used, testuserid from d1rpgcvqcran0q.public.testrun where id = %s"""
    cur.execute(sql_get_proxies, (test_run_id,))
    sql_get_proxies_result = cur.fetchall()
    executed_on = {}
    for address_testuser in sql_get_proxies_result:
        host_port = address_testuser[0].split(':')
        host = host_port[0]
        port = host_port[1]
        test_user_id = address_testuser[1]
        sql_last_proxy_access = """select last_usage from d1rpgcvqcran0q.public.proxies where host = %s and port = %s"""
        cur.execute(sql_last_proxy_access, (host, port,))
        sql_last_proxy_access_result = cur.fetchall()[0][0]
        # print(f"In test run {test_run_id} proxy {host}:{port} was last used on {sql_last_proxy_access_result}.")
        executed_on[test_user_id] = sql_last_proxy_access_result
    return executed_on


def get_number_of_hashtags_existing_only_for_one_user_at_test_run(test_run):
    # get number of hashtags that exist for only one of the two users
    sql_hashtags_exist_only_for_one_user = """
            with posts_hashtags as (
                select hashtagid, postid, testrunid, testuserid
                from (select * from d1rpgcvqcran0q.public.post_hashtag_relation where testrunid = %s) phr
                        join
                     (select id, testuserid from d1rpgcvqcran0q.public.posts where testrunid = %s) p on p.id = phr.postid)
            select count(distinct hashtagid) from posts_hashtags ph1
            where ph1.hashtagid not in (select ph2.hashtagid from posts_hashtags ph2 where ph2.testuserid != ph1.testuserid)
            """
    cur.execute(sql_hashtags_exist_only_for_one_user, (test_run, test_run, ))
    sql_hashtags_exist_only_for_one_user_result = cur.fetchall()[0][0]
    return sql_hashtags_exist_only_for_one_user_result


def analyse_hashtag_dev(test_runs, test_user_ids, action_type):
    """
    Plots the development of the number of different and equal hashtags between two users for a set of test runs.
    :param test_runs:
    :return:
    """
    # df = pd.DataFrame(columns=['Test_Run', 'Overlapping_Hashtags', 'Hashtags_Exist_Only_For_One_User', 'Total_Hashtags'])
    df = pd.DataFrame(columns=['Overlapping_Hashtags', 'Hashtags_Exist_Only_For_One_User'])
    for test in test_runs:
        # get number of all different hashtags for test run
        sql_all_hashtags = """select count(distinct hashtagid) from d1rpgcvqcran0q.public.post_hashtag_relation where testrunid = %s"""
        cur.execute(sql_all_hashtags, (test,))
        sql_all_hashtags_result = cur.fetchall()[0][0]

        # get number of overlapping hashtags
        sql_overlapping_hashtags = """
        with 
        posts_hashtags as (
            select hashtagid, postid, testrunid, testuserid
            from (select * from d1rpgcvqcran0q.public.post_hashtag_relation where testrunid = %s) phr
                    join
                 (select id, testuserid from d1rpgcvqcran0q.public.posts where testrunid = %s) p on p.id = phr.postid),
        
        overlapping_hashtags as (
            select distinct ph1.hashtagid, ph1.postid, ph1.testuserid from posts_hashtags ph1 join
            posts_hashtags ph2 on ph1.hashtagid = ph2.hashtagid and ph1.testuserid != ph2.testuserid)
            
        select count(distinct hashtagid) from overlapping_hashtags"""
        cur.execute(sql_overlapping_hashtags, (test, test,))
        sql_overlapping_hashtags_result = cur.fetchall()[0][0]

        sql_hashtags_exist_only_for_one_user_result = \
            get_number_of_hashtags_existing_only_for_one_user_at_test_run(test)

        values_to_add = {'Overlapping_Hashtags': sql_overlapping_hashtags_result,
                         'Hashtags_Exist_Only_For_One_User': sql_hashtags_exist_only_for_one_user_result}
        # 'Total_Hashtags': sql_all_hashtags_result
        row_to_add = pd.Series(values_to_add, name=test)
        df = df.append(row_to_add)
    appendix = ''
    for user in test_user_ids:
        appendix = appendix + "_" + str(user)
    df = df.astype(float)
    # plot regression - Overlapping_Hashtags
    d1 = np.polyfit(df.index.array, df['Overlapping_Hashtags'], 1)
    f1 = np.poly1d(d1)
    df.insert(2, 'Reg_Overlapping_Hashtags', f1(df.index.array))
    # plot regression - Hashtags_Exist_Only_For_One_User
    d2 = np.polyfit(df.index.array, df['Hashtags_Exist_Only_For_One_User'], 1)
    f2 = np.poly1d(d2)
    df.insert(3, 'Reg_Hashtags_Exist_Only_For_One_User', f2(df.index.array))
    plot = df.plot(kind='line', title=f"Hashtag Development of test user {appendix}")
    plot.set_xticks(df.index.array, minor=True)
    plot.set_xlabel("Test Runs")
    plot.set_ylabel("Number of Hashtags")
    plot.grid()
    fig = plot.get_figure()
    fig.savefig((base_path / f"Plots/{action_type}/hashtag_development{appendix}.png"))
    plt.close()
    print(f"Visualization of development of overlapping hashtags generated for {test_user_ids}.")


def get_test_run_ids_2_user(test_user_ids):
    """
    Get all test run ids for the given test user ids.
    :param test_user_ids:
    :return:
    """
    sql_test_run_ids = """select distinct id from d1rpgcvqcran0q.public.testrun 
    where (testuserid = %s or testuserid = %s) and duration is not null"""
    cur.execute(sql_test_run_ids, (test_user_ids[0], test_user_ids[1],))
    sql_test_run_ids_result = cur.fetchall()
    test_run_id_list = []
    for test in sql_test_run_ids_result:
        test_run_id_list.append(test[0])
    return test_run_id_list  # [1:len(test_run_id_list)] removing first test run to see its influence


def get_number_of_failed_testruns(test_user_ids):
    sql_failed_test_runs = """select count(distinct id) from d1rpgcvqcran0q.public.testrun
    where (testuserid = %s or testuserid = %s) and duration is null"""
    cur.execute(sql_failed_test_runs, (test_user_ids[0], test_user_ids[1],))
    sql_test_run_ids_result = cur.fetchall()[0][0]
    return sql_test_run_ids_result


def get_different_posts_noise(batch_size):
    """
    Retrieve the current computed different_posts_noise.
    :return:
    """
    # computing different_posts_noise
    sql_noise = """select overall_noise from control_group_results where batch_size = %s order by updated desc limit 1"""
    cur.execute(sql_noise, (batch_size,))
    return cur.fetchall()[0][0]


def compute_differences(test_run, test_users, comment, noise):
    """
    Compute differences of posts accounting for different_posts_noise and not accounting for it.
    :return:
    """
    total_posts_user, overlapping_posts = post_overlap_analysis_2_users(test_run, test_users)
    avg = sum(total_posts_user.values()) / len(total_posts_user.values())
    difference_user_avg_total_posts = {}
    difference_user_acc_for_noise_not_avg_total_posts = {}
    for test_user in test_users:
        try:
            difference_user_avg_total_posts[test_user] = ((total_posts_user.get(test_user) - overlapping_posts) /
                                                          avg) * 100
            difference_user_acc_for_noise_not_avg_total_posts[test_user] = \
                ((total_posts_user.get(test_user) - overlapping_posts) / total_posts_user.get(test_user) -
                 (noise)) * 100
        except ZeroDivisionError as e:
            difference_user_avg_total_posts[test_user] = 0
            difference_user_acc_for_noise_not_avg_total_posts[test_user] = 0
            comment[test_user] = "Test run seems erroneous."
            print("Error: " + str(e))
    return difference_user_avg_total_posts, difference_user_acc_for_noise_not_avg_total_posts, comment


def update_overlapping_post_test_results_with_new_values(test_run, test_users, batch_size):
    """
    Compute and add only values for given test_run and test_users.
    :param test_run:
    :param test_users:
    :return:
    """
    try:
        different_posts_noise = get_different_posts_noise(batch_size)
        total_failed_test_runs = get_number_of_failed_testruns(test_users)
        comment = {}
        for test_user in test_users:
            comment[test_user] = ''
        # executed_on = {user: sql_last_proxy_access_result}
        executed_on = last_proxy_access(test_run)

        # test run time : test_run_time_per_user = {user: test_run_time_per_user}
        test_run_time_per_user_result = test_run_time_per_user(test_run)

        # total posts viewed, overlapping posts total
        # total_posts_user = {user: total_posts_viewed}, overlapping_posts = number_of_overlapping_posts
        total_posts_user, overlapping_posts = post_overlap_analysis_2_users(test_run, test_users)

        # compute difference per test user avg total posts & difference per test user acc for different_posts_noise not avg total posts
        difference_user_avg_total_posts, difference_user_acc_for_noise_not_avg_total_posts, comment = \
            compute_differences(test_run, test_users, comment, different_posts_noise)

        # number of posts liked : user_following = {user: number_of_posts_liked}
        number_of_posts_liked = liked_posts(test_user_ids=test_users, test_run_id=test_run)

        # get number of content creators followed for test run
        number_of_content_creators_followed = get_number_of_content_creators_followed(test_user_ids=test_users,
                                                                                      test_run_id=test_run)

        # get number of posts that were watched longer
        number_of_posts_watched_longer = get_number_of_posts_watched_longer(test_user_ids=test_users,
                                                                            test_run_id=test_run)

        sql_overlapping_post_test_results = """
                insert into d1rpgcvqcran0q.public.overlapping_post_test_results(test_user_id, test_run_id, executed_on,
                test_run_time, total_posts_viewed, overlapping_posts_total, difference_user_avg_total_posts, 
                difference_user_acc_for_noise_not_avg_total_posts, number_of_posts_liked, number_of_followers, comment, 
                total_failed_testruns, number_of_posts_watched_longer) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                on conflict on constraint overlapping_post_test_results_pkey do nothing"""

        for test_user in test_users:
            cur.execute(sql_overlapping_post_test_results, (
                # test_user_id
                test_user,
                # test_run_id
                test_run,
                # executed_on
                executed_on.get(test_user),
                # test_run_time
                test_run_time_per_user_result.get(test_user),
                # total_posts_viewed
                total_posts_user.get(test_user),
                # overlapping_posts_total
                overlapping_posts,
                # difference_user_avg_total_posts
                difference_user_avg_total_posts.get(test_user),
                # difference_user_acc_for_noise_not_avg_total_posts
                difference_user_acc_for_noise_not_avg_total_posts.get(test_user),
                # number_of_posts_liked
                number_of_posts_liked.get(test_user),
                # number_of_followers
                number_of_content_creators_followed.get(test_user),
                # comment
                comment.get(test_user),
                # total failed test runs
                total_failed_test_runs,
                # number_of_posts_watched_longer
                number_of_posts_watched_longer.get(test_user)
            ))
            conn.commit()
        print(f"Overlapping post test results for test users {test_users} updated with data from test run {test_run}.")
    except (psycopg2.InterfaceError, psycopg2.OperationalError) as cursor_error:
        print(cursor_error)
        print("Re-instantiating db connection and trying to store data again.")
        instance_db()
        update_overlapping_post_test_results_with_new_values(test_run, test_users, batch_size)


def update_overlapping_post_test_results_all(test_runs, test_users, different_posts_noise):
    """
    Analyse the development of overlapping posts for a set of test runs with two test users.
    Also check on the development of overlapping posts per batch.
    Update already computed values.
    :param test_runs:
    :param test_users:
    :return:
    """
    total_failed_test_runs = get_number_of_failed_testruns(test_users)

    for test_run in test_runs:
        comment = {}
        for test_user in test_users:
            comment[test_user] = ''

        # total posts viewed, overlapping posts total
        # total_posts_user = {user: total_posts_viewed}, overlapping_posts = number_of_overlapping_posts
        total_posts_user, overlapping_posts = post_overlap_analysis_2_users(test_run, test_users)

        # compute difference per test user avg total posts & difference per test user acc for different_posts_noise not avg total posts
        difference_user_avg_total_posts, difference_user_acc_for_noise_not_avg_total_posts, comment = \
            compute_differences(test_run, test_users, comment, different_posts_noise)

        # number of posts liked : user_following = {user: number_of_posts_liked}
        number_of_posts_liked = liked_posts(test_user_ids=test_users, test_run_id=test_run)

        # get number of content creators followed for test run
        number_of_content_creators_followed = get_number_of_content_creators_followed(test_user_ids=test_users,
                                                                                      test_run_id=test_run)

        # get number of posts that were watched longer
        number_of_posts_watched_longer = get_number_of_posts_watched_longer(test_user_ids=test_users,
                                                                            test_run_id=test_run)

        sql_overlapping_post_test_results_update = """
        update d1rpgcvqcran0q.public.overlapping_post_test_results
        set total_posts_viewed = %s, overlapping_posts_total = %s, difference_user_avg_total_posts = %s, 
        difference_user_acc_for_noise_not_avg_total_posts = %s, number_of_posts_liked = %s, number_of_followers = %s,
        comment = %s, total_failed_testruns = %s, number_of_posts_watched_longer = %s
        where test_user_id  = %s and test_run_id = %s"""

        sql_overlapping_post_test_results_insert = """
                        insert into d1rpgcvqcran0q.public.overlapping_post_test_results(test_user_id, test_run_id, 
                        executed_on, test_run_time, total_posts_viewed, overlapping_posts_total, 
                        difference_user_avg_total_posts, difference_user_acc_for_noise_not_avg_total_posts, 
                        number_of_posts_liked, number_of_followers, comment, total_failed_testruns, 
                        number_of_posts_watched_longer) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                        on conflict on constraint overlapping_post_test_results_pkey do nothing"""

        for test_user in test_users:
            if check_if_data_already_exists_overlapping_post_test_results(test_user, test_run):
                cur.execute(sql_overlapping_post_test_results_update, (
                    # total_posts_viewed
                    total_posts_user.get(test_user),
                    # overlapping_posts_total
                    overlapping_posts,
                    # difference_user_avg_total_posts
                    difference_user_avg_total_posts.get(test_user),
                    # difference_user_acc_for_noise_not_avg_total_posts
                    difference_user_acc_for_noise_not_avg_total_posts.get(test_user),
                    # number_of_posts_liked
                    number_of_posts_liked.get(test_user),
                    # number_of_followers
                    number_of_content_creators_followed.get(test_user),
                    # comment
                    comment.get(test_user),
                    # total failed test runs
                    total_failed_test_runs,
                    # number_of_posts_watched_longer
                    number_of_posts_watched_longer.get(test_user),
                    # test_user_id
                    test_user,
                    # test_run_id
                    test_run
                ))
                conn.commit()
            else:
                # executed_on = {user: sql_last_proxy_access_result}
                executed_on = last_proxy_access(test_run)
                # test run time : test_run_time_per_user = {user: test_run_time_per_user}
                test_run_time_per_user_result = test_run_time_per_user(test_run)
                cur.execute(sql_overlapping_post_test_results_insert, (
                    # test_user_id
                    test_user,
                    # test_run_id
                    test_run,
                    # executed_on
                    executed_on.get(test_user),
                    # test_run_time
                    test_run_time_per_user_result.get(test_user),
                    # total_posts_viewed
                    total_posts_user.get(test_user),
                    # overlapping_posts_total
                    overlapping_posts,
                    # difference_user_avg_total_posts
                    difference_user_avg_total_posts.get(test_user),
                    # difference_user_acc_for_noise_not_avg_total_posts
                    difference_user_acc_for_noise_not_avg_total_posts.get(test_user),
                    # number_of_posts_liked
                    number_of_posts_liked.get(test_user),
                    # number_of_followers
                    number_of_content_creators_followed.get(test_user),
                    # comment
                    comment.get(test_user),
                    # total failed test runs
                    total_failed_test_runs,
                    # number_of_posts_watched_longer
                    number_of_posts_watched_longer.get(test_user)
                ))
                conn.commit()
    print(f"Overlapping post test results for test users {test_users} updated with data from test runs {test_runs}.")


def check_if_data_already_exists_overlapping_post_test_results(test_user_id, test_run_id):
    """
    Check if overlapping_post_test_results already contains entry for given test user id and test run id.
    :param test_user_id:
    :param test_run_id:
    :return:
    """
    sql_check_overlapping_post_test_results = """select test_user_id, test_run_id from overlapping_post_test_results
    where test_user_id = %s and test_run_id = %s"""
    cur.execute(sql_check_overlapping_post_test_results, (test_user_id, test_run_id,))
    results = cur.fetchall()
    if len(results) > 0:
        return True
    else:
        return False


def store_gradient(test_user, grad):
    """
    Update overlapping_post_test_results with current gradient of regression slopes.
    :param test_user:
    :param grad:
    :return:
    """
    sql_update_grad = """update d1rpgcvqcran0q.public.overlapping_post_test_results 
    set grad_regr_diff_acc_for_noise = %s where test_user_id = %s"""
    cur.execute(sql_update_grad, (grad.min(), test_user,))
    conn.commit()


def generate_charts_overlapping_post_test_results(test_runs, test_users, action_type):
    """
    Generates a graph that shows the development of overlapping posts for a set of test users and their test runs.
    :param action_type:
    :param test_runs:
    :param test_users:
    :return:
    """
    # create dataframe for each test user and set of differences
    # don't forget to create value set for x axis: test run - date & time - number of likes for user x

    # initializing dataframe correctly, we create only one dataframe to have one plot at the end
    columns = ['Test_Run_Desc']
    columns_reference = {}
    appendix = ''
    for test_user in test_users:
        appendix = appendix + "_" + str(test_user)
        columns.append(f'Diff_Acc_For_Noise_{test_user}')
        columns_reference[test_user] = f'Diff_Acc_For_Noise_{test_user}'
    df = pd.DataFrame(columns=columns)

    for test_run in test_runs:
        values_to_add = {}
        number_of_action = {}
        executed_on = []
        action = ''
        for test_user in test_users:
            # fetch data from overlapping_post_test_results
            sql_fetch_test_results = """
            select executed_on, difference_user_acc_for_noise_not_avg_total_posts, 
            number_of_posts_liked, number_of_followers, number_of_posts_watched_longer
            from d1rpgcvqcran0q.public.overlapping_post_test_results where 
            test_run_id = %s and test_user_id = %s"""
            cur.execute(sql_fetch_test_results, (test_run, test_user,))
            result = cur.fetchall()[0]

            executed_on.append(result[0])
            diff_acc_for_noise = result[1]
            number_of_action[test_user] = {'Like': result[2], 'Follow': result[3], 'WatchedLonger': result[4]}
            if number_of_action.get(test_user).get('Like') != 0 or number_of_action.get(test_user).get('Follow') != 0 \
                    or number_of_action.get(test_user).get('WatchedLonger') != 0:
                action += f"User {test_user}"
            if number_of_action.get(test_user).get('Like') != 0:
                action += f" liked {number_of_action.get(test_user).get('Like')};"
            if number_of_action.get(test_user).get('Follow') != 0:
                action += f" followed {number_of_action.get(test_user).get('Follow')};"
            if number_of_action.get(test_user).get('WatchedLonger') != 0:
                action += f" watched {number_of_action.get(test_user).get('WatchedLonger')} longer;"
            values_to_add.update({columns_reference.get(test_user): diff_acc_for_noise})
        datetime = max(executed_on)
        datetime = str(datetime.date()) + "(" + str(datetime.hour) + ":" + str(datetime.minute) + ")"

        if action == '':
            action += "No likes/follows"
        values_to_add.update({'Test_Run_Desc': str(test_run) + " - " + datetime + " - " + action})
        row_to_add = pd.Series(values_to_add, name=test_run)
        df = df.append(row_to_add)

    # creating regressions
    df.insert(0, 'Index', np.arange(len(df['Test_Run_Desc'])))
    gradients = {}
    for test_user in columns_reference.keys():
        d = np.polyfit(df['Index'], df[columns_reference.get(test_user)], 1)
        f = np.poly1d(d)
        store_gradient(test_user=test_user, grad=nd.Gradient(f)([1]))
        gradients[test_user] = round(nd.Gradient(f)([1]).min(), 4)
        df.insert(len(df.columns), f'Reg_Diff_Acc_For_Noise_{test_user}', f(df['Index']))

    # plotting graphs
    colors = ['brown', 'green', 'red', 'blue', 'green', 'blue']
    plot = df.plot(x='Index', kind='line', figsize=(15, 10), color=colors,
                   title=f"Development of Different Posts For Test User {appendix} : {action_type}")
    plot.set_xlabel("Test Runs")
    plot.set_ylabel("Difference in %")
    plot.set_xticks(list(np.arange(len(df['Test_Run_Desc']))))
    plot.set_xticklabels(list(df['Test_Run_Desc']), rotation=45, ha='right')
    plot.grid()
    plt.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
    failed_test_runs = get_number_of_failed_testruns(test_users)
    plot.text(1, 0.01, f"Total failed test runs: {failed_test_runs}\n "
                       f"Regression Slope {test_users[0]}: {gradients.get(test_users[0])} |  "
                       f"{test_users[1]}: {gradients.get(test_users[1])}", verticalalignment='bottom',
              horizontalalignment='right', transform=plot.transAxes, color='black', fontsize=15)
    plt.tight_layout()
    fig = plot.get_figure()
    fig.savefig((base_path / f"Plots/{action_type}/development_diff_posts{appendix}.png"))
    print(f"Chart visualizing of different posts completed for {test_users}.")


def generate_chart_error_rate_2_users(all_test_users, action_type):
    """
    Generate a chart that plots the error rate against the gradient of the regressions to see how the gradient changes
    (becomes positive and thus shows a correlation between liking and difference of posts on a feed).
    x-axis: error rate for test user
    y-axis: gradient of regression
    :param test_runs:
    :return:
    """
    df = pd.DataFrame(columns=['Description', 'Error_Rate_Successful_Failed_Tests',
                               'Grad_Regr_Diff_Acc_For_Noise_User1', 'Grad_Regr_Diff_Acc_For_Noise_User2'])
    for test_user_pair in all_test_users:
        row_to_add = {}
        # retrieve gradients & error rates
        sql_grad_error = """select distinct test_user_id, total_failed_testruns, grad_regr_diff_acc_for_noise
        from overlapping_post_test_results where test_user_id = %s or test_user_id = %s"""
        cur.execute(sql_grad_error, (test_user_pair[0], test_user_pair[1]))
        results = cur.fetchall()
        total_failed_testruns = results[0][1]  ### CHECK AGAIN HERE, probs not correct
        grad_regr_diff_acc_for_noise = {}
        for user in results:
            if results.index(user) == 0:
                grad_regr_diff_acc_for_noise['user1'] = user[2]
            else:
                grad_regr_diff_acc_for_noise['user2'] = user[2]
        successful_testruns = len(get_test_run_ids_2_user(test_user_pair))
        error_rate_testruns = total_failed_testruns
        df = df.append(pd.Series(({'Description': f"User {test_user_pair[0]} & {test_user_pair[1]}",
                                   'Error_Rate_Successful_Failed_Tests': error_rate_testruns,
                                   'Grad_Regr_Diff_Acc_For_Noise_User1': grad_regr_diff_acc_for_noise.get('user1'),
                                   'Grad_Regr_Diff_Acc_For_Noise_User2': grad_regr_diff_acc_for_noise.get('user2')}),
                                 name=all_test_users.index(test_user_pair)))

    # plot
    plot = df.plot(kind='line',
                   title=f"Error Rate Against Gradient Of Regression For Diff Test Scenarios {action_type}")
    plot.set_xlabel("Different Test Scenarios")
    plot.set_ylabel("Gradient For Different Test Scenarios")
    plot.set_xticks(list(np.arange(len(df['Description']))))
    plot.set_xticklabels(list(df['Description']), rotation=45, ha='right')
    plot.grid()
    plt.legend(loc='upper left')
    plt.tight_layout()
    fig = plot.get_figure()
    fig.savefig((base_path / f"Plots/{action_type}/development_error_rate_gradient.png"))
    plt.close()
    print(f"Error rate chart generated for test users {all_test_users}.")


def development_of_post_metrics(test_run_ids, test_user_pair, action_type, action_user, thesis_chart=False):
    """
    Retrieve the number of likes, shares, comments, views of all posts that were seen during a certain test run.
    :param test_run_ids:
    :param test_user_pair:
    :return:
    """
    total_avg = {}
    sql_total_avg = """select avg(likes_diggcount) avg_likes, avg(sharecount) avg_sharing, 
    avg(commentcount) avg_comments, avg(playcount) avg_views
    from posts where (testuserid = %s or testuserid = %s)"""
    cur.execute(sql_total_avg, (test_user_pair[0], test_user_pair[1]))
    results = cur.fetchall()
    if len(results) > 0:
        total_avg["Avg_Likes"] = float(results[0][0])
        total_avg["Avg_Sharing"] = float(results[0][1])
        total_avg["Avg_Comments"] = float(results[0][2])
        total_avg["Avg_Views"] = float(results[0][3])

    averages = {}
    for test_run in test_run_ids:
        temp1 = {}
        for test_user in test_user_pair:
            sql_collect_posts_metrics = """select avg(likes_diggcount) avg_likes, avg(sharecount) avg_sharing,
                avg(commentcount) avg_comments, avg(playcount) avg_views
                from posts where testuserid = %s and testrunid = %s"""
            cur.execute(sql_collect_posts_metrics, (test_user, test_run,))
            results = cur.fetchall()
            temp2 = {}
            for item in results:
                temp2[test_user] = {
                    "Avg_Likes": float(item[0]),
                    "Avg_Sharing": float(item[1]),
                    "Avg_Comments": float(item[2]),
                    "Avg_Views": float(item[3])
                }
            temp1.update(temp2)
        averages[test_run] = temp1
    print(f"Post metrics for test users {test_user_pair} retrieved.")
    plot_post_metrics(averages, test_user_pair, action_type, action_user, total_avg, thesis_chart)


def plot_post_metrics(averages, test_user_pair, action_type, action_user, total_avg, thesis_chart):
    """
    Plot retrieved post metrics.
    :param averages:
    :return:
    """
    df = pd.DataFrame(columns=['Test_Run', 'Test_User', 'Avg_Likes', 'Avg_Sharing', 'Avg_Comments', 'Avg_Views'])
    for test_run in averages.keys():
        for user in averages.get(test_run):
            df = df.append(pd.Series(({
                'Test_Run': int(test_run),
                'Test_User': int(user),
                'Avg_Likes': averages.get(test_run).get(user).get('Avg_Likes'),
                'Avg_Sharing': averages.get(test_run).get(user).get('Avg_Sharing'),
                'Avg_Comments': averages.get(test_run).get(user).get('Avg_Comments'),
                'Avg_Views': averages.get(test_run).get(user).get('Avg_Views')
            }), name=list(averages.keys()).index(test_run), dtype=object))

    appendix = ''
    for user in test_user_pair:
        appendix += '_' + str(user)

    # plotting graphs
    fig, axes = plt.subplots(figsize=(15, 15), nrows=2, ncols=2)
    metrics = list(df.columns[2:6])
    indices = {'Avg_Likes': [0, 0], 'Avg_Sharing': [0, 1], 'Avg_Comments': [1, 0], 'Avg_Views': [1, 1]}
    colors = {'Avg_Likes': ['#ff3333', '#ff9999'], 'Avg_Sharing': ['#66cc00', '#b2ff66'],
              'Avg_Comments': ['#0066cc', '#66b2ff'], 'Avg_Views': ['#606060', '#c0c0c0']}
    gradients = {}
    total_gradients = {test_user_pair[0]: 0, test_user_pair[1]: 0}
    for metric in metrics:
        grouped_metric = df[['Test_User', 'Test_Run', metric]].groupby('Test_User')
        plot_data = pd.DataFrame(columns=[f"{metric}_{test_user_pair[0]}", f"{metric}_{test_user_pair[1]}"])
        test_runs = list(averages.keys())
        for user, data in grouped_metric:
            idx = f"{metric}_{user}"
            plot_data[idx] = data[metric]
            d = np.polyfit(data.index, data[metric], 1)
            f = np.poly1d(d)
            cur_gradient = round(nd.Gradient(f)([1]).min() / total_avg[metric], 4)  # normalizing slope
            gradients[idx] = cur_gradient
            total_gradients[user] += cur_gradient
        cur_axes = axes[indices.get(metric)[0], indices.get(metric)[1]]
        plot_data.plot(ax=cur_axes, color=colors.get(metric), title=f"Development Post Metric {metric}")
        cur_axes.set_xticks(list(np.arange(len(plot_data.index))))
        cur_axes.set_xticklabels(test_runs, rotation=45)
        if not thesis_chart:
            cur_axes.text(0.5, -0.1, f"{plot_data.columns[0]}: {gradients.get(plot_data.columns[0])} | "
                                     f"{plot_data.columns[1]}: {gradients.get(plot_data.columns[1])}",
                          horizontalalignment='center', size=11, transform=cur_axes.transAxes)
        cur_axes.grid()
        cur_axes.legend()
    if not thesis_chart:
        plt.figtext(0.5, -0.01, f"Combined Metrics Gradient: {test_user_pair[0]}: "
                               f"{round(total_gradients.get(test_user_pair[0]), 4)} | {test_user_pair[1]}: "
                               f"{round(total_gradients.get(test_user_pair[1]), 4)} \n "
                               f"For above displayed data user {action_user} performed {action_type}.",
                    horizontalalignment='center', fontsize=12, fontweight='bold')
    plt.tight_layout(pad=2.5)
    fig.savefig((base_path / f"Plots/{action_type}/dev_post_metrics{appendix}.png"))
    print(f"Post metrics chart generated for test users {test_user_pair}.")


def get_test_run_ids(test_user):
    """
    Retrieve all test_run_ids for tests that were completed successfully.
    :param test_user:
    """
    sql_test_runs = """select distinct id from d1rpgcvqcran0q.public.testrun 
        where testuserid = %s and duration is not null order by id asc"""
    cur.execute(sql_test_runs, (test_user,))
    results = cur.fetchall()
    test_runs = []
    for item in results:
        test_runs.append(item[0])
    return test_runs


def overlapping_posts_for_2_users(test_user_pair, different_posts_noise):
    """
    Compare the overlapping posts of two different users.
    Computation of overlapping different_posts_noise accounts for appropriate different_posts_noise.
    :return:
    """
    if len(test_user_pair) == 2:
        test_user_1 = test_user_pair[0]
        test_user_2 = test_user_pair[1]
    else:
        raise Exception("Given users are not a pair.")
    # get all post ids for every test run of each test user
    sql_retrieve_posts = """select id from posts where testrunid=%s and testuserid=%s"""
    posts = {}
    test_runs = {}
    for test_user in test_user_pair:
        test_runs[test_user] = get_test_run_ids(test_user)
        posts[test_user] = {}
        for test_run in test_runs.get(test_user):
            cur.execute(sql_retrieve_posts, (test_run, test_user,))
            results = cur.fetchall()
            post_list = []
            for item in results:
                post_list.append(item[0])
            posts[test_user][test_run] = post_list

    # compute the number of overlapping posts for those two users for sorted test runs, in total and store information
    # in compare_locations table, store plain number and percentage
    sql_add_row_table = """insert into overlapping_posts_2_users(test_user_id_1, test_user_id_2, test_run_id_user_1,
        test_run_id_user_2, number_of_overlapping_posts, number_of_overlapping_posts_percentage, 
        number_of_overlapping_posts_acc_noise, number_of_overlapping_posts_percentage_acc_noise,
        number_of_different_posts_user_1, number_of_different_posts_user_2, number_of_different_posts_percentage_user_1,
        number_of_different_posts_percentage_user_2, total_number_of_overlapping_posts, comment,
        total_number_of_overlapping_posts_percentage, total_number_of_posts_viewed_user_1, 
        total_number_of_posts_viewed_user_2) 
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    sql_update_table = """update overlapping_posts_2_users
    set number_of_overlapping_posts = %s, number_of_overlapping_posts_percentage = %s, 
        number_of_overlapping_posts_acc_noise = %s, number_of_overlapping_posts_percentage_acc_noise = %s,
        number_of_different_posts_user_1 = %s, number_of_different_posts_user_2 = %s, 
        number_of_different_posts_percentage_user_1 = %s, number_of_different_posts_percentage_user_2 = %s, 
        total_number_of_overlapping_posts = %s, comment = %s, total_number_of_overlapping_posts_percentage = %s, 
        total_number_of_posts_viewed_user_1 = %s, total_number_of_posts_viewed_user_2 = %s
    where test_user_id_1 = %s and test_user_id_2 = %s and test_run_id_user_1  = %s and test_run_id_user_2 = %s"""

    # total overlapping posts
    all_posts = {}
    for test_user_runs in posts.keys():
        all_posts[test_user_runs] = []
        for test_run_posts in posts.get(test_user_runs):
            for post in posts.get(test_user_runs).get(test_run_posts):
                all_posts[test_user_runs].append(post)
    total_overlapping_posts = len(set(all_posts.get(test_user_1)) & set(all_posts.get(test_user_2)))

    if len(test_runs.get(test_user_1)) == len(test_runs.get(test_user_2)):
        for i in range(0, len(test_runs.get(test_user_1))):
            posts_user_1 = posts.get(test_user_1).get(test_runs.get(test_user_1)[i])
            posts_user_2 = posts.get(test_user_2).get(test_runs.get(test_user_2)[i])
            # computing the posts both users have in COMMON --> overlapping posts
            number_of_overlapping_posts = (len(set(posts_user_1) & set(posts_user_2)))
            number_of_overlapping_posts_acc_noise = (len(set(posts_user_1) & set(posts_user_2))) - different_posts_noise
            total_posts_user_1 = len(posts_user_1)
            total_posts_user_2 = len(posts_user_2)
            number_of_different_posts_user_1 = total_posts_user_1 - number_of_overlapping_posts
            number_of_different_posts_user_2 = total_posts_user_2 - number_of_overlapping_posts
            if check_if_data_already_exists_overlapping_posts_2_users(test_user_1, test_user_2,
                                                                      test_runs.get(test_user_1)[i],
                                                                      test_runs.get(test_user_2)[i]):
                cur.execute(sql_update_table, (
                    number_of_overlapping_posts,
                    number_of_overlapping_posts / ((total_posts_user_1 + total_posts_user_2) / 2),
                    number_of_overlapping_posts_acc_noise,
                    number_of_overlapping_posts_acc_noise / ((total_posts_user_1 + total_posts_user_2) / 2),
                    number_of_different_posts_user_1,
                    number_of_different_posts_user_2,
                    number_of_different_posts_user_1 / total_posts_user_1,
                    number_of_different_posts_user_2 / total_posts_user_2,
                    total_overlapping_posts,
                    '',
                    total_overlapping_posts / (len(all_posts.get(test_user_1)) + len(all_posts.get(test_user_2))),
                    total_posts_user_1,
                    total_posts_user_2,
                    test_user_1,
                    test_user_2,
                    test_runs.get(test_user_1)[i],
                    test_runs.get(test_user_2)[i],
                ))
                conn.commit()
            else:
                cur.execute(sql_add_row_table, (
                    test_user_1,
                    test_user_2,
                    test_runs.get(test_user_1)[i],
                    test_runs.get(test_user_2)[i],
                    number_of_overlapping_posts,
                    number_of_overlapping_posts / ((total_posts_user_1 + total_posts_user_2) / 2),
                    number_of_overlapping_posts_acc_noise,
                    number_of_overlapping_posts_acc_noise / ((total_posts_user_1 + total_posts_user_2) / 2),
                    number_of_different_posts_user_1,
                    number_of_different_posts_user_2,
                    number_of_different_posts_user_1 / total_posts_user_1,
                    number_of_different_posts_user_2 / total_posts_user_2,
                    total_overlapping_posts,
                    '',
                    total_overlapping_posts / (len(all_posts.get(test_user_1)) + len(all_posts.get(test_user_2))),
                    total_posts_user_1,
                    total_posts_user_2,
                ))
                conn.commit()
    else:
        for user in test_user_pair:
            for test_run in test_runs.get(user):
                cur.execute(sql_add_row_table, (
                    user,
                    0,
                    test_run,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    total_overlapping_posts,
                    f'number of test runs for users {test_user_pair} unequal',
                    total_overlapping_posts / (len(all_posts.get(test_user_1)) + len(all_posts.get(test_user_2))),
                    0,
                    0,
                ))
                conn.commit()

    print(f"Overlapping post comparison for users {test_user_pair} completed.")


def check_if_data_already_exists_overlapping_posts_2_users(test_user_id_1, test_user_id_2, test_run_id_user_1,
                                                           test_run_id_user_2):
    """
    Check if overlapping_posts_2_users already lists PK test_user_1,test_user_2, test_run_1, test_run_2
    :return:
    """
    sql_check = """select test_user_id_1, test_user_id_2, test_run_id_user_1, test_run_id_user_2 
    from overlapping_posts_2_users where test_user_id_1 = %s and test_user_id_2  = %s and test_run_id_user_1  = %s
     and test_run_id_user_2 = %s"""
    cur.execute(sql_check, (test_user_id_1, test_user_id_2, test_run_id_user_1, test_run_id_user_2,))
    results = cur.fetchall()
    if len(results) > 0:
        return True
    else:
        return False


def retrieve_description(test_user_pair, test_runs):
    """
    Retrieve relevant description data of posts for a set of test users and test runs.
    :param test_user_pair:
    :param test_runs:
    :return:
    """
    sql_retrieve_description = """select id, desc_iteminfo, language_iso_long from posts 
        where testuserid = %s and testrunid = %s"""
    description_data = {}
    for user in test_user_pair:
        description_data[user] = {}
        for run in test_runs:
            cur.execute(sql_retrieve_description, (user, run,))
            results = cur.fetchall()
            temp_dict = {}
            if len(results) > 0:
                for item in results:
                    temp_dict[item[0]] = {'desc': item[1].strip(), 'lang': item[2].strip()}
                description_data[user][run] = temp_dict
    return description_data


def retrieve_hashtags(test_user_pair, test_runs):
    """
    Retrieve relevant hashtag data of posts for a set of test users and test runs.
    :param test_user_pair:
    :param test_runs:
    :return:
    """
    sql_retrieve_hashtags = """select phr.hashtagid, phr.name, phr.translation_english, phr.language, p.id, 
    p.testrunid, p.testuserid
    from (select phr1.hashtagid, phr1.postid, phr1.testrunid, h.name, h.iscommerce, h.id, h.translation_english, 
    h.language
            from d1rpgcvqcran0q.public.post_hashtag_relation phr1 join 
                d1rpgcvqcran0q.public.hashtags h on phr1.hashtagid = h.id
            where phr1.testrunid = %s) phr join
    (select id, testuserid, testrunid from d1rpgcvqcran0q.public.posts where testrunid = %s and testuserid = %s) p 
    on p.id = phr.postid"""
    sql_insert_hashtag_translation = """insert into translated_hashtags(post_id, hashtag_translation, hashtag_language) 
    values(%s,%s,%s)"""
    sql_update_hashtags = """update hashtags set translation_english = %s, language = %s where id = %s"""

    description_data = {}
    hashtag_data = {}
    posts_with_hashtags_to_ignore = []
    for user in test_user_pair:
        description_data[user] = {}
        hashtag_data[user] = {}
        for run in test_runs:

            # retrieve hashtags and their translations and update hashtags table if no translation exists
            cur.execute(sql_retrieve_hashtags, (run, run, user,))
            results = cur.fetchall()
            temp_dict = {}
            dict_hashtag_list_per_post = {}
            if len(results) > 0:
                for item in results:
                    cur_hashtag_id = item[0]
                    cur_hashtag = item[1].strip()
                    cur_hashtag_translation = clean_string(item[2])
                    cur_hashtag_language = item[3].strip()
                    cur_post_id = item[4]
                    if cur_hashtag_translation is not None:
                        if cur_post_id not in dict_hashtag_list_per_post.keys():
                            dict_hashtag_list_per_post[cur_post_id] = {'desc': [cur_hashtag_translation],
                                                                       'lang': cur_hashtag_language}
                        elif cur_post_id in dict_hashtag_list_per_post.keys() and cur_hashtag not in dict_hashtag_list_per_post.get(
                                cur_post_id):
                            dict_hashtag_list_per_post[cur_post_id]['desc'].append(cur_hashtag_translation)
                    else:
                        translated_string, lang = translate_string(cur_hashtag, incl_lang=True)
                        translated_string = clean_string(translated_string)
                        if cur_post_id not in dict_hashtag_list_per_post.keys():
                            dict_hashtag_list_per_post[cur_post_id] = {'desc': [translated_string],
                                                                       'lang': lang}
                        elif cur_post_id in dict_hashtag_list_per_post.keys() and translated_string not in dict_hashtag_list_per_post.get(
                                cur_post_id):
                            dict_hashtag_list_per_post[cur_post_id]['desc'].append(translated_string)
                        cur.execute(sql_update_hashtags, (translated_string, lang, cur_hashtag_id,))
                        conn.commit()
                hashtag_data[user][run] = dict_hashtag_list_per_post

                # join translated hashtags
                for post_id in dict_hashtag_list_per_post.keys():
                    curr_hashtag_string = ' '.join(
                        hashtag for hashtag in dict_hashtag_list_per_post.get(post_id).get('desc'))
                    try:
                        if len(curr_hashtag_string) > 0 and not curr_hashtag_string.isnumeric():
                            lang = dict_hashtag_list_per_post.get(post_id).get('lang')
                            if lang == '':
                                lang = detect(curr_hashtag_string)
                                if lang not in ['en', 'EN']:
                                    lang = TextBlob(curr_hashtag_string).detect_language()
                        else:
                            lang = dict_hashtag_list_per_post.get(post_id).get('lang')

                        # store concatenated hashtag
                        temp_dict[post_id] = {
                            'desc': curr_hashtag_string,
                            'lang': lang,
                            'already_translated': True
                        }
                        ## no longer needed to update translated_hashtags as hashtag translation already stored in hashtags
                        # cur.execute(sql_insert_hashtag_translation, (post_id, curr_hashtag_string, lang, ))
                        # conn.commit()
                    except:
                        print(dict_hashtag_list_per_post.get(post_id))
                        print(curr_hashtag_string)
                        posts_with_hashtags_to_ignore.append(post_id)
                description_data[user][run] = temp_dict
    print(
        f"Posts with empty or other hashtags to ignore for test users {test_user_pair}: {posts_with_hashtags_to_ignore}.")
    return description_data, hashtag_data


def deEmojify(text):
    """
    Removing emojis from string.
    Source: https://stackoverflow.com/questions/33404752/removing-emojis-from-a-string-in-python
    :param text:
    :return:
    """
    regrex_pattern = re.compile(pattern="["
                                        u"\U0001F600-\U0001F64F"  # emoticons
                                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                        u"\ufe0f"  # dingbats
                                        u"\u3030"
                                        "]+", flags=re.UNICODE)
    adjusted_text = regrex_pattern.sub(r'', text)
    if len(adjusted_text) == 0:
        return ''
    else:
        return adjusted_text


def clean_string(string, lang=None, already_translated=False):
    """
    Cleaning string: removing emjois, special characters, transforming to lower case, removing redundant white spaces.
    :param string:
    :return:
    """
    if len(string) > 0:
        remove_emojis = deEmojify(string)
        removed_special_characters = re.sub(r'[^a-zA-Z0-9\’\']', ' ', remove_emojis)
        lower_case = removed_special_characters.lower()
        remove_unnecessary_whitespaces = " ".join(lower_case.split())
        cleaned_string = remove_unnecessary_whitespaces
        if lang is not None and not cleaned_string.isnumeric() and not already_translated:
            if lang not in ['en', 'EN'] and len(cleaned_string) > 0:
                if len(cleaned_string) > 2:
                    try:
                        if TextBlob(cleaned_string).detect_language() not in ['en', 'EN']:
                            cleaned_string = translate_string(remove_unnecessary_whitespaces)
                    except urllib.error.HTTPError:
                        cleaned_string = translate_string(remove_unnecessary_whitespaces)
                else:
                    cleaned_string = translate_string(remove_unnecessary_whitespaces)
        return cleaned_string
    else:
        return string


def translate_string(string, incl_lang=False):
    """
    Translate a string into a specifc language using deepL API
    :param language_iso_code:
    :param string:
    :return: translation from DeepL
    """
    auth_key = "5966db67-aeb9-7df8-dbdd-c2c84fd7c15b"
    response = requests.post(url='https://api.deepl.com/v2/translate',
                             data={
                                 'target_lang': 'EN',
                                 'auth_key': auth_key,
                                 'text': string
                             })
    if response.__sizeof__() > 0:
        translations = json.loads(response.text).get('translations')
        translation = translations[0].get('text')
        detected_lang = translations[0].get('detected_source_language').lower()
    else:
        raise Exception('DeepL API returned empty response.')
    if incl_lang:
        return translation, detected_lang
    else:
        return translation


def download_package(package):
    try:
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        pass
    else:
        ssl._create_default_https_context = _create_unverified_https_context

    nltk.download(package)


def structuring_similarities_differences(dataframe, similarity_matrix=None, differences_matrix=None):
    """
    Compute the average similarity of
    - each post from a test run to the remaining post of the feed from the same test run
    - entire feed
    :param differences_matrix:
    :param dataframe:
    :param similarity_matrix:
    :return:
    """
    feed_similarity_differences = {}  # avg similarity / difference through out entire feed
    post_similarities_differences = {}  # post_id: {avg_sim_with_feed, avg_diff_with_feed}
    # iterating through every row instead of index as some rows have been removed due to empty strings
    dataframe_idx = []
    for i in range(dataframe.shape[0]):
        dataframe_idx.append(dataframe.index[i])

    for text_index in range(similarity_matrix.__len__()):
        # extracting relevant similarities/differences: all except to itself
        all_sim_except_of_current_element = similarity_matrix[text_index][np.arange(similarity_matrix[text_index].
                                                                                    __len__()) != text_index]
        all_diff_except_of_current_element = differences_matrix[text_index][np.arange(differences_matrix[text_index].
                                                                                      __len__()) != text_index]
        # computing averages from relevant similarities and differences
        avg_sim_of_all_sim_for_current_element = \
            sum(all_sim_except_of_current_element) / all_sim_except_of_current_element.__len__()
        avg_diff_of_all_sim_for_current_element = \
            sum(all_diff_except_of_current_element) / all_diff_except_of_current_element.__len__()

        post_similarities_differences[dataframe['post_id'][dataframe_idx[text_index]]] = {
            'avg_sim_for_post': avg_sim_of_all_sim_for_current_element,
            'avg_diff_for_post': avg_diff_of_all_sim_for_current_element
        }

    # computing average similarity and difference of all posts within feed
    all_sim = [post_similarities_differences.get(item).get('avg_sim_for_post') for item in
               post_similarities_differences.keys()]
    all_diff = [post_similarities_differences.get(item).get('avg_diff_for_post') for item in
                post_similarities_differences.keys()]
    feed_similarity_differences['avg_sim_entire_feed'] = sum(all_sim) / len(all_sim)
    feed_similarity_differences['avg_diff_entire_feed'] = sum(all_diff) / len(all_diff)

    return post_similarities_differences, feed_similarity_differences


def plot_similarities_differences(user_feed_similarities, users_similarities, text_source, action_type, epochs, lr,
                                  within_feed, thesis_chart):
    """
    Plot the computed similarities and differences in the database.
    :return:
    """
    appendix = ''
    color_set = [['#ff9999', '#ff3333'], ['#b2ff66', '#66cc00'], ['#c0c0c0', '#606060']]
    colors = {}
    test_users = []
    # generate dataframe
    df_users_individually = pd.DataFrame(columns=['Test_Run', 'Test_User', 'Avg_Similarity'])
    df_between_users = pd.DataFrame(columns=['Test_Run', 'Avg_Similarity'])

    # fill values based on user_feed_similarities
    for test_user in user_feed_similarities.keys():
        for test_run in user_feed_similarities.get(test_user).keys():
            df_users_individually = df_users_individually.append(pd.Series(({
                'Test_Run': test_run,
                'Test_User': test_user,
                'Avg_Similarity': user_feed_similarities.get(test_user).get(test_run)*100
            }), name=list(user_feed_similarities.get(test_user).keys()).index(test_run), dtype=object))
        appendix += '_' + str(test_user)
        colors[f'Avg_Similarity_{test_user}'] = color_set[
            list(user_feed_similarities.keys()).index(test_user)]
        test_users.append(test_user)

    # fill values based on users_similarities
    for run in users_similarities.keys():
        df_between_users = df_between_users.append(pd.Series(({
            'Test_Run': run,
            'Avg_Similarity': users_similarities.get(run)*100
        }), name=list(users_similarities.keys()).index(run), dtype=object))

    # plot similarity development
    if not within_feed:
        fig, axes = plt.subplots(figsize=(10, 10), nrows=2)
    else:
        fig, axes = plt.subplots(figsize=(10, 10), nrows=1)
    gradients_users_individually = {}
    labels_users_individually = []
    # plot similarities for each user
    for user, data in df_users_individually.groupby('Test_User'):
        # plotting trendline
        d = np.polyfit(data.index, data['Avg_Similarity'], 1)
        f = np.poly1d(d)
        gradients_users_individually[user] = round(nd.Gradient(f)([1]).min()*100, 4)
        data.insert(len(data.columns), 'Avg_Similarity_Trend', f(data.index))
        if not thesis_chart and not within_feed:
            data[['Avg_Similarity', 'Avg_Similarity_Trend']].plot(
                ax=axes[0], kind='line', color=colors.get(f'Avg_Similarity_{user}'),
                title=f'Hashtag Similarity Within Feed of Users {test_users}')
        else:
            data[['Avg_Similarity', 'Avg_Similarity_Trend']].plot(
                ax=axes, kind='line', color=colors.get(f'Avg_Similarity_{user}'))
        labels_users_individually.append(f'Avg_Similarity User {user}')
        labels_users_individually.append(f'Avg_Similarity_Trend User {user}')
    if not thesis_chart and not within_feed:
        axes[0].set_ylabel('Similarity in %', fontsize=15)
        axes[0].set_xlabel('Test Runs', fontsize=15)
        axes[0].text(0.5, -0.2, f"Increase of Avg_Similarity_Trend: User {test_users[0]}: "
                                f"{gradients_users_individually.get(test_users[0])} | "
                                f"User {test_users[1]}: {gradients_users_individually.get(test_users[1])}",
                     horizontalalignment='center', size=11, transform=axes[0].transAxes)
        axes[0].grid()
        axes[0].legend(labels=labels_users_individually)
        axes[0].set_xticks(list(np.arange(len(data))))
        axes[0].set_xticklabels(list(data['Test_Run']), rotation=45, ha='right', fontsize=15)
    else:
        axes.set_ylabel('Similarity in %', fontsize=15)
        axes.set_xlabel('Test Runs', fontsize=15)
        axes.text(0.5, -0.15, f"Increase of Avg_Similarity_Trend: User {test_users[0]}: "
                                f"{gradients_users_individually.get(test_users[0])} | "
                                f"User {test_users[1]}: {gradients_users_individually.get(test_users[1])}",
                     horizontalalignment='center', size=14, transform=axes.transAxes)
        axes.grid()
        axes.legend(labels=labels_users_individually)
        axes.set_xticks(list(np.arange(len(data))))
        axes.set_xticklabels(list(data['Test_Run']), rotation=45, ha='right', fontsize=15)

    # plot similarities between feeds of users
    # TODO check on data representation doesnt seem right
    if not within_feed:
        labels_users_individually = []
        # plotting trendline
        d = np.polyfit(df_between_users.index, df_between_users['Avg_Similarity'], 1)
        f = np.poly1d(d)
        gradient_between_users = round(nd.Gradient(f)([1]).min(), 4)
        df_between_users.insert(len(df_between_users.columns), 'Avg_Similarity_Trend', f(df_between_users.index))
        df_between_users.plot(ax=axes[1], kind='line', color=['#66b2ff', '#0066cc'],
                              title=f'Hashtag Similarity Between Feeds Of Users {test_users}')
        axes[1].set_ylabel('Similarity in %')
        axes[1].text(0.5, -0.2, f"Increase Of Avg Similarity Trend: {gradient_between_users}",
                     horizontalalignment='center', size=11, transform=axes[1].transAxes)
        axes[1].grid()
        axes[1].legend(labels=[f'Avg Similarity Between Users {test_users}', f'Avg Similarity Trend Between Users {test_users}'])
        axes[1].set_xticks(list(np.arange(len(df_between_users.index))))
        axes[1].set_xticklabels(list(df_between_users['Test_Run']), rotation=45, ha='right')
    plt.tight_layout(pad=2.5)
    fig.savefig((base_path / f"Plots/{action_type}/development_sim_{text_source}{appendix}_epochs{epochs}_lr{lr}.png"))


def reappearance_analysis_of_metric(test_user_pair, test_runs, metric, action_type, action_user, thesis_chart=False):
    """
    Retrieve the number of times a specific metric reappears in a set of test runs for each test user involved in the
    test run.
    :return:
    """
    # HASHTAG
    sql_insert_hashtag_appearances = """insert into hashtag_appearances_results(test_run_id, test_user_id, hashtag_id, 
    hashtag_name, number_of_appearances) values(%s,%s,%s,%s,%s) 
    on conflict on constraint hashtag_appearances_results_pkey do nothing"""

    sql_update_hashtag_appearances = """update hashtag_appearances_results 
    set number_of_appearances = %s
    where test_run_id = %s and test_user_id = %s and hashtag_id = %s and hashtag_name = %s"""

    sql_retrieve_hashtag_data = """select count(phr.postid) number_of_appearances, h.name, h.id
    from post_hashtag_relation phr
        join hashtags h on phr.hashtagid = h.id
        join posts p on phr.postid = p.id
    where phr.testrunid = %s and p.testuserid = %s
    group by h.id, h.name
    order by number_of_appearances desc"""

    sql_check_hashtag = """select * from hashtag_appearances_results where test_run_id = %s and test_user_id = %s 
    and hashtag_id = %s"""

    # CONTENT CREATOR
    sql_insert_content_creator = """insert into author_appearances_results(test_run_id, test_user_id, author_id, 
    author_uniqueid, number_of_appearances) values(%s,%s,%s,%s,%s)"""

    sql_update_content_creator = """update author_appearances_results
    set number_of_appearances = %s
    where test_run_id = %s and test_user_id = %s and author_id = %s and author_uniqueid = %s"""

    sql_retrieve_content_creator_data = """select count(p.id) number_of_appearances, a.uniqueid, p.authorid
    from posts p join authors a on p.authorid = a.id
    where p.testrunid = %s and p.testuserid = %s
    group by a.uniqueid, p.authorid
    order by number_of_appearances desc"""

    sql_check_content_creator = """select * from author_appearances_results where test_run_id = %s and test_user_id = %s
    and author_id = %s"""

    # SOUND
    sql_insert_music_sound = """insert into music_appearances_results(test_run_id, test_user_id, music_id, music_title, 
    number_of_appearances) values(%s,%s,%s,%s,%s) """

    sql_update_music_sound = """update music_appearances_results set number_of_appearances = %s
    where test_run_id = %s and test_user_id = %s and music_id = %s and music_title = %s"""

    sql_retrieve_music_sound_data = """select count(p.id) number_of_appearances, m.title, m.id
    from posts p join music m on p.musicid = m.id
    where p.testrunid = %s and p.testuserid = %s
    group by m.id, m.title
    order by number_of_appearances desc"""

    sql_check_music_sound = """select * from music_appearances_results where test_run_id = %s and test_user_id = %s
    and music_id = %s"""

    # select appropriate tools
    if metric == 'Hashtag':
        sql_retrieve_data = sql_retrieve_hashtag_data
        sql_insert_data = sql_insert_hashtag_appearances
        sql_update_data = sql_update_hashtag_appearances
        sql_check = sql_check_hashtag
    elif metric == 'Content Creator':
        sql_retrieve_data = sql_retrieve_content_creator_data
        sql_insert_data = sql_insert_content_creator
        sql_update_data = sql_update_content_creator
        sql_check = sql_check_content_creator
    elif metric == 'Sound':
        sql_retrieve_data = sql_retrieve_music_sound_data
        sql_insert_data = sql_insert_music_sound
        sql_update_data = sql_update_music_sound
        sql_check = sql_check_music_sound
    else:
        raise Exception("No correct metric given!")

    metric_results = {}
    for user in test_user_pair:
        metric_results[user] = {}
        for run in test_runs:
            cur.execute(sql_retrieve_data, (run, user,))
            results = cur.fetchall()
            metric_results[user][run] = {}
            if len(results) > 0:
                for item in results:
                    number_of_appearances = item[0]
                    name_field = item[1].strip()
                    id_field = item[2]
                    if metric == 'Hashtag':  # make sure that not too frequent appearing hashtags are considered
                        if number_of_appearances > 2 and name_field not in too_frequent_hashtags.values():
                            metric_results[user][run][id_field] = {
                                'name': name_field,
                                'number_of_appearances': int(number_of_appearances)
                            }
                    elif number_of_appearances > 2:
                        metric_results[user][run][id_field] = {
                            'name': name_field,
                            'number_of_appearances': int(number_of_appearances)
                        }
    print(f"Appearances computed for metric {metric}, users {test_user_pair}, and test runs {test_runs}.")
    # plot_metric_appearance_development(metric_results, test_user_pair, test_runs, metric, action_type, action_user)
    plot_overall_distribution_of_metric(metric_results, test_user_pair, test_runs, metric, action_type, action_user,
                                        thesis_chart)


def check_if_metric_exists(run, user, id_field, sql_check):
    cur.execute(sql_check, (run, user, id_field,))
    results = cur.fetchall()
    if len(results) > 0:
        return True
    else:
        return False


def get_cmap(length, name='hsv'):
    return plt.cm.get_cmap(name, length)


def plot_metric_appearance_development(metric_results, test_user_pair, test_runs, metric, action_type, action_user):
    """
    Visualize results from analysis on number of appearance of hashtag, content creator or sound for a test user
    in all test runs.
    Create for each test user one chart visualizing the number of appearances of a set of hashtags for each test run.
    :param test_user_pair:
    :param test_runs:
    :param metric:
    :return:
    """
    df = pd.DataFrame(columns=['Test_Run', 'Test_User', 'Name', 'Number_Of_Appearance'])

    sql_retrieve_content_creator_data = """select author_uniqueid, number_of_appearances from author_appearances_results 
    where test_run_id = %s and test_user_id = %s and number_of_appearances > 2"""

    sql_retrieve_hashtag_data = """select hashtag_id, hashtag_name, number_of_appearances from hashtag_appearances_results 
    where test_run_id = %s and test_user_id = %s and number_of_appearances > 2"""

    sql_retrieve_music_sound_data = """select music_title, number_of_appearances from music_appearances_results 
    where test_run_id = %s and test_user_id = %s and number_of_appearances > 2"""

    # select appropriate tools
    if metric == 'Hashtag':
        sql_retrieve_data = sql_retrieve_hashtag_data
    elif metric == 'Content Creator':
        sql_retrieve_data = sql_retrieve_content_creator_data
    elif metric == 'Sound':
        sql_retrieve_data = sql_retrieve_music_sound_data
    else:
        raise Exception("No correct metric given!")

    appendix = ''
    for user in test_user_pair:
        for run in test_runs:
            metric_appearances = metric_results.get(user).get(run)
            for metric_id in metric_appearances:
                df = df.append(pd.Series(({
                    'Test_Run': run,
                    'Test_User': user,
                    'Name': metric_appearances.get(metric_id).get('name'),
                    'Number_Of_Appearance': metric_appearances.get(metric_id).get('number_of_appearances')
                }), name=test_runs.index(run), dtype=object))

            ### fetching data doesn't take very long, but still too long
            # cur.execute(sql_retrieve_data, (run, user, ))
            # results = cur.fetchall()
            # if len(results) > 0:
            #     for item in results:
        appendix += '_' + str(user)

    ### generate distinct colors for each hashtag_name
    colors = distinctipy.get_colors(len(df['Name'].unique()))
    # distinctipy.color_swatch(colors)
    # cmap = get_cmap(len(df['Name'].unique()))
    colors_sel = {}
    i = 0
    for hashtag in df['Name'].unique():
        if hashtag not in colors_sel.keys():
            colors_sel[hashtag] = colors[i]
            i += 1

    fig, axes = plt.subplots(figsize=(15, 10), nrows=2)
    fig.suptitle(f"Development Of {metric} Reappearances For Users {test_user_pair}", fontsize=16, fontweight="bold")
    grouped_user = df.groupby('Test_User')
    for user, data_user in grouped_user:
        grouped_test_run = data_user.groupby('Test_Run')
        cur_axes = axes[list(grouped_user.groups).index(user)]
        labels = []
        for test_run, data_test_run in grouped_test_run:
            sum_appearances = 0
            for idx, hashtag in enumerate(data_test_run['Name']):
                cur_number_of_appearances = data_test_run.iloc[idx]['Number_Of_Appearance']
                cur_axes.bar(data_test_run.index[0], cur_number_of_appearances,
                             color=colors_sel.get(hashtag), bottom=sum_appearances)
                sum_appearances += cur_number_of_appearances
                if hashtag not in labels:
                    labels.append(hashtag)
        cur_axes.set_xticks(list(np.arange(len(grouped_test_run.groups.keys()))))
        cur_axes.set_xticklabels(list(grouped_test_run.groups.keys()), rotation=45, ha='right')
        cur_axes.set_ylabel(f'Number of Appearances of {metric}')
        cur_axes.legend(labels=labels, bbox_to_anchor=(1.04, 1.0), loc='upper left', ncol=4)
    plt.figtext(0.5, 0.01, f"For above displayed data user {action_user} performed {action_type}.",
                horizontalalignment='center', fontsize=12)
    plt.tight_layout(pad=2.5)
    fig.savefig((base_path / f"Plots/{action_type}/{metric}_appearance{appendix}.png"))
    print(f"Visualization of reappearance of {metric} generated for test users {test_user_pair}.")


def get_action_user(user_pair):
    """
    Retrieve the user who performed an action.
    :param user_pair:
    :return:
    """
    sql_fetch_test_results = """select number_of_posts_liked, number_of_followers, number_of_posts_watched_longer
    from d1rpgcvqcran0q.public.overlapping_post_test_results where test_user_id = %s"""
    number_of_action = {}
    action_user = []
    for user in user_pair:
        cur.execute(sql_fetch_test_results, (user,))
        results = cur.fetchall()
        for item in results:
            if item[0] > 0 or item[1] > 0 or item[2] > 0:
                if user not in action_user:
                    action_user.append(user)

    if len(action_user) > 1:
        # return str(action_user)
        raise Exception(f"More than one user performed an action for test user pair {user_pair}.")
    elif len(action_user) == 0:
        print(f"No user of the two {user_pair} performed any action.")
    else:
        return action_user[0]


def plot_overall_distribution_of_metric(metric_results, test_user_pair, test_runs, metric, action_type, action_user,
                                        thesis_chart):
    """
    Plot in a pie chart the overall distribution of the total number of the top 20 - 30 hashtags / content creators / sounds
    a user has seen across all test runs.
    :param metric_results:
    :param test_user_pair:
    :param test_runs:
    :param metric:
    :param action_type:
    :param action_user:
    :return:
    """
    df = pd.DataFrame(columns=['Test_User', 'Name', 'Total_Number_Of_Appearances'])

    sum_of_appearances = {}
    appendix = ''
    for user in test_user_pair:
        sum_of_appearances[user] = {}
        for run in test_runs:
            metric_appearances = metric_results.get(user).get(run)
            for metric_id in metric_appearances:
                name = metric_appearances.get(metric_id).get('name')
                number_of_appearances = metric_appearances.get(metric_id).get('number_of_appearances')
                if metric_id not in sum_of_appearances[user].keys():
                    sum_of_appearances[user][metric_id] = {
                        'Name': name,
                        'Total_Number_Of_Appearances': number_of_appearances
                    }
                elif metric_id in sum_of_appearances[user].keys():
                    sum_of_appearances[user][metric_id]['Total_Number_Of_Appearances'] += number_of_appearances

        # filter out all hashtags with less than 4 appearances, but for content creators
        # and sounds only filter for number of appearances < 2
        filtered_for_others = {}
        total_appearances_user = sum_of_appearances.get(user)
        filtered_for_others['Others'] = 0
        for metric_id in total_appearances_user:
            total_number_of_appearances = total_appearances_user.get(metric_id).get('Total_Number_Of_Appearances')
            if metric == 'Hashtag':
                if total_number_of_appearances < 4:
                    filtered_for_others['Others'] += total_number_of_appearances
                else:
                    filtered_for_others[metric_id] = total_number_of_appearances
            else:
                filtered_for_others[metric_id] = total_number_of_appearances

        for key in filtered_for_others:
            if key == 'Others':
                name = 'Others'
            else:
                name = total_appearances_user.get(key).get('Name')
            if filtered_for_others.get(key) != 0:
                df = df.append(pd.Series(({
                    'Test_User': user,
                    'Name': name,
                    'Total_Number_Of_Appearances': int(filtered_for_others.get(key))
                }), name=list(filtered_for_others.keys()).index(key), dtype=object))

        appendix += '_' + str(user)

    # generate distinct colors for each metric name
    colors = distinctipy.get_colors(len(df['Name'].unique()))
    # distinctipy.color_swatch(colors)
    # cmap = get_cmap(len(df['Name'].unique()))
    colors_sel = {}
    i = 0
    for hashtag in df['Name'].unique():
        if hashtag not in colors_sel.keys():
            colors_sel[hashtag] = colors[i]
            i += 1

    fig, axes = plt.subplots(figsize=(7, 10), nrows=2)
    if not thesis_chart:
        fig, axes = plt.subplots(figsize=(13, 10), nrows=2)
        fig.suptitle(f"Distribution Of {metric} Reappearances Across All Test Runs", fontsize=14, fontweight='bold')
    grouped_user = df.groupby('Test_User')
    for user, data_user in grouped_user:
        cur_axes = axes[list(grouped_user.groups).index(user)]
        data_user = data_user.sort_values('Total_Number_Of_Appearances', ascending=False)
        pie_wedge_collection = cur_axes.pie(data_user['Total_Number_Of_Appearances'], labels=data_user['Name'],
                                            startangle=90, labeldistance=1.05, autopct='%1.1f%%')
        for pie_wedge in pie_wedge_collection[0]:
            pie_wedge.set_facecolor(colors_sel[pie_wedge.get_label()])
        cur_axes.axis('equal')
        cur_axes.set_title(f"Distribution Of {metric} For User {user}")
        if not thesis_chart:
            if metric != 'Hashtag':
                bbtox_to_anschor = (0.95, 1)
                ncol = 2
            else:
                bbtox_to_anschor = (1.08, 1.0)
                ncol = 4
            cur_axes.legend(labels=data_user['Name'], bbox_to_anchor=bbtox_to_anschor, loc='upper left', ncol=ncol)
            plt.subplots_adjust(left=0.6, hspace=0.4)
    if not thesis_chart:
        plt.figtext(0.5, 0.01, f"For above displayed data user {action_user} performed {action_type}.",
                    horizontalalignment='center', fontsize=12)
    plt.tight_layout()
    fig.savefig((base_path / f"Plots/{action_type}/distribution_of_{metric}_reappearance{appendix}.png"),
                bbox_inches='tight')
    print(f"Visualization of reappearance distribution of {metric} across all test runs generated for test users {test_user_pair}.")


def heatmap_location(test_user_set, noise, switching_loc=False):
    """
    Generate a heatmap that compares for a set of users and corresponding test runs their overlapping posts.
    :param test_user_pair:
    :param test_runs:
    :return:
    """

    # retrieve user and test run data
    sql_retrieve_testrun_data = """select id, browser_language, country from testrun 
    where testuserid = %s and duration is not null"""
    user_incl_lang = {}  # user: {test_run, browser_lang}
    appendix = ''
    for user in test_user_set:
        cur.execute(sql_retrieve_testrun_data, (user, ))
        results = cur.fetchall()[:20]  # only consider first 20 test runs
        user_incl_lang[user] = {}
        for item in results:
            test_run_id = item[0]
            browser_language = item[1]
            country = item[2]
            if test_run_id not in user_incl_lang.get(user).keys():
                user_incl_lang[user][test_run_id] = {'browser_language': browser_language, 'country': country}
        appendix += '_' + str(user)

    # create valid test_run tuples/pairs to check for
    valid_test_run_tuples = []
    test_runs_user_pair_1 = list(user_incl_lang[test_user_set[0]].keys())
    test_runs_user_pair_1.sort()
    test_runs_user_pair_2 = list(user_incl_lang[test_user_set[2]].keys())
    test_runs_user_pair_2.sort()
    for i in range(len(test_runs_user_pair_1)):
        test_run_tuple = (test_runs_user_pair_1[i], test_runs_user_pair_2[i])
        test_run_tuple_itself_1 = (test_runs_user_pair_1[i], test_runs_user_pair_1[i])
        test_run_tuple_itself_2 = (test_runs_user_pair_2[i], test_runs_user_pair_2[i])
        if test_run_tuple not in valid_test_run_tuples:
            valid_test_run_tuples.append(test_run_tuple)
        if test_run_tuple_itself_1 not in valid_test_run_tuples:
            valid_test_run_tuples.append(test_run_tuple_itself_1)
        if test_run_tuple_itself_2 not in valid_test_run_tuples:
            valid_test_run_tuples.append(test_run_tuple_itself_2)

    # get noise
    noise = noise.get('avg_overlapping_posts')

    # get overlapping posts for every combination of test users
    user_combinations = list(itertools.permutations(test_user_set, 2))
    for user in test_user_set:
        user_combinations.append((user, user))
    overlapping_posts_user_combinations = {}
    df = pd.DataFrame()
    user_loc_lang_combinations = {}
    for user_pair in user_combinations:
        user_1 = user_pair[0]
        test_runs_user_1 = list(user_incl_lang.get(user_1).keys())
        test_runs_user_1.sort()
        user_2 = user_pair[1]
        test_runs_user_2 = list(user_incl_lang.get(user_2).keys())
        test_runs_user_2.sort()
        if len(test_runs_user_1) == len(test_runs_user_2):
            for run in range(len(test_runs_user_1)):
                user_1_browser_language = user_incl_lang.get(user_1).get(test_runs_user_1[run]).get('browser_language')
                user_1_country = user_incl_lang.get(user_1).get(test_runs_user_1[run]).get('country')
                user_2_browser_language = user_incl_lang.get(user_2).get(test_runs_user_2[run]).get('browser_language')
                user_2_country = user_incl_lang.get(user_2).get(test_runs_user_2[run]).get('country')
                user_1_setting = str(user_1) + "_" + user_1_country + "_" + user_1_browser_language
                user_2_setting = str(user_2) + "_" + user_2_country + "_" + user_2_browser_language
                user_1_run = test_runs_user_1[run]
                user_2_run = test_runs_user_2[run]
                different_posts, overlapping_posts, posts_user_1, posts_user_2 = \
                    get_number_of_different_overlapping_posts(user_1, user_1_run, user_2, user_2_run, noise)
                user_combination = user_1_setting + user_2_setting
                store = False
                if user_combination not in user_loc_lang_combinations.keys():
                    if switching_loc:
                        if (user_1_run, user_2_run) in valid_test_run_tuples or (user_2_run, user_1_run) in valid_test_run_tuples:
                            if user_1 == user_2 and user_1_country == user_2_country and user_1_browser_language == user_2_browser_language:
                                store = True
                            elif user_1 != user_2:
                                store = True
                            if store:
                                user_loc_lang_combinations[user_combination] = {
                                    'user_1': user_1_setting,
                                    'user_2': user_2_setting,
                                    'user_1_runs': [user_1_run],
                                    'user_2_runs': [user_2_run],
                                    'posts_user_1': posts_user_1,
                                    'posts_user_2': posts_user_2,
                                    'overlapping_posts': overlapping_posts
                                }
                    else:
                        user_loc_lang_combinations[user_combination] = {
                            'user_1': user_1_setting,
                            'user_2': user_2_setting,
                            'user_1_runs': [user_1_run],
                            'user_2_runs': [user_2_run],
                            'posts_user_1': posts_user_1,
                            'posts_user_2': posts_user_2,
                            'overlapping_posts': overlapping_posts
                        }
                elif user_combination in user_loc_lang_combinations.keys():
                    if switching_loc:
                        if (user_1_run, user_2_run) in valid_test_run_tuples or (user_2_run, user_1_run) in valid_test_run_tuples:
                            if user_1 == user_2 and user_1_country == user_2_country and user_1_browser_language == user_2_browser_language:
                                store = True
                            elif user_1 != user_2:
                                store = True
                            if store:
                                user_loc_lang_combinations[user_combination]['user_1_runs'].append(user_1_run)
                                user_loc_lang_combinations[user_combination]['user_2_runs'].append(user_2_run)
                                user_loc_lang_combinations[user_combination]['posts_user_1'] += posts_user_1
                                user_loc_lang_combinations[user_combination]['posts_user_2'] += posts_user_2
                                user_loc_lang_combinations[user_combination]['overlapping_posts'] += overlapping_posts
                    else:
                        user_loc_lang_combinations[user_combination]['user_1_runs'].append(user_1_run)
                        user_loc_lang_combinations[user_combination]['user_2_runs'].append(user_2_run)
                        user_loc_lang_combinations[user_combination]['posts_user_1'] += posts_user_1
                        user_loc_lang_combinations[user_combination]['posts_user_2'] += posts_user_2
                        user_loc_lang_combinations[user_combination]['overlapping_posts'] += overlapping_posts
        else:
            raise Exception(f"Number of test runs not equal for user pair {user_pair}!")

    for user_combo in user_loc_lang_combinations.keys():
        user_1 = user_loc_lang_combinations.get(user_combo).get('user_1')
        user_2 = user_loc_lang_combinations.get(user_combo).get('user_2')
        posts_user_1 = user_loc_lang_combinations.get(user_combo).get('posts_user_1')
        posts_user_2 = user_loc_lang_combinations.get(user_combo).get('posts_user_2')
        avg_number_test_runs = len(user_loc_lang_combinations.get(user_combo).get('user_1_runs'))
        overlapping_posts = user_loc_lang_combinations.get(user_combo).get('overlapping_posts')
        if user_1 != user_2:
            avg_overlapping_posts = round((overlapping_posts / avg_number_test_runs) - noise, 2)
        else:
            avg_overlapping_posts = round((overlapping_posts / avg_number_test_runs), 2)
        df = df.append(pd.Series(({
            'Test_User_X': user_1,
            'Test_User_Y': user_2,
            'Avg_Overlapping_Post': avg_overlapping_posts
        }), name=list(user_loc_lang_combinations.keys()).index(user_combo), dtype=object))

    print(df)

    fig = plt.figure(figsize=(27, 30))
    df_heatmap = df.pivot(index='Test_User_X', columns='Test_User_Y', values='Avg_Overlapping_Post')
    print(df_heatmap)
    sns.set(font_scale=4)
    h = sns.heatmap(df_heatmap, cmap='YlGnBu', annot=True)
    h.set_title(f"Avg Number Of Overlapping Posts Across All Test Runs", fontsize=40)
    plt.yticks(rotation=0)
    h.set_ylabel('')
    h.set_xlabel('')
    plt.show()
    fig.savefig((base_path / f"Plots/Location/avg_overlapping_posts{appendix}.png"))
    print(f"Visualization of avg overlapping posts across all test runs generated for test users {test_user_set}.")


def get_posts(test_user, test_run):
    sql_retrieve_posts = """select id from posts where testrunid = %s and testuserid = %s"""
    cur.execute(sql_retrieve_posts, (test_run, test_user,))
    results = cur.fetchall()
    post_list = []
    for item in results:
        post_list.append(item[0])
    return post_list


def get_number_of_different_overlapping_posts(test_user_1, test_run_user_1, test_user_2, test_run_user_2, noise):

    # posts user 1
    posts_user_1 = get_posts(test_user_1, test_run_user_1)

    # posts user 2
    posts_user_2 = get_posts(test_user_2, test_run_user_2)

    avg_total_posts = int((len(posts_user_1) + len(posts_user_2)) / 2)

    overlapping_posts = int((len(set(posts_user_1) & set(posts_user_2))))

    different_posts = round((((avg_total_posts - overlapping_posts) / avg_total_posts) - noise), 4)

    overlapping_posts_percentage = round((overlapping_posts / avg_total_posts), 4)

    return different_posts, overlapping_posts_percentage, len(posts_user_1), len(posts_user_2)


def get_content_creators(test_run, test_user):
    sql_retrieve_content_creator_data = """select p.authorid from posts p join authors a 
    on p.authorid = a.id where p.testrunid = %s and p.testuserid = %s"""
    cur.execute(sql_retrieve_content_creator_data, (test_run, test_user, ))
    results = cur.fetchall()
    content_creator_ids = []
    for item in results:
        content_creator_ids.append(item[0])
    return content_creator_ids


def get_number_of_different_content_creators(test_user_1, test_run_user_1, test_user_2, test_run_user_2, noise):

    # content creators user 1
    content_creator_ids_user_1 = get_content_creators(test_run=test_run_user_1, test_user=test_user_1)

    # content creators user 2
    content_creator_ids_user_2 = get_content_creators(test_run=test_run_user_2, test_user=test_user_2)

    # overlapping content creators
    overlapping_content_creator_ids = int(len(set(content_creator_ids_user_1) & set(content_creator_ids_user_2)))

    # average of total content creators to compute percentage of different content creators
    avg_total_content_creators = int((len(content_creator_ids_user_1) + len(content_creator_ids_user_2)) / 2)

    # different content creators
    different_content_creators = round(((avg_total_content_creators - overlapping_content_creator_ids) /
                                        avg_total_content_creators) - noise, 4)

    return different_content_creators


def get_sound(test_run, test_user):
    sql_retrieve_music_sound_data = """select m.id from posts p join music m on p.musicid = m.id 
    where p.testrunid = %s and p.testuserid = %s"""
    cur.execute(sql_retrieve_music_sound_data, (test_run, test_user, ))
    results = cur.fetchall()
    sound_ids = []
    for item in results:
        sound_ids.append(item[0])
    return sound_ids


def get_number_of_different_sound(test_user_1, test_run_user_1, test_user_2, test_run_user_2, noise):

    # sounds user 1
    sound_ids_user_1 = get_sound(test_run=test_run_user_1, test_user=test_user_1)

    # sounds user 2
    sound_ids_user_2 = get_sound(test_run=test_run_user_2, test_user=test_user_2)

    # overlapping sounds
    overlapping_sound_ids = int(len(set(sound_ids_user_1) & set(sound_ids_user_2)))

    # average of total sounds to compute percentage of different sounds
    avg_total_sound = int((len(sound_ids_user_1) + len(sound_ids_user_2)) / 2)

    # different sounds
    different_sounds = round(((avg_total_sound - overlapping_sound_ids) / avg_total_sound) - noise, 4)

    return different_sounds


def get_hashtags(test_run, test_user):
    sql_retrieve_hashtag_ids = """select distinct h.id from hashtags h
        join
            (select phr1.hashtagid, phr1.postid, p.id, p.desc_iteminfo, p.testrunid, p.testuserid from post_hashtag_relation phr1
                join posts p on p.id = phr1.postid and p.testrunid = phr1.testrunid
            where p.testuserid = %s and p.testrunid = %s) phr2
        on h.id = phr2.hashtagid"""
    cur.execute(sql_retrieve_hashtag_ids, (test_user, test_run, ))
    results = cur.fetchall()
    hashtag_ids = []
    for item in results:
        hashtag_ids.append(item[0])
    return hashtag_ids


def get_number_of_different_hashtags(test_user_1, test_run_user_1, test_user_2, test_run_user_2, noise):

    # hashtags user 1
    hashtags_ids_user_1 = get_hashtags(test_run=test_run_user_1, test_user=test_user_1)

    # hashtags user 2
    hashtags_ids_user_2 = get_hashtags(test_run=test_run_user_2, test_user=test_user_2)

    # overlapping hashtags
    overlapping_hashtag_ids = int(len(set(hashtags_ids_user_1) & set(hashtags_ids_user_2)))

    # average of total hashtags to compute percentage of different hashtags
    avg_total_hashtag = int((len(hashtags_ids_user_1) + len(hashtags_ids_user_2)) / 2)

    # different sounds
    different_hashtags = round(((avg_total_hashtag - overlapping_hashtag_ids) / avg_total_hashtag) - noise, 4)

    return different_hashtags


def get_number_of_actions_performed(test_runs, action_user, action_type):

    number_of_action_percentage = []
    number_of_action_abs = []
    for run in test_runs:

        if action_type == 'Follow':
            sql_retrieve_number_of_follows = """select count(author_id) from author_followed 
            where test_user_id = %s and test_run_id = %s and followed = True"""
            sql_retrieve_total_number_of_different_followers = """select count(distinct p.authorid)
            from posts p where p.testrunid = %s and p.testuserid = %s"""
            # get number of authors followed
            cur.execute(sql_retrieve_number_of_follows, (action_user, run, ))
            number_of_follows = cur.fetchall()[0][0]
            # get number of distinct authors visible for user
            cur.execute(sql_retrieve_total_number_of_different_followers, (run, action_user, ))
            total_number_of_distinct_authors = cur.fetchall()[0][0]
            number_of_action_percentage.append(round(number_of_follows / total_number_of_distinct_authors, 4))
            number_of_action_abs.append(number_of_follows)

        elif action_type == 'Like':
            sql_retrieve_number_of_likes = """select count(post_id) from liked_post 
            where testuser_id = %s and testrun_id = %s and liked = True"""
            sql_total_number_of_posts_seen = """select count(id) from posts where testuserid = %s and testrunid = %s"""
            # get number of posts liked
            cur.execute(sql_retrieve_number_of_likes, (action_user, run, ))
            number_of_likes = cur.fetchall()[0][0]
            # get number of total posts seen
            cur.execute(sql_total_number_of_posts_seen, (action_user, run, ))
            total_number_of_posts_seen = cur.fetchall()[0][0]
            number_of_action_percentage.append(round(number_of_likes / total_number_of_posts_seen, 4))
            number_of_action_abs.append(number_of_likes)

        elif action_type == 'Video View Rate':
            sql_retrieve_number_of_posts_watched_longer = """select count(post_id) from longer_watched_posts 
            where test_user_id = %s and test_run_id = %s"""
            sql_total_number_of_posts_seen = """select count(id) from posts where testuserid = %s and testrunid = %s"""
            # get number of posts watched longer
            cur.execute(sql_retrieve_number_of_posts_watched_longer, (action_user, run, ))
            number_of_posts_watched_longer = cur.fetchall()[0][0]
            # get number of total posts seen
            cur.execute(sql_total_number_of_posts_seen, (action_user, run,))
            total_number_of_posts_seen = cur.fetchall()[0][0]
            number_of_action_percentage.append(round(number_of_posts_watched_longer / total_number_of_posts_seen, 4))
            number_of_action_abs.append(number_of_posts_watched_longer)

    return number_of_action_percentage, number_of_action_abs


def get_last_execution_time_from_overlapping_post_test_results(test_run_id):
    sql_last_execution = """select executed_on from overlapping_post_test_results where test_run_id = %s"""
    cur.execute(sql_last_execution, (test_run_id, ))
    results = cur.fetchall()
    executed_on_list = []
    for item in results:
        executed_on_list.append(item[0])
    return executed_on_list


def get_rescue_value(user_1, user_2, test_run):
    """
    Check if for current test run a rescue value is stored in drop_rescue, if so return rescue values in dict and
    incorporate them accordingly.
    :param user_1:
    :param user_2:
    :param test_run:
    :return:
    """
    sql_retrieve_rescue_values = """select rescue_overlap, rescue_posts, rescue_hashtags, rescue_content_creators, 
    rescue_sounds from drop_rescue where drop_test_run_id = %s and user_1 = %s and user_2 = %s"""
    cur.execute(sql_retrieve_rescue_values, (test_run, user_1, user_2, ))
    results = cur.fetchall()
    if results is not None:
        if len(results) > 0:
            rescue_values = {
                'overlap': results[0][0],
                'posts': results[0][1],
                'hashtags': results[0][2],
                'content_creators': results[0][3],
                'sounds': results[0][4]
            }
            return rescue_values
        else:
            return None
    else:
        return None


def difference_analysis(test_user_pair, test_runs, action_type, noise, action_user, thesis_chart=False, account_for_drop=True):
    """
    Visualize the overlap of posts, hashtags, content creators, and sounds in one chart.
    :return:
    """

    if len(test_user_pair) == 2:
        user_1 = test_user_pair[0]
        user_2 = test_user_pair[1]
    else:
        raise Exception(f"Too many users for difference analysis: {test_user_pair}!")

    # create DataFrame with percentage of
    # different posts, hashtags, content creators, sounds for two users across all test runs

    df = pd.DataFrame(columns=['Different_Posts', 'Different_Posts_Regression',
                               'Different_Hashtags', 'Different_Hashtags_Regression',
                               'Different_Content_Creators', 'Different_Content_Creators_Regression',
                               'Different_Sounds', 'Different_Sounds_Regression',
                               'Number_of_Actions_Performed', 'Number_of_Actions_Performed_Regression'])
    appendix = f'_{str(user_1)}_{str(user_2)}'
    total_posts = []
    noise_posts = 0
    noise_content_creators = 0
    noise_hashtags = 0
    noise_sounds = 0
    if action_type != 'Control Group':
        noise_posts = noise.get('avg_difference_posts')
        noise_content_creators = noise.get('avg_different_content_creators')
        noise_hashtags = noise.get('avg_different_hashtags')
        noise_sounds = noise.get('avg_different_sounds')

    for run in test_runs:
        different_posts, overlapping_posts, total_posts_user_1, total_posts_user_2 = \
            get_number_of_different_overlapping_posts(test_user_1=user_1, test_run_user_1=run, test_user_2=user_2,
                                                      test_run_user_2=run, noise=noise_posts)

        total_posts.append((total_posts_user_1 + total_posts_user_2) / 2)
        different_hashtags = get_number_of_different_hashtags(test_user_1=user_1, test_run_user_1=run,
                                                              test_user_2=user_2, test_run_user_2=run,
                                                              noise=noise_hashtags)
        different_content_creators = get_number_of_different_content_creators(test_user_1=user_1, test_user_2=user_2,
                                                                              test_run_user_1=run, test_run_user_2=run,
                                                                              noise=noise_content_creators)
        different_sounds = get_number_of_different_sound(test_user_1=user_1, test_user_2=user_2, test_run_user_1=run,
                                                         test_run_user_2=run, noise=noise_sounds)

        # account for rescue if appropriate
        if account_for_drop:
            rescue_values = get_rescue_value(user_1=user_1, user_2=user_2, test_run=run)
            if rescue_values is not None:
                different_posts = rescue_values.get('posts')
                different_hashtags = rescue_values.get('hashtags')
                different_content_creators = rescue_values.get('content_creators')
                different_sounds = rescue_values.get('sounds')

        executed_on = get_last_execution_time_from_overlapping_post_test_results(run)
        datetime = max(executed_on)
        datetime = str(datetime.year)[2:] + '-' + str(datetime.month) + '-' + str(datetime.day) + "(" + str(datetime.hour) + ":" + str(datetime.minute) + ")"

        df = df.append(pd.Series(({
            'Test_Desc': str(run) + '|' + datetime,
            'Different_Posts': different_posts*100,
            'Different_Hashtags': different_hashtags*100,
            'Different_Content_Creators': different_content_creators*100,
            'Different_Sounds': different_sounds*100,
        }), name=test_runs.index(run), dtype=object))

    # compute regressions
    regression_column_names = {'Different_Posts': 'Different_Posts_Regression',
                               'Different_Hashtags': 'Different_Hashtags_Regression',
                               'Different_Content_Creators': 'Different_Content_Creators_Regression',
                               'Different_Sounds': 'Different_Sounds_Regression'}

    # retrieve action data
    number_of_actions_performed = []
    if action_type != 'Control Group' and action_user is not None:
        regression_column_names['Number_of_Actions_Performed'] = 'Number_of_Actions_Performed_Regression'
        number_of_action_percentage, number_of_action_abs = get_number_of_actions_performed(test_runs, action_user, action_type)
        number_of_actions_performed = number_of_action_abs
        df['Number_of_Actions_Performed'] = [action*100 for action in number_of_action_percentage]

    gradients = {}
    for column in regression_column_names.keys():
        d = np.polyfit(df.index.array, df[column], 1)
        f = np.poly1d(d)
        gradients[regression_column_names.get(column)] = round(nd.Gradient(f)([1]).min(), 4)
        df[regression_column_names.get(column)] = f(df.index)

    # plot data
    colors = ['#ff9999', '#ff3333', '#b2ff66', '#66cc00', '#66b2ff', '#0066cc', '#ff8000', '#ffb266', '#c0c0c0', '#606060']
    if not thesis_chart:
        plot = df.plot(figsize=(20, 10), color=colors)
        plot.set_title(f"Difference Analysis For Users {test_user_pair}", fontsize=20)
    else:
        plot = df.plot(figsize=(20, 10), color=colors)
    if not df['Number_of_Actions_Performed'].isnull().values.any():
        plot.scatter(df.index.array, df['Number_of_Actions_Performed'], linestyle='None', marker='o', color='black',
                     label=f'Number of {action_type}')
    plot.set_xlabel("Test Runs", fontsize=20)
    plot.set_ylabel("Difference / Number Of Actions In %", fontsize=20)
    plot.set_xticks(list(np.arange(len(test_runs))))
    plot.set_xticklabels(list(df['Test_Desc']), rotation=45, ha='right', fontsize=15)
    plot.grid()
    failed_test_runs = get_number_of_failed_testruns(test_user_pair)
    if not thesis_chart:
        plot.legend(bbox_to_anchor=(1.05, 1.0), loc='upper left')
        plot.text(1.06, 0.3, f"Total failed test runs: {failed_test_runs}\n"
                             f"Different Posts Regression Slope: {gradients.get('Different_Posts_Regression')}\n"
                             f"Hashtags Only  Regression Slope: {gradients.get('Different_Hashtags_Regression')}\n"
                             f"Different Content Creators Regression Slope: {gradients.get('Different_Content_Creators_Regression')}\n"
                             f"Different Sounds Regression Slope: {gradients.get('Different_Sounds_Regression')}\n"
                             f"Number of Actions Regression Slope: {gradients.get('Number_of_Actions_Performed_Regression')}\n"
                             f"For displayed data user {action_user} performed {action_type}, in total {sum(number_of_actions_performed)}",
                  horizontalalignment='left', color='black', fontsize=13, transform=plot.transAxes)
    else:
        plot.legend()
    # plt.subplots_adjust(left=0.3)
    plt.tight_layout()
    fig = plot.get_figure()
    fig.savefig((base_path / f"Plots/{action_type}/difference_analysis{appendix}.png"))
    print(f"Chart visualizing of difference analysis completed for {test_user_pair}.")


def compute_noise_control_scenarios(test_user_pairs, batch_size, account_for_unfinished_scenarios=False):
    """
    Compute avg noise for difference of posts, content creators, hashtags, sounds for every test run
    :return:
    """
    noise_all_computation = {}
    noise_avg_overall_runs_computation = {}
    noise_avg_overall_runs_overall_users_computation = {}
    noise_avg_per_run_computation = {}
    test_runs = []

    # as some test scenarios did not complete all runs we have to reduce the number of test runs for which we calculate
    # the noises
    test_runs_to_consider = 0
    if account_for_unfinished_scenarios:
        number_of_test_runs = []
        for pair in test_user_pairs:
            # get test runs
            test_runs = get_test_run_ids_2_user(pair)
            number_of_test_runs.append(len(test_runs))
        test_runs_to_consider = min(number_of_test_runs)

    for pair in test_user_pairs:
        # get test runs
        test_runs = get_test_run_ids_2_user(pair)

        # only consider the first 20 test runs if current user pair performed more than that
        if len(test_runs) > 20 and not account_for_unfinished_scenarios:
            test_runs = test_runs[:20]
        elif account_for_unfinished_scenarios:
            test_runs = test_runs[:test_runs_to_consider]

        # set users
        user_1 = pair[0]
        user_2 = pair[1]
        curr_user_pair = str(user_1)+str(user_2)

        noise_all_computation[curr_user_pair] = {}
        noise_avg_overall_runs_computation[curr_user_pair] = {}

        for run in test_runs:

            # fetch different posts
            different_posts, overlapping_posts, total_posts_user_1, total_posts_user_2 = \
                get_number_of_different_overlapping_posts(test_user_1=user_1, test_run_user_1=run, test_user_2=user_2,
                                                          test_run_user_2=run, noise=0)

            # fetch different content creators
            different_content_creators = \
                get_number_of_different_content_creators(test_user_1=user_1, test_run_user_1=run,
                                                         test_user_2=user_2, test_run_user_2=run, noise=0)

            # fetch different hashtags
            different_hashtags = \
                get_number_of_different_hashtags(test_user_1=user_1, test_run_user_1=run,
                                                 test_user_2=user_2, test_run_user_2=run, noise=0)

            # fetch different sounds
            different_sounds = \
                get_number_of_different_sound(test_user_1=user_1, test_run_user_1=run,
                                              test_user_2=user_2, test_run_user_2=run, noise=0)

            # account for rescue if appropriate
            rescue_values = get_rescue_value(user_1=user_1, user_2=user_2, test_run=run)
            if rescue_values is not None:
                overlapping_posts = rescue_values.get('overlap')
                different_posts = rescue_values.get('posts')
                different_hashtags = rescue_values.get('hashtags')
                different_content_creators = rescue_values.get('content_creators')
                different_sounds = rescue_values.get('sounds')

            noise_all_computation[curr_user_pair][run] = {
                'overlapping_posts': overlapping_posts,
                'different_posts': different_posts, 'different_content_creators': different_content_creators,
                'different_hashtags': different_hashtags, 'different_sounds': different_sounds
            }

        avg_overlapping_posts = sum([noise_all_computation[curr_user_pair][run]['overlapping_posts']
                                    for run in noise_all_computation[curr_user_pair]]) / len(test_runs)

        avg_difference_posts = sum([noise_all_computation[curr_user_pair][run]['different_posts']
                                    for run in noise_all_computation[curr_user_pair]]) / len(test_runs)

        avg_different_content_creators = sum([noise_all_computation[curr_user_pair][run]['different_content_creators']
                                              for run in noise_all_computation[curr_user_pair]]) / len(test_runs)

        avg_different_hashtags = sum([noise_all_computation[curr_user_pair][run]['different_hashtags']
                                      for run in noise_all_computation[curr_user_pair]]) / len(test_runs)

        avg_different_sounds = sum([noise_all_computation[curr_user_pair][run]['different_sounds']
                                    for run in noise_all_computation[curr_user_pair]]) / len(test_runs)

        noise_avg_overall_runs_computation[curr_user_pair]['avg_overlapping_posts'] = avg_overlapping_posts
        noise_avg_overall_runs_computation[curr_user_pair]['avg_difference_posts'] = avg_difference_posts
        noise_avg_overall_runs_computation[curr_user_pair]['avg_different_content_creators'] = avg_different_content_creators
        noise_avg_overall_runs_computation[curr_user_pair]['avg_different_hashtags'] = avg_different_hashtags
        noise_avg_overall_runs_computation[curr_user_pair]['avg_different_sounds'] = avg_different_sounds

    noise_avg_overall_runs_overall_users_computation = {
        'avg_overlapping_posts': round(sum([noise_avg_overall_runs_computation.get(pair).get('avg_overlapping_posts')
                                           for pair in noise_avg_overall_runs_computation.keys()]) / len(test_user_pairs), 4),
        'avg_difference_posts': round(sum([noise_avg_overall_runs_computation.get(pair).get('avg_difference_posts')
                                           for pair in noise_avg_overall_runs_computation.keys()]) / len(test_user_pairs), 4),
        'avg_different_content_creators': round(sum([noise_avg_overall_runs_computation.get(pair).
                                                    get('avg_different_content_creators') for pair in
                                                     noise_avg_overall_runs_computation.keys()]) / len(test_user_pairs), 4),
        'avg_different_hashtags': round(sum([noise_avg_overall_runs_computation.get(pair).get('avg_different_hashtags')
                                             for pair in noise_avg_overall_runs_computation.keys()]) / len(test_user_pairs), 4),
        'avg_different_sounds': round(sum([noise_avg_overall_runs_computation.get(pair).get('avg_different_sounds')
                                           for pair in noise_avg_overall_runs_computation.keys()]) / len(test_user_pairs), 4)
    }

    print(noise_all_computation)
    print(noise_avg_overall_runs_computation)
    print(f"Noise for users {test_user_pairs} collecting data from {batch_size} batches: ")
    print(f"Avg Overlapping Posts: {noise_avg_overall_runs_overall_users_computation['avg_overlapping_posts']}")
    print(f"Avg Different Posts: {noise_avg_overall_runs_overall_users_computation['avg_difference_posts']}")
    print(f"Avg Different Content Creators: {noise_avg_overall_runs_overall_users_computation['avg_different_content_creators']}")
    print(f"Avg Different Hashtags: {noise_avg_overall_runs_overall_users_computation['avg_different_hashtags']}")
    print(f"Avg Different Sounds: {noise_avg_overall_runs_overall_users_computation['avg_different_sounds']}")

    # get avg noise of all users for every test run
    for run in range(len(test_runs)):
        noise_avg_per_run_computation[run] = {
            'avg_difference_posts': round(sum([noise_all_computation[pair][list(noise_all_computation[pair].keys())[run]]
                                         ['different_posts'] for pair in noise_all_computation.keys()]) /
                                    len(noise_all_computation.keys()), 4),
            'avg_different_content_creators':
                round(sum([noise_all_computation[pair][list(noise_all_computation[pair].keys())[run]]
                     ['different_content_creators'] for pair in noise_all_computation.keys()]) /
                len(noise_all_computation.keys()), 4),
            'avg_different_hashtags': round(sum([noise_all_computation[pair][list(noise_all_computation[pair].keys())[run]]
                                           ['different_hashtags'] for pair in noise_all_computation.keys()]) /
                                      len(noise_all_computation.keys()), 4),
            'avg_different_sounds': round(sum([noise_all_computation[pair][list(noise_all_computation[pair].keys())[run]]
                                         ['different_sounds'] for pair in noise_all_computation.keys()]) /
                                    len(noise_all_computation.keys()), 4)
        }

    return noise_all_computation, noise_avg_per_run_computation, noise_avg_overall_runs_overall_users_computation


def compute_noise_posts(test_users, batch_size):
    """
    Compute different_posts_noise on average from control group users
    :param test_users:
    :return:
    """
    # compute overall average difference in posts between two users for all different user pairs, then take average
    # from those different differences
    # compute average from all 16 users for each of the 20 test runs -> to plot with trendline
    data_values = []
    overall_differences = []
    sql_differences = """select test_user_id, difference_user_avg_total_posts, 
    difference_user_acc_for_noise_not_avg_total_posts, total_posts_viewed, overlapping_posts_total
    from overlapping_post_test_results where test_run_id = %s"""
    for test_user_pair in test_users:
        test_run_ids = get_test_run_ids_2_user(test_user_pair)
        # get differences
        differences = []
        if len(test_run_ids) > 0:
            for test_run_id in test_run_ids:
                cur.execute(sql_differences, (test_run_id,))
                results = cur.fetchall()
                for item in results:
                    data_values.append(
                        {
                            "test_run_id": test_run_id,
                            "test_user_id": item[0],
                            "diff_avg_total_posts": item[1],
                            "diff_acc_for_noise": item[2]
                        }
                    )
                    differences.append((item[3] - item[4]) / item[3])
            avg_difference = sum(differences) / len(differences)
            overall_differences.append(avg_difference)
            sql_update_control_group_results_1 = """insert into control_group_results(description, average_noise, 
            batch_size) values(%s,%s,%s)"""
            cur.execute(sql_update_control_group_results_1,
                        (f"CG users: {test_user_pair}", avg_difference, batch_size))
            conn.commit()
    overall_avg_difference = sum(overall_differences) / len(overall_differences)
    sql_update_control_group_results_2 = """update control_group_results set overall_noise = %s, batch_size = %s
    where overall_noise is null"""
    cur.execute(sql_update_control_group_results_2, (overall_avg_difference, batch_size))
    conn.commit()
    print(f"[NOISE] Overall difference of posts from all control group users: {overall_avg_difference}")











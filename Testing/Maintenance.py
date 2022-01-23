import csv
import itertools
from _csv import reader

import langdetect
from googletrans import Translator

from src.Proxy import *
from DataAnalysis.Analysis_Methods import *

database = DatabaseHelper()
base_path = Path(__file__).parent


def proxy_maintenance_init(country):
    """
    Find new proxy.
    :param country:
    :return:
    """
    print(find_disposable_proxy(country))  # refresh webshare.io accordingly
    print(get_new_proxy(country))  # returns the new proxy address
    proxies_maintenance()  # maintain proxy address table
    update_db_for_user(host='45.57.243.208', port='7749', user=48)  # update test user with new proxy address


def clean_internal_music_ids():
    """
    Clean-up internal music ids every now and then.
    :return:
    """
    database.update_internal_music_ids()
    print("Internal music id cleaning completed.")


def copy_language_iso():
    sql_lang = """select id, testrunid, testuserid, language_iso from posts where language_iso is not null and language_iso_long is null"""
    database.cur.execute(sql_lang)
    results = database.cur.fetchall()
    sql_update = """update posts set language_iso_long = %s where id = %s and testrunid = %s and testuserid = %s"""
    for item in results:
        database.cur.execute(sql_update, (item[3], item[0], item[1], item[2]))
        database.conn.commit()
        print(item)


def get_authors():
    authors = {"authors": []}
    sql_authors = """select distinct uniqueid from authors limit 500"""
    database.cur.execute(sql_authors)
    results = database.cur.fetchall()
    for item in results:
        authors['authors'].append(item[0])
    print(authors)


def get_music():
    music = []
    sql_music = """select distinct id from music limit 500"""
    database.cur.execute(sql_music)
    results = database.cur.fetchall()
    for item in results:
        music.append(item[0])
    print(music)


def get_author_uniqueids(test_user_ids):
    test_run_ids = get_test_run_ids_2_user(test_user_ids)
    sql = """select r.uniqueid from (select count(distinct p.id) number_of_posts, uniqueid
        from posts p join authors a on p.authorid = a.id
        where (p.testuserid = %s or p.testuserid = %s) and p.testrunid in (2069, 2113, 2205, 2238, 226, 2299)
        group by authorid, uniqueid, followercount
        order by number_of_posts desc) r where r.number_of_posts > 1"""
    authors = []
    database.cur.execute(sql, (test_user_ids[0], test_user_ids[1]))
    result = database.cur.fetchall()
    for item in result:
        authors.append(item[0])
    print(authors)


def get_music_ids(test_user_ids):
    test_run_ids = get_test_run_ids_2_user(test_user_ids)
    sql = """select s.id from (select m2.number_of_posts, m1.id from music m1 join
    (
        select m.id music_id, count(distinct p.id) number_of_posts
        from posts p join music m on p.music_internal_id = m.internal_id
        where (p.testuserid = %s or p.testuserid = %s) and p.testrunid in (1729,1763,1791,1821,1825)
        group by m.id
    ) m2 on m1.id = m2.music_id
    order by m2.number_of_posts desc) s where s.number_of_posts > 1"""
    music = []
    database.cur.execute(sql, (test_user_ids[0], test_user_ids[1]))
    result = database.cur.fetchall()
    for item in result:
        music.append(item[0])
    print(music)


def update_tables_due_to_location(old_test_user, browser_lang, new_test_user):
    sql_test_runs = """select id from testrun where testuserid = %s and browser_language = %s"""
    database.cur.execute(sql_test_runs, (old_test_user, browser_lang))
    test_runs = []
    results = database.cur.fetchall()
    for item in results:
        test_runs.append(item[0])

    # update testrun with new_test_user
    sql_runs_retrieve = """select date, description, ip_used, country, browser_language, id, duration
    from testrun where id = %s and testuserid=%s"""
    sql_runs_insert = """insert into testrun(testuserid, date, description, ip_used, country, browser_language, id, duration)
    values(%s,%s,%s,%s,%s,%s,%s,%s) on conflict on constraint testrun_pkey do nothing"""
    for run in test_runs:
        database.cur.execute(sql_runs_retrieve, (run, old_test_user,))
        results = database.cur.fetchall()
        for item in results:
            database.cur.execute(sql_runs_insert, (
                # testuserid,
                new_test_user,
                # date,
                item[0],
                # description,
                item[1],
                # ip_used,
                item[2],
                # country,
                item[3],
                # browser_language,
                item[4],
                # id,
                item[5],
                # duration
                item[6],
            ))
            database.conn.commit()

    # update posts with new test_user
    sql_posts = """select id,desc_iteminfo,fullurl,language_iso_long,language_label,
                video_druation_sec,likes_diggcount,sharecount,commentcount,playcount,musicid,authorid,testrunid,
                post_position,isAd,testuserid,batch_position,music_internal_id from posts where testuserid = %s
                and testrunid = %s"""
    posts = """insert into d1rpgcvqcran0q.public.posts(id,desc_iteminfo,fullurl,language_iso_long,language_label,
                        video_druation_sec,likes_diggcount,sharecount,commentcount,playcount,musicid,authorid,testrunid,
                        post_position,isAd,testuserid,batch_position,music_internal_id)
                        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        on conflict on constraint posts_pkey do nothing;"""

    for run in test_runs:
        database.cur.execute(sql_posts, (old_test_user, run,))
        results = database.cur.fetchall()
        for item in results:
            database.cur.execute(posts, (
                # id,
                item[0],
                # desc_iteminfo,
                item[1],
                # fullurl,
                item[2],
                # language_iso_long,
                item[3],
                # language_label,
                item[4],
                # video_druation_sec,
                item[5],
                # likes_diggcount,
                item[6],
                # sharecount,
                item[7],
                # commentcount,
                item[8],
                # playcount,
                item[9],
                # musicid,
                item[10],
                # authorid,
                item[11],
                # testrunid,
                item[12],
                # post_position,
                item[13],
                # isAd,
                item[14],
                # testuserid,
                new_test_user,
                # batch_position,
                item[16],
                # music_internal_id
                item[17],
            ))
            database.conn.commit()

    # delete posts with old_test_user
    sql_delete_post = """delete from posts where testuserid=%s and testrunid=%s"""
    # delete from overlapping
    sql_over = """delete from overlapping_post_test_results where test_user_id=%s and test_run_id=%s"""
    # delete testrun with old_test_user
    sql_delete_runs = """delete from testrun where testuserid=%s and id=%s"""
    for run in test_runs:
        # posts
        database.cur.execute(sql_delete_post, (old_test_user, run,))
        database.conn.commit()
        # over
        database.cur.execute(sql_over, (old_test_user, run,))
        database.conn.commit()
        # testrun
        database.cur.execute(sql_delete_runs, (old_test_user, run,))
        database.conn.commit()


def update_proxies_to_usermanager():
    sql_select_users = """select user_using_this_proxy, host, port, country from proxies where user_using_this_proxy is not null"""
    database.cur.execute(sql_select_users, ())
    results = database.cur.fetchall()
    sql_update_usermanager = """update usermanager set host = %s, port = %s, country = %s where userid = %s"""
    for item in results:
        database.cur.execute(sql_update_usermanager, (item[1], item[2], item[3], item[0]))
        database.conn.commit()


def generate_combinations(users):
    result = []
    for comb in itertools.combinations(users, 2):
        result.append(list(comb))
    return result


def translate_hashtag_strings():
    # get all hashtag strings
    sql_hashtags = """select distinct id, name, postid from hashtags join 
    (select postid, hashtagid from post_hashtag_relation) phr on id = phr.hashtagid"""
    sql_insert_translated_hashtag = """insert into translated_hashtags(post_id, hashtag_translation, hashtag_language) 
    values (%s,%s,%s)"""
    database.cur.execute(sql_hashtags, ())
    results = database.cur.fetchall()
    temp_dict = {}
    concat_hashtags = {}
    posts_with_hashtags_to_ignore = []
    if len(results) > 0:
        for item in results:
            translated_hashtag = item[1].strip()
            cur_hashtag_without_emojis = deEmojify(translated_hashtag)
            if item[2] not in concat_hashtags.keys():
                concat_hashtags[item[2]] = [cur_hashtag_without_emojis]
            elif item[2] in concat_hashtags.keys() and translated_hashtag not in concat_hashtags.get(item[2]):
                concat_hashtags[item[2]].append(cur_hashtag_without_emojis)
        for post_id in concat_hashtags.keys():
            curr_hashtag_string = ' '.join(hashtag for hashtag in concat_hashtags.get(post_id))
            try:
                if len(curr_hashtag_string) > 0 and any(curr_hashtag_string):
                    temp_dict[post_id] = {
                        'desc': curr_hashtag_string,
                        'lang': detect(curr_hashtag_string)
                    }
            except:
                print(concat_hashtags.get(post_id))
                print(curr_hashtag_string)
                posts_with_hashtags_to_ignore.append(post_id)
        # store temp_dict in json
        base_path = Path(__file__).parent
        file_path = (base_path / "../utilities/hashtag_translations.json").resolve()
        with open(file_path, 'w') as fp:
            json.dump(temp_dict, fp)

        for post_id in temp_dict.keys():
            translated_hashtag = temp_dict.get(post_id).get('desc')
            cur_lang = temp_dict.get(post_id).get('lang')
            if cur_lang not in ['en', 'EN'] and len(translated_hashtag) > 2:
                try:
                    if TextBlob(translated_hashtag).detect_language() not in ['en', 'EN']:
                        translated_hashtag, cur_lang = translate_string(string=translated_hashtag, incl_lang=True)
                except urllib.error.HTTPError:
                    translated_hashtag, cur_lang = translate_string(string=translated_hashtag, incl_lang=True)
            temp_dict[post_id]['desc'] = translated_hashtag
            temp_dict[post_id]['lang'] = cur_lang
            try:
                database.cur.execute(sql_insert_translated_hashtag, (post_id, translated_hashtag, cur_lang,))
                database.conn.commit()
            except psycopg2.errors.UniqueViolation as e:
                print(e)
                print(post_id)
                print(temp_dict.get(post_id))


def update_hashtags():
    sql_hashtags = """select distinct id, name from hashtags where translation_english is null"""
    sql_update_hashtags = """update hashtags set language = %s, translation_english = %s where id = %s"""
    database.cur.execute(sql_hashtags, ())
    results = database.cur.fetchall()
    foreign_characters = []

    if len(results) > 0:
        for item in results:
            hashtag_id = item[0]
            translated_hashtag = item[1].strip()
            if item[0] in foreign_characters:
                translated_hashtag, lang = translate_string(translated_hashtag, incl_lang=True)
            else:
                cleaned_hashtag = clean_string(translated_hashtag)
                lang = ''
                try:
                    if not cleaned_hashtag.isnumeric():
                        lang = detect(cleaned_hashtag)
                    if not cleaned_hashtag.isnumeric() and lang not in ['en', 'EN'] and len(cleaned_hashtag) > 2:
                        try:
                            lang = TextBlob(cleaned_hashtag).detect_language()
                            if lang not in ['en', 'EN']:
                                translated_hashtag, lang = translate_string(string=cleaned_hashtag, incl_lang=True)
                        except urllib.error.HTTPError:
                            translated_hashtag, lang = translate_string(string=cleaned_hashtag, incl_lang=True)
                except langdetect.lang_detect_exception.LangDetectException as err:
                    print(err)
                    print(hashtag_id)
                    print(translated_hashtag)
                    print(cleaned_hashtag)
            database.cur.execute(sql_update_hashtags, (lang, translated_hashtag, item[0]))
            database.conn.commit()
            print(f"Hashtag << {item[1].strip()} >> updated.")


def clean_hashtags():

    # hashtag_ids = []
    # file = open(file_path, 'r')
    # lines = file.readlines()
    # for line in lines:
    #     hashtag_ids.append(line.strip('\n'))
    # print(hashtag_ids)

    sql_update_hashtag = """update hashtags set translation_english = %s, language = %s where id = %s"""
    sql_get_hashtag = """select name from hashtags where id = %s"""

    file_path = (base_path / "../utilities/foreign_char_trans_8.csv").resolve()
    hashtags = {}
    with open(file_path, 'r') as file:
        csv_reader = reader(file)
        for row in csv_reader:
            cur_value = row[0].split(';')
            hashtags[cur_value[0]] = cur_value[1].strip()
    print(hashtags)

    for hashtag in hashtags:
        cur.execute(sql_update_hashtag, (hashtags.get(hashtag), 'en', hashtag, ))
        conn.commit()
        print(f"<< {hashtags.get(hashtag)} >> updated")

    # file_path_2 = (base_path / "../utilities/final_translations_of_foreign_characters_2.csv").resolve()
    # with open(file_path_2, 'r') as file2:
    #     csv_reader = reader(file2)
    #     for row in csv_reader:
    #         cur_value = row[0].split(';')
    #         print(cur_value)
    #         hashtags[cur_value[0]] = cur_value[1]

    # for hashtag in hashtags:
    #     cur.execute(sql_update_hashtag, (hashtags.get(hashtag), 'en', hashtag, ))
    #     conn.commit()
    #     print(f"<< {hashtag} >> updated")


def fill_empty_translations():
    sql = """select name, id from hashtags where language = 'en' and translation_english = ''"""
    sql_update_hashtag = """update hashtags set translation_english = %s where id = %s"""
    cur.execute(sql, ())
    results = cur.fetchall()
    for item in results:
        cur.execute(sql_update_hashtag, (item[0].strip(), item[1], ))
        conn.commit()


def get_all_hashtags(test_users):
    base_path = Path(__file__).parent.parent
    sql_hashtags_test = """select distinct p.id, phr.translation_english
        from (select phr1.postid, h.id, h.translation_english
                from d1rpgcvqcran0q.public.post_hashtag_relation phr1 join 
                    d1rpgcvqcran0q.public.hashtags h on phr1.hashtagid = h.id) phr join
        (select id from d1rpgcvqcran0q.public.posts where testuserid = %s) p on p.id = phr.postid"""

    sql_hashtags_training = """select distinct p.id, phr.translation_english
        from (select phr1.postid, h.id, h.translation_english
                from d1rpgcvqcran0q.public.post_hashtag_relation phr1 join 
                    d1rpgcvqcran0q.public.hashtags h on phr1.hashtagid = h.id) phr join
        (select id from d1rpgcvqcran0q.public.posts where testuserid not in (53, 54, 91, 92, 123, 124)) p on p.id = phr.postid"""

    test_hashtags = {}

    for user in test_users:
        # form test data set
        cur.execute(sql_hashtags_test, (user, ))
        results_test = cur.fetchall()
        for item in results_test:
            post_id = item[0]
            cur_hashtag = item[1].strip()
            if post_id in test_hashtags.keys() and cur_hashtag not in test_hashtags.get(post_id):
                test_hashtags[post_id].append(cur_hashtag)
            else:
                test_hashtags[post_id] = [cur_hashtag]

    # concat_test_hashtags = {}
    # for post_id in test_hashtags.keys():
    #     concat_hashtag = ' '.join(hashtag for hashtag in test_hashtags.get(post_id))
    #     if post_id not in concat_test_hashtags.keys():
    #         concat_test_hashtags[post_id] = concat_hashtag

    file_test_data_set = (base_path / "utilities/test_data_set.csv").resolve()
    with open(file_test_data_set, 'w') as f:
        w = csv.writer(f)
        for row in test_hashtags.items():
            w.writerow(row)

    training_hashtags = {}
    # for training data set
    cur.execute(sql_hashtags_training, ())
    results_training = cur.fetchall()
    for item in results_training:
        post_id = item[0]
        cur_hashtag = item[1].strip()
        if post_id in training_hashtags.keys() and cur_hashtag not in training_hashtags.get(post_id):
            training_hashtags[post_id].append(cur_hashtag)
        elif post_id not in test_hashtags.keys():
            training_hashtags[post_id] = [cur_hashtag]

    # concat_training_hashtags = {}
    # for post_id in training_hashtags.keys():
    #     concat_hashtag = ' '.join(hashtag for hashtag in training_hashtags.get(post_id))
    #     if post_id not in concat_training_hashtags.keys():
    #         concat_training_hashtags[post_id] = concat_hashtag

    file_training_data_set = (base_path / "utilities/training_data_set.csv").resolve()
    with open(file_training_data_set, 'w') as f:
        w = csv.writer(f)
        for row in training_hashtags.items():
            w.writerow(row)


def filter_hashtags_fyp():
    base_path = Path(__file__).parent.parent
    sql_get_fyp_hashtags = """select distinct h.id, h.translation_english from hashtags h
        join (select phr1.hashtagid, phr1.postid, p.id, p.desc_iteminfo, p.testrunid, p.testuserid from post_hashtag_relation phr1
                join posts p on p.id = phr1.postid) phr
        on h.id = phr.hashtagid
    where h.name like '%fyp%' or h.name like '%foryou%'"""
    cur.execute(sql_get_fyp_hashtags, )
    results = cur.fetchall()
    file_fyp_hashtags = (base_path / "hashtags_to_ignore.json").resolve()
    fyp_hashtags_to_ignore = {}
    for item in results:
        fyp_hashtags_to_ignore[item[0]] = item[1].strip()

    with open(file_fyp_hashtags, 'w') as f:
        json.dump(fyp_hashtags_to_ignore, f)


def get_rescue_value(users, start_end_runs, drops_set, noise):
    sql_insert_drops = """insert into drop_rescue(user_1, user_2, drop_test_run_id, rescue_overlap, rescue_posts, 
        rescue_hashtags, rescue_content_creators, rescue_sounds) values(%s,%s,%s,%s,%s,%s,%s,%s)"""

    for run_pair1 in start_end_runs:
        user_1 = users[start_end_runs.index(run_pair1)][0]
        user_2 = users[start_end_runs.index(run_pair1)][1]
        final_avg_overlapping = []
        final_avg_diff_posts = []
        final_avg_diff_hashtags = []
        final_avg_diff_content_creators = []
        final_avg_diff_sounds = []
        for run_pair2 in run_pair1:
            avg_overlapping = 0
            avg_diff_posts = 0
            avg_diff_hashtags = 0
            avg_diff_content_creators = 0
            avg_diff_sounds = 0
            noise_posts = noise.get('avg_difference_posts')
            noise_content_creators = noise.get('avg_different_content_creators')
            noise_hashtags = noise.get('avg_different_hashtags')
            noise_sounds = noise.get('avg_different_sounds')
            for run in run_pair2:
                different_posts, overlapping_posts, total_posts_user_1, total_posts_user_2 = \
                    get_number_of_different_overlapping_posts(test_user_1=user_1, test_run_user_1=run, test_user_2=user_2,
                                                              test_run_user_2=run, noise=noise_posts)

                different_hashtags = get_number_of_different_hashtags(test_user_1=user_1, test_run_user_1=run,
                                                                      test_user_2=user_2, test_run_user_2=run,
                                                                      noise=noise_hashtags)
                different_content_creators = get_number_of_different_content_creators(test_user_1=user_1, test_user_2=user_2,
                                                                                      test_run_user_1=run, test_run_user_2=run,
                                                                                      noise=noise_content_creators)
                different_sounds = get_number_of_different_sound(test_user_1=user_1, test_user_2=user_2, test_run_user_1=run,
                                                                 test_run_user_2=run, noise=noise_sounds)
                avg_overlapping += overlapping_posts
                avg_diff_posts += different_posts
                avg_diff_hashtags += different_hashtags
                avg_diff_content_creators += different_content_creators
                avg_diff_sounds += different_sounds
            final_avg_overlapping.append(round(avg_overlapping / 2, 4))
            final_avg_diff_posts.append(round(avg_diff_posts / 2, 4))
            final_avg_diff_hashtags.append(round(avg_diff_hashtags / 2, 4))
            final_avg_diff_content_creators.append(round(avg_diff_content_creators / 2, 4))
            final_avg_diff_sounds.append(round(avg_diff_sounds / 2, 4))
        for i in range(len(final_avg_overlapping)):
            print(f"Copy / Paste for user {user_1, user_2}: {final_avg_overlapping[i]}, {final_avg_diff_posts[i]}, "
                  f"{final_avg_diff_hashtags[i]}, {final_avg_diff_content_creators[i]}, {final_avg_diff_sounds[i]}")
            for drop in drops_set[start_end_runs.index(run_pair1)][i]:
                cur.execute(sql_insert_drops, (user_1, user_2, drop, final_avg_overlapping[i], final_avg_diff_posts[i],
                                               final_avg_diff_hashtags[i], final_avg_diff_content_creators[i],
                                               final_avg_diff_sounds[i], ))
                conn.commit()
                print(f"**** drop {drop} updated ****")


if __name__ == '__main__':
    # fill_empty_translations()
    # clean_hashtags()

    control_group_5_batches = [[72, 73], [74, 75], [95, 96]]
    control_group_3_batches = [[125, 126], [137, 138], [139, 140], [141, 142], [143, 144], [147, 148], [149, 150]]
    noise_all_computation_5, noise_run_computation_5, noise_avg_overall_runs_overall_users_computation_5 = \
        compute_noise_control_scenarios(control_group_5_batches, 5)
    noise_all_computation_3, noise_run_computation_3, noise_avg_overall_runs_overall_users_computation_3 = \
        compute_noise_control_scenarios(control_group_3_batches[:len(control_group_3_batches) - 2], 3, False)
    # get_rescue_value([[61, 62], [63, 64]], [[[1358, 1442]], [[1359, 1447]]], [[[1384, 1397, 1406]], [[1374, 1385, 1398, 1407]]],
    #                  noise_avg_overall_runs_overall_users_computation_5)

    get_rescue_value([[157, 158]], [[[2717]]],
                     [[[2752, 2772, 2787]]],
                     noise_avg_overall_runs_overall_users_computation_3)


    get_rescue_value([[123, 124], [113, 114], [135, 136], [115, 116], [117, 118], [49, 50], [53, 54], [155, 156],
                      [83, 84], [85, 86], [87, 88], [145, 146], [91, 92], [151, 152]],
                     [[[2298, 2516]], [[2294]], [[2299, 2468]],
                      [[1824, 1945], [2292]], [[2297, 2375]], [[1341]], [[1349]], [[2748, 2809]],
                      [[1683, 1783]], [[1684, 1784]], [[1685, 1785]], [[2286, 2425], [2711, 2814]], [[1666, 1787]],
                      [[2712]]],
                     [[[2325, 2376, 2427, 2467, 2492]], [[2322]], [[2326, 2377, 2428]], [[1870, 1915], [2323, 2374]],
                      [[2324]],
                      [[1363, 1378]], [[1365, 1380]], [[2768, 2786]], [[1699, 1755]], [[1705, 1756]], [[1706, 1757]],
                      [[2320],
                       [2750, 2770, 2784]], [[1687, 1709, 1759]], [[2751, 2771, 2785]]],
                     noise_avg_overall_runs_overall_users_computation_3)
    get_all_hashtags([53, 54, 91, 92, 123, 124])
    # clean_hashtags()
    filter_hashtags_fyp()
    update_hashtags()
    translate_hashtag_strings()
    # print(generate_combinations([103, 104, 107, 108]))
    # update_proxies_to_usermanager()
    # proxy_maintenance_init('US')

    # update_tables(110, 'fr', 134)
    # # update_overlapping_post_test_results_with_new_values(test_run=1843, test_users=[109, 110])
    # get_author_uniqueids([135, 136])
    # get_music_ids([117, 118])
    # get_authors()
    # get_music()
    # copy_language_iso()
    clean_internal_music_ids()

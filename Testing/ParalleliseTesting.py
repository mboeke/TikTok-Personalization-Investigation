import concurrent.futures

from src.WebHelper import *
from src.Proxy import *
from src.DataStoring import *
from src.TestRun import TestRun

base_path = Path(__file__).parent

def get_test_data():
    database = DatabaseHelper()
    file_path = (base_path / "../Testing/TestSets/test_user_167.json").resolve()
    with open(file_path) as file:
        test_json = json.load(file)

    # test_data with settings from database
    # test_data = []
    # for test_set in test_json:
    #     if test_json.get(test_set).get('login'):
    #         test_json.get(test_set)['phone_number'] = database.get_phone_number(test_user_id=test_json.get(
    #             test_set).get('test_user_id'))
    #         test_json.get(test_set)['country_phone_number_prefix'] = database.get_country_phone_number_prefix(
    #             test_user_id=test_json.get(test_set).get('test_user_id'))
    #     test_json.get(test_set)['proxy'] = {
    #         "proxy_username": "PLACEHOLDER", "proxy_password": "PLACEHOLDER",
    #         "proxy_host": database.get_proxy_host(test_user_id=test_json.get(test_set).get('test_user_id')),
    #         "proxy_port": database.get_proxy_port(test_user_id=test_json.get(test_set).get('test_user_id')),
    #         "country": database.get_proxy_country(test_user_id=test_json.get(test_set).get('test_user_id'))}
    #     test_data.append(test_json.get(test_set))
    # return test_data

    # test test setting data
    test_data = [test_json.get('167')]
    for test_set in test_data:
        if test_set.get('login'):
            test_set['phone_number'] = database.get_phone_number(test_user_id=test_set.get('test_user_id'))
            test_set['country_phone_number_prefix'] = database.get_country_phone_number_prefix(
                test_user_id=test_set.get('test_user_id'))
        test_set['proxy'] = {
            "proxy_username": "PLACEHOLDER", "proxy_password": "PLACEHOLDER",
            "proxy_host": database.get_proxy_host(test_user_id=167),
            "proxy_port": database.get_proxy_port(test_user_id=167),
            "country": database.get_proxy_country(test_user_id=167)
        }
        # c1 = 'US'
        # test_set['proxy'] = {"proxy_username": "PLACEHOLDER", "proxy_password": "PLACEHOLDER",
        #                      'proxy_host': 'PLACEHOLDER', 'proxy_port': 'PLACEHOLDER', 'country': c1}
    return test_data

    # for account creation purposes:
    # cur = [test_data[0]]
    # print(cur[0].get('test_user_id'))
    # return cur


def run_test(test_data):
    # setting start time
    start = time.time()

    # initializing logger
    file_path = (base_path / f"../DataAnalysis/console_logs/console_log_{test_data.get('test_run_id')}_user_"
                             f"{test_data.get('test_user_id')}.log").resolve()
    logging.basicConfig(filename=file_path, filemode='w')
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)
    logger.warning(f'Starting execution for testuser {test_data.get("test_user_id")}.')

    # initializing DatabaseHelper() object only once for test run
    database = DatabaseHelper()

    # initializing helper instance
    helper = WebHelper(test_user_id=test_data.get('test_user_id'),
                       test_run_id=test_data.get('test_run_id'),
                       logger=logger,
                       database=database,
                       phone_number=test_data.get('phone_number'),
                       country_phone_number_prefix=test_data.get('country_phone_number_prefix'),
                       reuse_cookies=test_data.get('reuse_cookies'),
                       proxy=test_data.get('proxy'),
                       browser_language=test_data.get("browser_language"))

    # triggering login for user via phone number only if "login" set true in test_data
    if test_data.get('login'):
        helper.login_user_phone()
        helper.handle_banners()

    # trigger handling of banners
    helper.handle_banners()

    # pause video until actually watching
    helper.pause_video()

    # pause first video
    if test_data.get('collecting_data_for_first_posts'):
        helper.handle_banners()
        helper.pause_video()

    # set cookies
    if test_data.get('reuse_cookies'):
        helper.set_cookies()

    # define number of batches to scroll through
    if len(test_data.get('number_of_posts_to_like_per_batch')) != 0 \
            or len(test_data.get('number_of_creators_to_follow_per_batch')) != 0 \
            or len(test_data.get('number_of_posts_to_watch_longer_per_batch')):
        if test_data.get('number_of_batches') != max(len(test_data.get('number_of_posts_to_like_per_batch')),
                                                     len(test_data.get('number_of_creators_to_follow_per_batch')),
                                                     len(test_data.get('number_of_posts_to_watch_longer_per_batch'))):
            raise Exception("Number of batches to scroll through doesn't match!")
        else:
            number_of_batches = test_data.get('number_of_batches')
    else:
        number_of_batches = test_data.get('number_of_batches')

    # initializing data storing instance
    data_storing = DataStoring(helper=helper,
                               logger=logger,
                               database=database,
                               number_of_batches=number_of_batches,
                               test_user_id=test_data.get('test_user_id'),
                               test_run_id=test_data.get('test_run_id'))

    # trigger handling of banners
    helper.handle_banners()

    # handling first set of posts
    data_storing.get_separate_posts_data(collecting_data_for_first_posts=test_data.get("collecting_data_for_first_posts"))

    # handling remaining posts, scrolling through batches
    data_storing.get_request_posts_data(time_to_look_at_post_action=test_data.get('time_to_look_at_post_action'),
                                        time_to_look_at_post_normal=test_data.get('time_to_look_at_post_normal'),
                                        number_of_posts_to_like_per_batch=test_data.get('number_of_posts_to_like_per_batch'),
                                        number_of_creators_to_follow_per_batch=test_data.get('number_of_creators_to_follow_per_batch'),
                                        number_of_posts_to_watch_longer_per_batch=test_data.get('number_of_posts_to_watch_longer_per_batch'),
                                        posts_with_hashtag_to_like=test_data.get('posts_with_hashtag_to_like'),
                                        posts_with_hashtag_to_watch_longer=test_data.get('posts_with_hashtag_to_watch_longer'),
                                        posts_of_content_creators_to_like=test_data.get('posts_of_content_creators_to_like'),
                                        posts_of_music_ids_to_like=test_data.get('posts_of_music_ids_to_like'))
    helper.close_driver()
    # commencing shut down of test run: unflagging used proxy, closing driver, storing collected data, computing
    # duration and storing it for corresponding testrun
    helper.database.unflag_proxy(proxy_host=test_data['proxy']['proxy_host'],
                                 proxy_port=test_data['proxy']['proxy_port'])
    data_storing.store_collected_data()
    duration = time.time() - start
    test_data['duration'] = (duration / 60)
    logger.warning(f'Execution for testuser {test_data.get("test_user_id")} completed in {duration} seconds '
                   f'({duration / 60} minutes).')
    return test_data


if __name__ == '__main__':
    tests = get_test_data()
    with TestRun(test_data=tests) as test_run:
        for test in tests:
            test['test_run_id'] = test_run.test_run_id
        with concurrent.futures.ProcessPoolExecutor() as executor:
            test_data_results = executor.map(run_test, tests)
        test_user_ids = []
        batch_size = 0
        for test in test_data_results:
            test_run.store_test_duration(duration=test.get('duration'), test_user_id=test.get('test_user_id'))
            test_user_ids.append(test.get('test_user_id'))
            batch_size = test.get('number_of_batches')
        # update analysis table
        # update_overlapping_post_test_results_with_new_values(test_run=test_run.test_run_id, test_users=test_user_ids,
        #                                                      batch_size=batch_size)

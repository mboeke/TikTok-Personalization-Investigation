import time

import brotli
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from src.DatabaseHelper import *


class DataStoring:
    """
    class extracting data from TikTok and storing it in Database accordingly
    :param test_data: specifying test
    :param helper: Helper() object with current webdriver object
    :param number_of_batches: number of batches that shall be stored
    """

    def __init__(self, helper, logger, database, number_of_batches, test_user_id, test_run_id):
        self.database = database
        self.helper = helper
        self.logger = logger
        self.number_of_batches = number_of_batches
        self.test_user_id = test_user_id
        self.test_run_id = test_run_id
        self.posts_seen_due_to_separate_posts = []
        self.posts_of_current_batch = []
        self.already_checked_posts = []
        self.current_total_posts = []
        self.separate_posts_not_stored = []
        self.request_post_ids = []
        self.request_posts_not_on_feed = []
        self.temp_data_collection = {}
        self.first_post_href = None
        self.get_request_posts_ids()  # get ids of posts in api/recommend/item_list

    def get_request_posts_ids(self):
        """
        Get the list of post ids that are part of request api/recommend/item_list json
        :param
        :return: list of post ids
        """
        data_requests = self.get_request_response()
        # it may happen that TikTok only displays 2 posts when loading the website for the first time,
        # then no requests for post data of first batch are sent, thus the bot has to trigger them manually by scrolling
        # down a bit and back to the top as soon as the requests appeared
        while len(data_requests) < 0:
            self.helper.move_to_next_post_trigger_requests()
            data_requests = self.get_request_response()
            if len(data_requests) > 0:
                # check that at least first post of data is visible in ForYou feed, then the remaining ones are as well
                # check by searching for comment of first post
                if self.helper.driver.find_element(By.XPATH, f"//img[@alt={data_requests[0][0].get('desc')}]").is_displayed():
                    self.helper.driver.find_element_by_tag_name('body').send_keys(Keys.CONTROL + Keys.HOME)
        for data in data_requests:
            for post in data:
                self.request_post_ids.append(post.get('id'))

    def get_request_response(self):
        data_requests = []
        try:
            for request in self.helper.driver.requests:
                # look for correct request url
                # check if request actually holds data, if not don't use it
                if "https://m.tiktok.com/api/recommend/item_list/?aid=1988&app_name=tiktok_web&device_platform=web_pc" \
                        in request.url and 'body' in request.response.__dir__():
                    if len(request.response.body) > 0:
                        data_requests.append(json.loads(request.response.body)['itemList'])
            return data_requests
        except json.decoder.JSONDecodeError:
            raise json.decoder.JSONDecodeError
        except BaseException:
            raise BaseException('API/recommend/item_list request seems to be empty.')

    def get_api_recommend_item_list_requests(self):
        try:
            api_recommend_itemList_requests = []
            for request in self.helper.driver.requests:
                try:
                    # select the correct request among all: "api/recommend/item_list"
                    # make sure response contains non-empty "body" attribute
                    if "https://m.tiktok.com/api/recommend/item_list/?aid=1988&app_name=tiktok_web&device_platform" \
                       "=web_pc" in request.url and 'body' in request.response.__dir__():
                        if len(request.response.body) > 0:
                            api_recommend_itemList_requests.append({'request': request,
                                                                    'request_body': json.loads(request.response.body)
                                                                    ['itemList']})
                except json.decoder.JSONDecodeError:
                    raise json.decoder.JSONDecodeError
            return api_recommend_itemList_requests
        except BaseException:
            raise BaseException("No api/recommmend/item_list requests found.")

    def get_separate_posts_data(self, collecting_data_for_first_posts=True):
        """
        Store data for each post that is not in request_post_ids, thus for each post for which data must be extracted
        on separate tab
        :param collecting_data_for_first_posts: Indicates whether first set of posts which is not included in
        api/recommend/item_list request shall be stored or not, because the additional watching time for each of those
        posts may distort the test results
        :return:
        """
        post_urls = self.helper.driver.find_elements(By.XPATH, '//span[@class="lazyload-wrapper"]/div/div/div[5]/div/a')
        for url in post_urls:
            curr_video_URL = url.get_attribute('href')
            curr_post_id = curr_video_URL.rsplit('/', 1)[1]
            if curr_post_id not in self.request_post_ids:
                if collecting_data_for_first_posts:  # check if separate posts shall currently be stored or not
                    self.helper.open_new_tab(curr_video_URL)
                    self.helper.pause_video(href=curr_video_URL)  # pausing video on separate tab
                    self.posts_seen_due_to_separate_posts.append(curr_post_id)
                    for request in self.helper.driver.requests:
                        try:
                            if request.url == curr_video_URL and len(request.response.body) > 0:
                                # save data and decode response body in utf-8 format
                                body = request.response.body
                                try:
                                    html_data = body.decode('utf-8')
                                except UnicodeDecodeError as err:
                                    self.logger.warning(err)
                                    pass
                                try:
                                    decompressed_str = brotli.decompress(body)
                                    html_data = decompressed_str.decode('utf-8')
                                except Exception:
                                    self.logger.warning('Request response can not be decoded.')
                                    raise Exception
                                soup = BeautifulSoup(html_data, 'lxml')
                                data = json.loads(soup.find('script', id="__NEXT_DATA__").string)
                                post_data = data['props']['pageProps']['itemInfo']['itemStruct']
                                # post and batch position need to be updated separately for separate posts
                                self.helper.current_post_id = post_data.get('id')
                                self.helper.update_post_and_batch_positions(optional_batch_position=0)
                                self.temp_store_data(data=post_data)
                        except AttributeError as err:  # handling AttributeError of request.response.body is empty,
                            self.logger.warning(err)  # thus of 'NoneType' and has no attribute body
                            if request.response.body is None:
                                self.store_data_if_visible(
                                    request_data=json.loads(requests.get(request.url).text)['itemList'])
                            else:
                                raise AttributeError(err)
                    self.helper.close_second_tab()

                    # after closing separate tab first video should be paused again if it is playing
                    self.helper.pause_video(curr_video_URL)
                else:
                    self.separate_posts_not_stored.append(curr_post_id)
                    self.helper.current_post_id = curr_post_id
                    self.helper.update_post_and_batch_positions(optional_batch_position=0)
                    self.logger.warning(f"Post with url: {curr_video_URL} not stored for user: {self.test_user_id}.")
            time.sleep(2)
        self.logger.warning(f"Following separate posts were not stored for user {self.test_user_id} in test run "
                            f"{self.test_run_id}: {self.separate_posts_not_stored}")

        # after handling separate posts, the first post shall still be paused as it was already watched in the meantime

    def get_request_posts_data(self,
                               time_to_look_at_post_action,
                               time_to_look_at_post_normal,
                               number_of_posts_to_like_per_batch=None,
                               number_of_creators_to_follow_per_batch=None,
                               number_of_posts_to_watch_longer_per_batch=None,
                               posts_with_hashtag_to_like=None,
                               posts_with_hashtag_to_watch_longer=None,
                               posts_of_content_creators_to_like=None,
                               posts_of_music_ids_to_like=None):
        """
        Collect data for those posts that are listed in api/recommend/item_list.
        Like posts in a batch if specified as such in number_of_posts_to_like_per_batch,
        e.g. number_of_posts_to_like_per_batch = [0, 1, 0] => only like 1 post in 2nd batch
        :param
        :return:
        """
        if posts_with_hashtag_to_like is None:
            posts_with_hashtag_to_like = []
        if posts_with_hashtag_to_watch_longer is None:
            posts_with_hashtag_to_watch_longer = []
        if posts_of_content_creators_to_like is None:
            posts_of_content_creators_to_like = []
        if posts_of_music_ids_to_like is None:
            posts_of_music_ids_to_like = []

        for batch in range(0, self.number_of_batches):

            # get posts of current batch
            self.current_total_posts = self.helper.driver.find_elements_by_xpath("//span[@class='lazyload-wrapper']")

            # get the number of posts of current batch, if number of posts == 0 bot needs to scroll one more to trigger
            # loading of next batch ; update list of posts detected after running through first batch by deducting
            # number of posts already visited with list of post_batch_position
            self.update_posts_of_current_batch()
            while self.posts_of_current_batch == 0:
                self.helper.move_to_next_post()
                self.current_total_posts = self.helper.driver.find_elements_by_xpath(
                    "//span[@class='lazyload-wrapper']")
                self.update_posts_of_current_batch()

            # update already_checked_posts
            self.already_checked_posts.extend(self.posts_of_current_batch)

            # store info in logger and print it for monitoring purposes
            self.logger.warning(f"### Moving to batch number {batch} with {len(self.posts_of_current_batch)} posts for "
                                f"test user {self.test_user_id} in test run {self.test_run_id}.")
            print(f"### Moving to batch number {batch} with {len(self.posts_of_current_batch)} posts for "
                  f"test user {self.test_user_id} in test run {self.test_run_id}.")

            # trigger like if applicable
            number_of_posts_to_like = 0
            if number_of_posts_to_like_per_batch is not None and 0 < len(number_of_posts_to_like_per_batch):
                if len(number_of_posts_to_like_per_batch) > batch:
                    number_of_posts_to_like = number_of_posts_to_like_per_batch[batch]

            # trigger follow creators if applicable
            number_of_creators_to_follow = 0
            if number_of_creators_to_follow_per_batch is not None and 0 < len(number_of_creators_to_follow_per_batch):
                if len(number_of_creators_to_follow_per_batch) > batch:
                    number_of_creators_to_follow = number_of_creators_to_follow_per_batch[batch]

            # trigger watching posts longer if applicable
            number_of_posts_to_watch_longer = 0
            if number_of_posts_to_watch_longer_per_batch is not None and 0 < len(number_of_posts_to_watch_longer_per_batch):
                if len(number_of_posts_to_watch_longer_per_batch) > batch:
                    number_of_posts_to_watch_longer = number_of_posts_to_watch_longer_per_batch[batch]

            # scroll through batch and perform an action (like, follow etc.) if applicable
            # store on which posts an action was performed
            # posts with hashtags to like don't need a separate trigger handling as do like & follow
            self.helper.scroll_and_action(time_to_look_at_post_action=time_to_look_at_post_action,
                                          time_to_look_at_post_normal=time_to_look_at_post_normal,
                                          number_of_posts_to_like=number_of_posts_to_like,
                                          number_of_creators_to_follow=number_of_creators_to_follow,
                                          number_of_posts_to_watch_longer=number_of_posts_to_watch_longer,
                                          posts_with_hashtag_to_like=posts_with_hashtag_to_like,
                                          posts_with_hashtag_to_watch_longer=posts_with_hashtag_to_watch_longer,
                                          posts_of_content_creators_to_like=posts_of_content_creators_to_like,
                                          posts_of_music_ids_to_like=posts_of_music_ids_to_like,
                                          posts_of_current_batch=self.posts_of_current_batch,
                                          batch=batch,
                                          separate_posts_not_stored=self.separate_posts_not_stored,
                                          posts_seen_due_to_separate_posts=self.posts_seen_due_to_separate_posts)

        # iterate through all api/recommend/item_list requests
        request_list = self.get_api_recommend_item_list_requests()

        # close driver to prevent any further interaction
        self.helper.close_driver()

        # store data
        for request in request_list:
            data = request.get('request_body')
            try:
                self.store_data_if_visible(request_data=data)
            except AttributeError as err:  # handling AttributeError of request.response.body is empty,
                self.logger.warning(err)  # thus of 'NoneType' and has no attribute body
                if request.response.body is None:
                    self.store_data_if_visible(request_data=json.loads(requests.get(request.url).text)['itemList'])
                else:
                    raise AttributeError(err)
        self.logger.warning(f"For {self.test_user_id} in {self.test_run_id} posts where in the following order: "
                            f"{self.helper.post_batch_positions}.")

    def update_posts_of_current_batch(self):
        """
        Iterate through currently collected posts from DOM and append posts_of_current_batch with those posts that have
        not yet been visited.
        :return:
        """
        # current_total_posts and already_checked_posts to access post_id correctly?
        self.posts_of_current_batch = []
        for curr_post in self.current_total_posts:
            if curr_post not in list(post for post in self.already_checked_posts):
                self.posts_of_current_batch.append(curr_post)

    def store_data_if_visible(self, request_data):
        """
        Store the data of the post only if the post is actually shown on the ForYou feed.
        :param request_data:
        :return:
        """
        for post_data in request_data:
            post_id = post_data.get('id')
            try:
                if post_id in self.helper.post_batch_positions:
                    # If store_data_if_visible() called from get_separate_posts_data() then post_batch_positions need to
                    # be updated separately
                    if post_id not in self.helper.post_batch_positions:
                        self.helper.current_post_id = post_id
                        self.helper.update_post_and_batch_positions(optional_batch_position=0)

                    # Store data from post in temp_data_collection list.
                    self.temp_store_data(data=post_data)
                else:  # finding issue why some posts not stored in db
                    self.logger.warning(f"Post {post_id} is not displayed on feed and thus not stored in db.")
            except:
                if post_id not in self.request_posts_not_on_feed:
                    self.request_posts_not_on_feed.append(post_id)

    def store_data_from_request(self, post_url, batch_position):
        """
        Get data from post url and store it
        :param batch_position:
        :param post_url:
        :return:
        """
        request = requests.get(post_url)
        html_data = request.content.decode('utf-8')
        soup = BeautifulSoup(html_data, 'lxml')
        data = json.loads(soup.find('script', id="__NEXT_DATA__").string)
        self.post_position = self.database.store_data(
            data=data['props']['pageProps']['itemInfo']['itemStruct'], post_position=self.post_position,
            batch_position=batch_position, test_user_id=self.test_user_id, test_run_id=self.test_run_id)

    def temp_store_data(self, data):
        """
        Add the collected data to be stored later to self.temp_data_collection = [{data:, post}].
        :return:
        """
        # Only store those posts in self.temp_data_collection that have not been stored yet.
        if data.get('id') not in self.temp_data_collection.keys():
            self.logger.warning(f"Storing post {data.get('id')}")  # finding issue why some posts not stored in db
            self.temp_data_collection[data.get('id')] = data
        else:  # finding issue why some posts not stored in db
            self.logger.warning(f"Post {data.get('id')} is already in temp_data_collection list and thus not added again.")

    def store_collected_data(self):
        """
        After having run through posts of all batches, and the chrome driver being closed, this method stores the
        collected data.
        :return:
        """
        # make sure chrome driver is closed before starting to store data
        # for every data package, call database.store_data
        # update post_position here as before in store_data_if_visible()
        try:
            if self.helper.driver is None:
                for post in self.temp_data_collection.keys():
                    self.database.store_data(
                        data=self.temp_data_collection.get(post),
                        post_position=self.helper.post_batch_positions.get(post).get('post_position'),
                        batch_position=self.helper.post_batch_positions.get(post).get('batch_position'),
                        test_user_id=self.test_user_id,
                        test_run_id=self.test_run_id
                    )
                    self.logger.warning(f"Post {post} stored in db.")
                self.logger.warning(f'Data storing completed for test run: {self.test_run_id} and test user: {self.test_user_id}.')

                if len(self.helper.posts_liked) > 0:
                    # store liked posts
                    for liked_post in self.helper.posts_liked:
                        self.database.update_liked_post(postid=liked_post, testuserid=self.test_user_id,
                                                        testrunid=self.test_run_id)
                    self.logger.warning(f"Storing posts that were liked completed for test run: {self.test_run_id} "
                          f"and test user: {self.test_user_id}.")

                if len(self.helper.creators_followed) > 0:
                    # store follower
                    for followed_post in self.helper.creators_followed:
                        author_id = int(self.temp_data_collection.get(followed_post).get('author').get('id'))
                        self.database.update_followed_post(author_id=author_id, post_id=followed_post,
                                                           test_user_id=self.test_user_id, test_run_id=self.test_run_id)

                # store posts that were watched longer if applicable
                if len(self.helper.posts_watched_longer) > 0:
                    for post in self.helper.posts_watched_longer.keys():
                        self.database.store_longer_watched_post(post_id=post,
                                                                test_user_id=self.test_user_id,
                                                                test_run_id=self.test_run_id,
                                                                time_watched=self.helper.posts_watched_longer.get(post),
                                                                percentage_watched=self.helper.time_to_look_at_post_action)

                # update internal music ids
                # self.database.update_internal_music_ids()
            else:
                raise BaseException(f'Driver Session is not closed, data cannot be stored for test run: '
                                    f'{self.test_run_id} and test user: {self.test_user_id}.')
        except BaseException as e:
            self.logger.warning(e)
            raise BaseException


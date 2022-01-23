import itertools
import json
import logging
import random
from pathlib import Path

import psycopg2
import numpy as np
import time

import selenium
import seleniumwire
import pickle
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from typing import Optional
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from langdetect import detect

from src.DataStoring import DataStoring
from src.SMSHandler import SMSHandler
from src.Proxy import *


class WebHelper:
    """
    Class with functions to navigate TikTok's UI: login, handle login box, setup driver connection via proxy

    :param proxy: dictionary of {proxy_username, proxy_password, proxy_host, proxy_port} to create link of the form
    http://proxy_username:proxy_password@proxy_host:proxy_port, e.g. {'proxy_username': "auamyynt-dest",
    'proxy_password': "j5u77rwhbdnj", 'proxy_host': "45.95.96.132", 'proxy_port': 8691}
    """

    def __init__(self, test_user_id, test_run_id, logger, database, phone_number, country_phone_number_prefix,
                 reuse_cookies=False, proxy=None, browser_language="en", **kwargs):
        self.BASE_URL = "https://tiktok.com/"
        self.base_path = Path(__file__).parent
        self.logger = logger
        self.test_user_id = test_user_id
        self.test_run_id = test_run_id
        self.database = database
        self.phone_number = phone_number
        self.country_phone_number_prefix = country_phone_number_prefix
        self.reuse_cookies = reuse_cookies,
        self.password = self.database.get_password(test_user_id=self.test_user_id)
        self.driver: Optional[webdriver] = None
        self.second_url = None
        self.proxy = proxy
        self.browser_language = browser_language
        self.find_correct_driver()
        self.tiktok_loading_container_visible = False
        self.posts_of_current_batch = []
        self.current_post = None
        self.current_post_href = None
        self.current_post_id = None
        self.current_post_href_web_element = None
        self.post_batch_positions = {}
        self.post_position = 0
        self.time_to_look_at_post_normal = 0,
        self.time_to_look_at_post_action = 0
        self.batch = 0
        self.posts_liked = []
        self.creators_followed = []
        self.separate_posts_not_stored = []
        self.random_selection_likes = []
        self.random_selection_followers = []
        self.random_selection_watching = []
        self.posts_with_hashtag_to_like = []
        self.posts_with_hashtag_to_watch_longer = []
        self.posts_of_content_creators_to_like = []
        self.posts_of_music_ids_to_like = []
        self.posts_watched_longer = {}
        self.durations_from_posts = {}
        self.already_seen_content_creators = []
        self.already_seen_music = []

    def start_session(self):
        """
        Starting the Selenium driver session
        :param self:
        :return: self including driver
        """
        try:
            # bypassing detection of automated software testing
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option('prefs', {'intl.accept_languages': f'{self.browser_language}'})
            chrome_options.add_argument(f'--lang={self.browser_language}')

            sizes = list(itertools.combinations([800, 825, 850, 875, 900], 2))
            random_size = random.choice(sizes)
            chrome_options.add_argument(f'--window-size={random_size[0]},{random_size[1]}')
            chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                        "(KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36")

            chrome_options.add_argument('incognito')

            # use proxy if provided:
            options = {}
            if self.proxy is not None:
                url = "{proxy_username}:{proxy_password}@{proxy_host}:{proxy_port}".format(
                    proxy_username=self.proxy['proxy_username'], proxy_password=self.proxy['proxy_password'],
                    proxy_host=self.proxy['proxy_host'], proxy_port=self.proxy['proxy_port'])
                options = {
                    'proxy': {
                        'http': 'http://' + url,
                        'https': 'https://' + url,
                        'no_proxy': 'localhost,127.0.0.1'
                    }
                }

            # check if ".exe" appendix necessary or not depending on machine
            file_path_db = (base_path / "../utilities/db_credentials.json").resolve()
            with open(file_path_db) as file:
                db_credentials = json.load(file)
            if db_credentials.get('user') == 'PLACEHOLDER':
                # initializing web driver
                self.driver = webdriver.Chrome(chrome_options=chrome_options, seleniumwire_options=options,
                                               executable_path='PLACEHOLDER')
            else:
                file_path_chromedriver = (self.base_path / "../chromedriver.exe").resolve()
                # initializing web driver
                self.driver = webdriver.Chrome(chrome_options=chrome_options, seleniumwire_options=options,
                                               executable_path=file_path_chromedriver)

            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.get(self.BASE_URL)
            self.check_for_random_verification()
        except (ConnectionAbortedError, seleniumwire.thirdparty.mitmproxy.exceptions.TcpDisconnect) as err:
            self.logger.warning(err)
            self.logger.warning('\n New driver session with new proxy initialized.')

            # get new proxy
            new_proxy_host, new_proxy_port2 = get_db_proxy(self.proxy.get('country'),
                                                           {'proxy_host': self.proxy['proxy_host'],
                                                            'proxy_port': self.proxy['proxy_port']})
            # deactivate proxy as blocked
            self.database.deactivate_proxy_in_db(self.proxy['proxy_host'], self.proxy['proxy_port'])
            # update proxy used by current session, close current driver, & reinitialize driver session
            self.proxy['proxy_host'] = new_proxy_host
            self.proxy['proxy_port'] = new_proxy_port2
            self.close_driver()
            self.start_session()

    def find_correct_driver(self):
        """
        Close and re-open driver until correctl HTML DOM loaded. TikTok seem to occasionally load DOM for mobile
        application as first div after body indicates through <div id="app">. However, collecting data for this DOM is
        very time consuming.
        :return:
        """
        is_pc_version = False
        while not is_pc_version:
            self.start_session()
            html = self.driver.find_element_by_xpath('//html')
            if html.get_attribute('pc') == 'yes':
                is_pc_version = True
            else:
                self.close_driver()
        if self.driver.find_element_by_xpath('//div[@class="tt-feed"]').__sizeof__() < 0:
            self.driver.refresh()

    def check_for_random_verification(self):
        """
        Sometimes TikTok requires random verification.
        :return:
        """
        time.sleep(3)
        # wait until login slide gone, i.e. until I approved verification manually
        self.logger.warning(f"Random verification window detected for user {self.test_user_id}.")
        WebDriverWait(self.driver, 200).until(EC.invisibility_of_element_located((By.ID, 'tiktok-verify-ele')))
        time.sleep(3)
        self.logger.warning(f"Random verification window handled for user {self.test_user_id}.")

    def set_cookies(self):
        """
        Retrieve the user's cookies and set them in current driver session.
        :return:
        """
        cookies = self.database.get_cookies_db(test_user_id=self.test_user_id)
        print(cookies)
        if len(cookies) > 0:
            for cookie in cookies:
                for item in cookie.keys():
                    if cookie.get(item) is None:
                        cookie.get(item).pop(item)
                self.driver.add_cookie(cookie)
        else:
            print(f"User cookies db does not store cookies for user {self.test_user_id} yet.")
            self.logger.warning(f"User cookies db does not store cookies for user {self.test_user_id} yet.")

    def close_driver(self):
        """
        Once test case has been completed, driver shall be closed such that user gets logged out --> cleaning test case.
        :return:
        """
        if self.driver is not None:

            # add or update cookies db
            cur_cookies = self.database.get_cookies_db(test_user_id=self.test_user_id)
            new_cookies = self.driver.get_cookies()
            if len(cur_cookies) == 0:
                self.database.add_entry_user_cookies_db(cookies=new_cookies, test_user_id=self.test_user_id)
            elif len(cur_cookies) > 0:
                self.database.update_user_cookies_db(cookies=new_cookies, test_user_id=self.test_user_id)

            self.driver.close()
            self.driver: Optional[webdriver] = None

    def open_new_tab(self, url):
        """
        Open a second tab and call url.
        :param url:
        :return:
        """
        self.driver.execute_script("window.open('');")
        self.driver.switch_to.window(self.driver.window_handles[1])
        self.driver.get(url)
        self.second_url = url
        time.sleep(2)

    def close_second_tab(self):
        """
        Close currently opened second tab.
        :return:
        """
        if self.second_url is not None and self.driver.current_url == self.second_url:
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])

    def check_element_available(self, element):
        if element.size:
            return True
        else:
            return False

    def check_current_IP(self):
        self.driver.get("https://httpbin.org/ip")
        IP = self.driver.find_element(By.XPATH, "/html/body/pre").text
        self.logger.warning(IP)
        return IP

    def wait_for_element(self, seconds, xpath):
        """
        Wait X seconds for a specific element to appear

        :param seconds: seconds the driver shall wait
        :param xpath: xpath of the element for which the driver shall wait for
        :return:
        """
        try:
            # wait = WebDriverWait(self.driver, seconds)
            # element = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
            element = WebDriverWait(self.driver, seconds).until(
                EC.element_to_be_clickable((By.XPATH, xpath)))
        except:
            raise RuntimeError("Element with xpath %s could not be found in %int" % xpath, seconds)

    def wait_until_TikTok_loaded(self):
        try:
            time.sleep(1)
            if self.driver.find_element(By.XPATH, '//*[@class="tiktok-ui-loading-container tiktok-loading"]'). \
                    __sizeof__() > 0:
                self.tiktok_loading_container_visible = True
                while self.tiktok_loading_container_visible:
                    time.sleep(0.5)
                    if not self.driver.find_element(By.XPATH, '//*[@class="tiktok-ui-loading-container '
                                                              'tiktok-loading"]').is_displayed():
                        self.tiktok_loading_container_visible = False
                        time.sleep(0.5)
        except:
            time.sleep(0.5)

    def open_login_frame(self):
        """
        Function opens login module and selects "Use phone / email / username"
        :return:
        """
        # self.wait_until_TikTok_loaded()
        global username_selection, login_btn
        login_button_translation = {
            "en": "Log in",
            "de": "Anmelden",
            "es": "Iniciar sesión",
            "fr": "Connexion"
        }
        phone_email_username_for_login_translation = {
            "en": ["Use phone / email / username", "Phone / Email / Username"],
            "de": ["Telefonnr./E-Mail/Benutzernamen nutzen"],
            "es": ["Usar teléfono/correo/nombre de usuario"],
            "fr": ["Utiliser téléphone/e-mail/nom d'utilisateur"]
        }
        if self.driver is None:
            raise RuntimeError("No session active.")
        time.sleep(2)
        try:
            self.wait_for_element(20, f'//*[@id="main"]/div[1]//button[contains(text(),'
                                      f'"{login_button_translation.get(self.browser_language)}")]')
            login_btn = self.driver.find_element(By.XPATH, f'//*[@id="main"]/div[1]//button[contains(text(),'
                                                           f'"{login_button_translation.get(self.browser_language)}")]')
        except BaseException:
            self.logger.warning('Login_btn_1 not found.')
        if login_btn.__sizeof__() <= 0:
            try:
                self.wait_for_element(20, f'//*[@id="app"]/div[1]//button[contains(text(),'
                                          f'"{login_button_translation.get(self.browser_language)}")]')
                login_btn = self.driver.find_element_by_xpath(f'//*[@id="app"]/div[1]//button[contains(text(),'
                                                              f'"{login_button_translation.get(self.browser_language)}")]')
            except BaseException:
                self.logger.warning('Login_btn_2 not found.')
        try:
            if self.check_element_available(login_btn):
                login_btn.click()
                self.wait_until_TikTok_loaded()
                time.sleep(8)
                iframe = self.driver.find_element_by_xpath('//iframe')
                self.driver.switch_to.frame(iframe)
                # handling different iframe layouts for selecting email / username login option
                try:
                    username_sl1 = self.driver.find_element(
                        By.XPATH, f'//div[contains(text(), '
                                  f'"{phone_email_username_for_login_translation.get(self.browser_language)[0]}")]')
                    username_selection = username_sl1
                except BaseException:
                    username_sl1_found = False
                    self.logger.warning("username_sl1 couldn't be found")
                if username_selection.__sizeof__() < 0:
                    if len(phone_email_username_for_login_translation.get(self.browser_language)) > 1:
                        try:
                            username_sl2 = self.driver.find_element(
                                By.XPATH, f'//div[contains(text(), '
                                          f'"{phone_email_username_for_login_translation.get(self.browser_language)[1]}")]')
                            username_selection = username_sl2
                        except BaseException:
                            username_sl2_found = False
                            self.logger.warning("username_sl2 couldn't be found")
                if self.check_element_available(username_selection):
                    username_selection.click()
        except BaseException:
            raise BaseException("Can't open login module.")

    def login_user_mail(self, username_input, password):
        """
        Login a specific user on TikTok

        :param self:
        :param username: username that shall be logged in
        :param password: password corresponding to user account
        :return:
        """
        self.open_login_frame()
        with_email = self.driver.find_element(By.XPATH, '//*[@id="root"]/div/div[1]/form/div[1]/a')
        self.wait_for_element(10, '//*[@id="root"]/div/div[1]/form/div[1]/a')
        self.check_element_available(with_email)
        with_email.click()
        username_input_field = self.driver.find_element(By.XPATH, '//*[@placeholder="Email or Username"]')
        pw_input_field = self.driver.find_element(By.XPATH, '//*[@placeholder="Password"]')
        login_submit_btn = self.driver.find_element(By.XPATH, '//*[@id="root"]/div/div[1]/form/button')
        self.simulate_typing(username_input_field, username_input)
        self.simulate_typing(pw_input_field, password)
        login_submit_btn.click()
        time.sleep(15)
        # important to switch back out of iframe after session complete
        self.driver.switch_to.default_content()

    def login_user_phone(self):
        """
        Function logs in user with phone number.
        :param country_phone_number_prefix:
        :param phone_number:
        :return:
        """
        self.open_login_frame()
        select_login_with_password_translation = {
            "en": "Log in with password",
            "de": "Mit Passwort anmelden",
            "es": "Iniciar sesión con contraseña",
            "fr": "Connexion avec un mot de passe"
        }
        try:
            if self.driver.find_element_by_xpath(
                    f'//*[@id="root"]/div/div[1]/form/a[contains(text(), '
                    f'"{select_login_with_password_translation.get(self.browser_language)}")]').__sizeof__() > 0:
                # self.driver.find_element_by_xpath(
                #     f'//*[@id="root"]/div/div[1]/form/a[contains(text(), '
                #     f'"{select_login_with_password_translation.get(self.browser_language)}")]').click()
                # self.login_user_phone_with_password()

                self.login_user_with_verification_code()

        except selenium.common.exceptions.NoSuchElementException as e:
            pass

    def login_user_phone_with_password(self):
        """
        Log in user with password instead of using a verification code.
        :param phone_number:
        :param country_phone_number_prefix:
        :return:
        """
        phone_number_placeholder_translation = {
            "en": "Phone number",
            "de": "Telefonnummer",
            "es": "Número de teléfono",
            "fr": "Numéro de téléphone"
        }
        password_input_translation = {
            "en": "Password",
            "de": "Passwort",
            "es": "Contraseña",
            "fr": "Mot de passe"
        }
        phone_number_input_field = self.driver.find_element(
            By.XPATH,
            f'//form/div[2]/input[@placeholder="{phone_number_placeholder_translation.get(self.browser_language)}"]')
        self.select_country_phone_number_prefix(self.country_phone_number_prefix)
        self.simulate_typing(phone_number_input_field, self.phone_number)
        password_input_field = self.driver.find_element_by_xpath(
            f"//*[@id='root']/div/div[1]/form//input[@placeholder='{password_input_translation.get(self.browser_language)}']")
        self.simulate_typing(password_input_field, self.password)
        self.driver.find_element_by_xpath("//*[@id='root']/div/div[1]/form/button").click()
        time.sleep(5)
        # wait until login slide gone, i.e. until I approved verification manually
        WebDriverWait(self.driver, 200).until(EC.invisibility_of_element_located((By.ID, 'login_slide')))
        time.sleep(5)

    def login_user_with_verification_code(self):
        """
        Handle login with verification code, double check if successful, otherwise resend code.
        :param phone_number:
        :param phone_number_country_prefix_numerous:
        :return:
        """
        self.logger.warning(
            f"Log in with password not available, thus logging in with verification code for test "
            f"user {self.test_user_id} in test run {self.test_run_id}.")
        phone_number_input_field = self.driver.find_element(By.XPATH,
                                                            '//form/div[2]/input[@placeholder="Phone number"]')
        self.select_country_phone_number_prefix(self.country_phone_number_prefix)
        self.simulate_typing(phone_number_input_field, self.phone_number)
        self.driver.find_element(By.XPATH, '//*[@id="root"]/div/div[1]/form/div[3]/button[contains('
                                           'text(), "Send code")]').click()
        time.sleep(5)
        # wait until login slide gone, i.e. until I approved verification manually
        WebDriverWait(self.driver, 200).until(EC.invisibility_of_element_located((By.ID, 'login_slide')))

        # wait a few seconds to receive newest verification code, possibly adjust get_verification_code method assuring
        # that just sent code is received
        # get country prefix number for country prefix name to receive verification code from Twilio correctly
        verification_code = SMSHandler(database=self.database).get_verification_code(
            test_user_id=self.test_user_id,
            phone_number=self.phone_number,
            phone_number_country_prefix_numerous=self.get_country_prefix(self.country_phone_number_prefix))
        if not verification_code.isdigit() and verification_code == "Trigger Resend":
            self.trigger_resend_code()
        verification_input_field = self.driver.find_element_by_xpath('//input[@placeholder="Enter 4-digit code"]')
        self.simulate_typing(verification_input_field, verification_code)
        self.driver.find_element(By.XPATH, '//*[@id="root"]/div/div[1]/form/button').click()
        time.sleep(5)
        # check if error msg displayed, if so resend code and login again
        try:
            if self.driver.find_element_by_xpath('//*[@id="root"]/div/div[1]/form/button/../div[4]').__sizeof__() > 0:
                print(f"Login with verification code {verification_code} for test user {self.test_user_id} in test run "
                      f"{self.test_run_id} failed. Resending code.")
                self.trigger_resend_code()
        except selenium.common.exceptions.NoSuchElementException as e:
            self.logger.warning(f"Login with verification code seemed successful for test user {self.test_user_id} in"
                                f"test run {self.test_run_id}.")
        time.sleep(5)
        self.driver.switch_to.default_content()

    def trigger_resend_code(self):
        """
        Trigger a resend verification code.
        :param phone_number:
        :param phone_number_country_prefix_numerous:
        :return:
        """
        resend_code_btn = self.driver.find_element_by_xpath('//*[@id="root"]/div/div[1]/form/div[3]/button['
                                                            'contains(text(), "Resend code")]')
        while resend_code_btn.get_attribute('disabled').__sizeof__() > 0:
            print(f"Waiting for test user {self.test_user_id} in test run {self.test_run_id} to resend code.")
            time.sleep(1)
        resend_code_btn.click()
        self.login_user_with_verification_code()

    def select_country_phone_number_prefix(self, country_phone_number_prefix):
        """
        Function selects correct country phone number prefix.
        :param country_phone_number_prefix:
        :param phone_number:
        :return:
        """
        phone_number_placeholder_translation = {
            "en": "Phone number",
            "de": "Telefonnummer",
            "es": "Número de teléfono",
            "fr": "Numéro de téléphone"
        }
        country_list = self.driver.find_element(
            By.XPATH,
            f'//form/div[2]/input[@placeholder="'
            f'{phone_number_placeholder_translation.get(self.browser_language)}"]/../div[1]')
        country_list.click()
        country_phone_number_prefix_list = self.driver.find_elements(
            By.XPATH, f'//form/div[2]/input[@placeholder="'
                      f'{phone_number_placeholder_translation.get(self.browser_language)}"]/../div[1]/ul/li/span/span')
        # instead of selecting the correct country prefix based on the actual numerous prefix, select the
        # corresponding country name
        found_prefix = None
        for prefix_web_item in country_phone_number_prefix_list:
            string = prefix_web_item.text.partition('+')
            # select country not number prefix
            prefix = string[0]
            prefix = prefix[0:(len(prefix) - 1)]
            if prefix == country_phone_number_prefix:
                actions = ActionChains(self.driver)
                actions.move_to_element(prefix_web_item).perform()
                found_prefix = prefix_web_item
                break
        found_prefix.click()

    def simulate_typing(self, element, word):
        """
        Simulating typing like a human
        :param element: element in which string input shall be provided
        :param word: word that shall be typed in
        :return:
        """
        for letter in word:
            time.sleep(random.choice(np.arange(0.01, 0.03, 0.005)))
            element.send_keys(letter)

    def pause_video(self, href=None, play=False):
        try:
            if href is None:
                post_id, href, dom_element = self.get_current_post()

            self.wait_until_TikTok_loaded()
            video_state = ''
            pause_button = self.driver.find_element_by_xpath(f"//*[@href='{href}']"
                                                             f"//div[contains(@class, 'toggle-icon-v4')]")
            check_video_state = self.driver.find_elements_by_xpath(
                f"//*[@href='{href}']//div[contains(@class, 'toggle-icon-v4')]//*[local-name()='path']")

            # set video state
            if len(check_video_state) > 1:  # --> video in play mode
                video_state = 'play'
            elif len(check_video_state) == 1:
                video_state = 'pause'

            if play and video_state == 'pause':  # video is paused, but should be played
                pause_button.click()
            elif not play and video_state == 'play':  # video is playing, but should be paused
                pause_button.click()
        except:
            self.logger.warning("Video could not be paused.")

    def handle_banners(self):
        """
        Handle all kinds of banners if available.
        :return:
        """
        self.handle_signup_box()
        self.handle_intro_shortcuts_box()
        self.handle_eu_cookie_banner()
        self.handle_notifications_from_tiktok_popup()
        self.handle_privacy_policy_notification()

    def handle_signup_box(self):
        """
        Handling newly appearing login in box to access ForYouFeed
        :return:
        """
        try:
            check = self.driver.find_element(By.XPATH, 'html/body').get_attribute('class')
            if len(check) > 0:
                self.driver.switch_to.frame(self.driver.find_element_by_xpath('/html/body/div[2]/div[1]/iframe'))
                if len(self.driver.find_elements_by_xpath(
                        '//*[@id="root"]/div/div[1]/div/div[1]/div[1]/div[contains(text(),"Log in to TikTok")]')) > 0:
                    if self.driver.find_element_by_xpath('//*[@id="root"]/div/div[1]/div/div[1]/div[1]/div').text == \
                            'Sign up for TikTok':
                        self.driver.switch_to.default_content()
                        self.driver.find_element_by_xpath('/html/body/div[2]/div[1]/div/img').click()
                        if len(self.driver.find_elements_by_xpath('//*[@id="root"]/div/div[1]/div/div[1]/div[1]/div['
                                                                  'contains(text(),"Log in to TikTok")]')) > 0:
                            raise Exception("Sign Up Box still open.")
            else:
                self.logger.warning('Sign-up box not available.')
        except Exception as e:
            raise Exception(e)

    def handle_intro_shortcuts_box(self):
        """
        TikTok sometimes informs the user about new shortcuts to navigate the feed. This notification box shall be
        closed in case it appears.
        :return:
        """
        try:
            if self.driver.find_element_by_xpath("//*[@class='tt-feed']//div[contains(@class, "
                                                 "'keyboard-shortcut-container')]").__sizeof__() > 0:
                self.driver.find_element_by_xpath("//*[@class='tt-feed']//div[contains(@class, "
                                                  "'keyboard-shortcut-container')]//div[1][contains(@class, "
                                                  "'keyboard-shortcut-close')]").click()
                time.sleep(1)
                self.logger.warning("Intro-Shortcuts-Box closed.")
        except selenium.common.exceptions.NoSuchElementException as e:
            self.logger.warning('Intro-Shortcuts-Box not available.')

    def handle_eu_cookie_banner(self):
        """
        If test executed in the EU, TikTok displays the cookie policy adhering to the EU GDPR regulations. This policy
        shall be closed if visible.
        :return:
        """
        try:
            if self.driver.find_element_by_xpath("//*[contains(@class, 'cookie-banner')]").__sizeof__() > 0:
                self.driver.find_element_by_xpath("//*[contains(@class, 'cookie-banner')]/div/button").click()
                time.sleep(1)
                self.logger.warning("EU Cookie Policy accepted.")
        except selenium.common.exceptions.NoSuchElementException as e:
            self.logger.warning('EU Cookie Policy not available.')

    def handle_notifications_from_tiktok_popup(self):
        """
        TikTok sometimes asks the user to turn on notification messages from TikTok. If this notification is available
        it shall be closed.
        :return:
        """
        try:
            if self.driver.find_element_by_xpath("//*[contains(@class, 'push-permission')]").__sizeof__() > 0:
                self.driver.find_element_by_xpath("//*[contains(@class, 'push-permission')]//button[not(contains("
                                                  "@class, 'tiktok-btn-pc-primar'))]").click()
                self.logger.warning("Notifications from TikTok pop-up canceled.")
                time.sleep(1)
        except selenium.common.exceptions.NoSuchElementException as e:
            self.logger.warning('Notifications from TikTok pop-up not available.')

    def handle_privacy_policy_notification(self):
        """
        TikTok has updated their privacy policy and informs the user through a banner. This banner limits the visible
        area of the feed. Therefore, if it is available it shall be closed.
        :return:
        """
        try:
            if self.driver.find_element_by_xpath("//*[contains(@class, 'universal-banner-fixed middle')]//a[contains("
                                                 "text(), 'Privacy Policy')]").__sizeof__() > 0:
                self.driver.find_element_by_xpath("//*[contains(@class, 'universal-banner-fixed middle')]//div["
                                                  "contains(@class, 'right')]/div").click()
                self.logger.warning('Privacy Policy Update banner closed.')
        except selenium.common.exceptions.NoSuchElementException as e:
            self.logger.warning('Privacy Policy Update banner not available.')

    def scroll_and_action(self, time_to_look_at_post_normal, time_to_look_at_post_action, number_of_posts_to_like,
                          number_of_creators_to_follow, number_of_posts_to_watch_longer, posts_with_hashtag_to_like,
                          posts_with_hashtag_to_watch_longer, posts_of_content_creators_to_like,
                          posts_of_music_ids_to_like, posts_of_current_batch, batch, separate_posts_not_stored,
                          posts_seen_due_to_separate_posts):
        """
        Scroll through batch, look at each post x seconds, like x posts randomly
        :return:
        """
        self.time_to_look_at_post_normal = time_to_look_at_post_normal
        self.time_to_look_at_post_action = time_to_look_at_post_action
        self.posts_of_current_batch = posts_of_current_batch
        self.batch = batch
        self.separate_posts_not_stored = separate_posts_not_stored

        # handle shortcuts intro box again
        self.handle_intro_shortcuts_box()

        # randomly create list of posts that shall be liked/followed/watched_longer; attention if
        # separate_posts_not_stored is not empty separate posts were not stored in db and thus should not liked; to
        # prevent any db errors this can only happen in the first batch, for the first batch take the length of the
        # list of separate posts that were not stored, if no separate posts were stored, the bot mustn't like those
        # first posts, otherwise the list is empty and the bot may like the first posts
        if number_of_posts_to_like != 0:
            self.random_selection_likes = self.select_random_selection(number_of_posts_to_like)
        else:
            self.random_selection_likes = []
        if number_of_creators_to_follow != 0:
            self.random_selection_followers = self.select_random_selection(number_of_creators_to_follow)
        else:
            self.random_selection_followers = []
        if number_of_posts_to_watch_longer != 0:
            self.random_selection_watching = self.select_random_selection(number_of_posts_to_watch_longer)
        else:
            self.random_selection_watching = []
        if len(posts_with_hashtag_to_like) != 0:
            self.posts_with_hashtag_to_like = posts_with_hashtag_to_like
        else:
            self.posts_with_hashtag_to_like = []
        if len(posts_with_hashtag_to_watch_longer) != 0:
            self.posts_with_hashtag_to_watch_longer = posts_with_hashtag_to_watch_longer
        else:
            self.posts_with_hashtag_to_watch_longer = []
        if len(posts_of_content_creators_to_like) != 0:
            self.posts_of_content_creators_to_like = posts_of_content_creators_to_like
        else:
            self.posts_of_content_creators_to_like = []
        if len(posts_of_music_ids_to_like) != 0:
            self.posts_of_music_ids_to_like = posts_of_music_ids_to_like
        else:
            self.posts_of_music_ids_to_like = []

        # start playing video if paused
        self.pause_video(play=True)

        # Iterate through list of posts of current batch and perform action a post if applicable
        for post in self.posts_of_current_batch:
            self.current_post = post
            self.logger.warning(
                f"Moving to post {self.posts_of_current_batch.index(self.current_post)} of batch {self.batch} for test"
                f" user {self.test_user_id} in test run {self.test_run_id}.")
            print(
                f"Moving to post {self.posts_of_current_batch.index(self.current_post)} of batch {self.batch} for test user "
                f"{self.test_user_id} in test run {self.test_run_id}.")
            # update current post variables
            try:
                self.current_post_href_web_element = self.current_post.find_element_by_xpath(
                    "./child::div/div/div/div/a")
                self.current_post_href = self.current_post_href_web_element.get_attribute('href')
            except selenium.common.exceptions.NoSuchElementException:
                time.sleep(0.5)
                self.current_post_href_web_element = self.current_post.find_element_by_xpath(
                    "./child::div/div/div/div/a")
                self.current_post_href = self.current_post_href_web_element.get_attribute('href')
            partition = self.current_post_href.rpartition('/')
            self.current_post_id = partition[len(partition) - 1]

            # only watch, like, follow etc. post if not yet seen from data collection of first few posts, if any post
            # was already seen it shall be skipped when scrolling through batch
            if self.current_post_id not in posts_seen_due_to_separate_posts:
                self.update_post_and_batch_positions()

                # wait until TikTok loaded, then watch post as applicable
                self.wait_until_TikTok_loaded()
                self.watch_post()
                self.trigger_like_or_follow(number_of_posts_to_like, number_of_creators_to_follow)

            # move to the next post
            self.move_to_next_post()

    def select_random_selection(self, number_of_random_items_to_select):
        """
        Select a number of random posts to perform some action on for the current batch.
        :param number_of_random_items_to_select:
        :return:
        """
        random_selection = []
        if number_of_random_items_to_select > 0:
            if self.batch == 0:  # pay attention to first few posts for which their data may not be stored
                random_selection = random.sample(list(range(len(self.separate_posts_not_stored) + 1,
                                                            self.posts_of_current_batch.__len__())),
                                                 number_of_random_items_to_select)
            else:
                random_selection = random.sample(list(range(0, self.posts_of_current_batch.__len__())),
                                                 number_of_random_items_to_select)
        self.logger.warning(f"Random posts to like/follow/watch longer: {random_selection}")
        return random_selection

    def trigger_like_or_follow(self, number_of_posts_to_like, number_of_creators_to_follow):
        """
        Check if current post shall be liked or followed.
        :return:
        """
        # if current post in random_selection_likes/_followers perform applicable action
        # if applicable: like the post currently watching if it is the one randomly picked for batch
        if number_of_posts_to_like > 0:
            if self.posts_of_current_batch.index(self.current_post) in self.random_selection_likes:
                self.like_post()

        # if applicable: follow the creator of the current post
        if number_of_creators_to_follow > 0:
            if self.posts_of_current_batch.index(self.current_post) in self.random_selection_followers:
                self.follow_creator()

        # if applicable: like post if it has at least one of the hashtags specified in posts_with_hashtags_to_like
        if len(self.posts_with_hashtag_to_like) > 0:
            self.like_post_if_contains_relevant_hashtag()

        # if applicable: like post if of certain content creator
        if len(self.posts_of_content_creators_to_like) > 0:
            self.like_posts_if_contains_relevant_content_creator()

        # if applicable: like post if has certain music id
        if len(self.posts_of_music_ids_to_like) > 0:
            self.like_posts_if_contains_relevant_music_id()

    def get_hashtags_of_current_post(self):
        """
        Retrieve all hashtags of the current post
        :return:
        """
        # get all hashtags of the current post, store them in a list
        current_post_hashtags = []
        hashtag_elements_of_post = self.driver.find_elements_by_xpath(f"//*[@href='{self.current_post_href}']/../"
                                                                      f"../../div[2]/a[contains(@href, 'tag')]")
        for hashtag in hashtag_elements_of_post:
            hashtag_href = hashtag.get_attribute('href')
            pure_hashtag = hashtag_href.rpartition('/')[2].rpartition('?')[0]
            current_post_hashtags.append(pure_hashtag)
        return current_post_hashtags

    def like_post_if_contains_relevant_hashtag(self):
        """
        Check if the current post contains a hashtag which shall be liked, if so like the post.
        :return:
        """
        try:
            current_post_hashtags = self.get_hashtags_of_current_post()

            # compare hashtags of current post with hashtags that shall be liked
            common_hashtags = set(self.posts_with_hashtag_to_like) & set(current_post_hashtags)
            if common_hashtags.__len__() > 0:
                self.logger.warning(f"Post {self.current_post_href} contains hashtags that shall be liked. Thus post "
                                    f"liked by test user {self.test_user_id} in test run {self.test_run_id}.")
                self.like_post()
            else:
                self.logger.warning(f"No common hashtags for post {self.current_post_href} and test user "
                                    f"{self.test_user_id} in test run {self.test_run_id}.")
        except selenium.common.exceptions.NoSuchElementException:
            self.logger.warning(f"Current post {self.current_post_href}, which test user {self.test_user_id} in test "
                                f"run {self.test_run_id} is looking at, has no hashtags.")

    def like_posts_if_contains_relevant_content_creator(self):
        """
        Check if the current post is from a certain content creator, if so like the post.
        :return:
        """
        try:
            current_post_content_creator = self.driver.find_element_by_xpath(
                f"//*[@href='{self.current_post_href}']/../../..//h3[contains(@class, 'author-uniqueId')]").text
            # like post if current content creator one of those for whom the posts shall be liked
            if current_post_content_creator in self.posts_of_content_creators_to_like:
                self.like_post()
            elif current_post_content_creator in self.already_seen_content_creators:
                self.like_post()
                self.logger.warning(f"Test user {self.test_user_id} already saw another post from the same content "
                                    f"creator therefore likes his post.")
            else:
                self.logger.warning(f"The current content creator of post {self.current_post_href} shall not be liked"
                                    f"for test user {self.test_user_id} in test run {self.test_run_id}.")
            self.already_seen_content_creators.append(current_post_content_creator)
        except selenium.common.exceptions.NoSuchElementException:
            self.logger.warning(f"Current post {self.current_post_href}, which test user {self.test_user_id} in test "
                                f"run {self.test_run_id} is looking at, has no content creator.")

    def like_posts_if_contains_relevant_music_id(self):
        """
        Check if current post has music that shall be liked, if so like the post.
        :return:
        """
        try:
            # get music id
            current_post_music_href = self.driver.find_element_by_xpath(
                f"//*[@href='{self.current_post_href}']/../../..//div[contains(@class, 'tt-video-music')]/h4/a"). \
                get_attribute('href')
            current_post_music_id = current_post_music_href.rpartition('?')[0].rpartition('-')[2]
            # like post if current content creator one of those for whom the posts shall be liked
            if current_post_music_id in self.posts_of_music_ids_to_like:
                self.like_post()
            elif current_post_music_id in self.already_seen_music:
                self.like_post()
                self.logger.warning(f"Test user {self.test_user_id} already saw another post with the same music "
                                    f"therefore likes this post.")
            else:
                self.logger.warning(f"The music of the current post {self.current_post_href} shall not be liked"
                                    f"for test user {self.test_user_id} in test run {self.test_run_id}.")
            self.already_seen_music.append(current_post_music_id)
        except selenium.common.exceptions.NoSuchElementException:
            self.logger.warning(f"Current post {self.current_post_href}, which test user {self.test_user_id} in test "
                                f"run {self.test_run_id} is looking at, has no content creator.")

    def move_to_next_post(self):
        """
        Scroll to the next post, but exactly to the next post taking into account the post that currently plays its
        video. Since sometimes the website has a hick-up the method needs to verify that the bot actually moved to a
        new post.
        :return:
        """
        try:
            next_post = self.driver.find_element_by_xpath(f'//*[@href="{self.current_post_href}"]/../../../../../'
                                                          f'following-sibling::span[1]')
            self.driver.execute_script("return arguments[0].scrollIntoView(true);", next_post)
        except selenium.common.exceptions.NoSuchElementException:
            self.wait_until_TikTok_loaded()
            time.sleep(0.5)
            self.move_to_next_post()

    def verify_moved_to_next_post(self):
        """
        Get correct href of post that was watched previously.
        :return:
        """
        # update self.current_post_href with post that was indeed watched previously considering the fact that
        # two posts could have "video" DOM element
        previous_post_href = self.current_post_href
        posts = self.driver.find_elements_by_xpath('//video/../../..')
        # if more than one post has the video element, scroll again to the last post that had it to assure correct
        # scrolling
        if len(posts) > 1:
            new_current_post = posts[1].get_attribute('href')
            self.current_post_href = new_current_post
            self.move_to_next_post()
        else:
            new_current_post = posts[0].get_attribute('href')

        # Verify that bot actually moves to next post, if newly selected previous_post_href equals the old previous_post_href
        # then it seems bot has not moved to next post, thus move_to_next_post() must be performed again
        if previous_post_href == new_current_post:
            self.logger.warning(f"Bot for {self.test_user_id} in test run {self.test_run_id} did not move from post"
                                f"{previous_post_href} to the next post.")
            self.current_post_href = previous_post_href
            self.move_to_next_post()

    def update_post_and_batch_positions(self, optional_batch_position=None):
        """
        Update the dictionary of post_batch_positions for a post with its post_id.
        dictionary structure: {post_id: {corresponding_post_position: , corresponding_batch_position}}
        :param optional_batch_position:
        :param post_id:
        :return:
        """
        # check that post either not yet in post_bat_positions or not at the same position
        store = False
        if self.current_post_id not in self.post_batch_positions:
            store = True
        # storing post again if appeared in feed at different position, i.e. only if not a separate post and if not at
        # same position as the current post shall be stored at
        elif self.current_post_id not in self.separate_posts_not_stored and \
                self.current_post_id in self.post_batch_positions.keys() and \
                self.post_batch_positions.get(self.current_post_id).get('post_position') != (self.post_position + 1):
            store = True

        if store:
            self.post_position += 1
            if optional_batch_position is not None:
                self.post_batch_positions[self.current_post_id] = {"post_position": self.post_position,
                                                                   "batch_position": optional_batch_position}
            else:
                self.post_batch_positions[self.current_post_id] = {"post_position": self.post_position,
                                                                   "batch_position": self.batch}

    def like_post(self):
        """
        Like the post currently looking at and store data of liked post.
        :return:
        """
        # like post if not yet liked
        if self.current_post_id not in self.posts_liked:
            # like post if not one of the separate posts
            if self.current_post_id not in self.separate_posts_not_stored:
                current_video = self.driver.find_element_by_xpath(f'//*[@href="{self.current_post_href}"]')
                actions = ActionChains(self.driver)
                actions.move_to_element(current_video).perform()
                like_btn = self.driver.find_element_by_xpath(
                    f'//*[@href="{self.current_post_href}"]/../../div[2]/div[1]/strong[@title="like"]/../div')
                like_btn.click()
                self.verify_post_liked()
                partition = self.current_post_href.rpartition('/')
                post_id = partition[len(partition) - 1]
                if post_id not in self.posts_liked:
                    self.posts_liked.append(post_id)
                    self.logger.warning(f"Post {post_id} liked and stored in temp_list accordingly.")
                time.sleep(1)
            else:
                self.logger.warning(f"Current post {self.current_post_href} is one of the first posts for which"
                                    f"their data was not stored. Thus, post should not be liked as could not be"
                                    f"stored.")
        else:
            self.logger.warning(f"Current post {self.current_post_href} already liked.")

    def verify_post_liked(self):
        """
        Verify the post is actually liked by checking on the like element.
        :return:
        """
        like_btn_fill_element = self.driver.find_element_by_xpath(f'//*[@href="{self.current_post_href}"]/../../div[2]/'
                                                                  f'div[1]/strong[@title="like"]/../div//*['
                                                                  f'local-name()="svg"]').get_attribute('fill')
        if like_btn_fill_element != 'none':
            self.like_post()
        else:
            self.logger.warning(f"Post {self.current_post_href} was liked by {self.test_user_id} in test run "
                                f"{self.test_run_id}.")

    def watch_post(self):
        """
        Watch a post for a certain amount of time declared by self.time_to_look_at_post)
        :return:
        """
        try:
            # account for the time that passes until the video is actually being watched, i.e. the bot sleeps, the time
            # that passed until then shall be deducted from the time that shall be actually watched, otherwise the bot
            # may watch for too long
            start_retrieval_for_watching = time.time()
            to_be_watched_longer = self.watch_post_longer_check()

            computed_time_to_look_at_post = 0
            end_retrieval_for_watching = 0
            try:
                # if time to look at post bigger 1 it indicates post shall be watched in seconds, otherwise
                # percentage of entire length of post shall be watched
                if not to_be_watched_longer:
                    computed_time_to_look_at_post = self.get_duration_for_post(time=self.time_to_look_at_post_normal)

                # if current post shall be watched longer, the post shall be watched for a certain percentage of
                # its entire length
                elif to_be_watched_longer:
                    computed_time_to_look_at_post = self.get_duration_for_post(time=self.time_to_look_at_post_action)

                end_retrieval_for_watching = time.time() - start_retrieval_for_watching

                if end_retrieval_for_watching > computed_time_to_look_at_post:
                    self.logger.warning("Time to watch post shorter than time it took to retrieve duration!")
                    self.posts_watched_longer[self.current_post_id] = end_retrieval_for_watching
                else:
                    computed_time_to_look_at_post -= end_retrieval_for_watching
                    time.sleep(computed_time_to_look_at_post)
                    self.posts_watched_longer[self.current_post_id] = computed_time_to_look_at_post
            except TypeError:
                self.logger.warning(computed_time_to_look_at_post)
                self.logger.warning(type(computed_time_to_look_at_post))
                self.logger.warning("Computed time is NoneType and cannot be compared to end retrieval time.")
                self.posts_watched_longer[self.current_post_id] = end_retrieval_for_watching
            else:
                time.sleep(0.5)  # backup: if duration cannot be compute, watch only half a second
        except selenium.common.exceptions.NoSuchElementException:
            time.sleep(0.5)  # backup: if duration cannot be compute, watch only half a second

    def watch_post_longer_check(self):
        """
        Return true if current post shall be watched longer.
        Either post contains a certain hashtag or selection randomly.
        :return:
        """
        # post among random selection to watch longer
        if len(self.random_selection_watching) > 0:
            if self.posts_of_current_batch.index(self.current_post) in self.random_selection_watching:
                return True
            else:
                return False

        # post contains hashtag for which posts shall be watched longer
        elif len(self.posts_with_hashtag_to_watch_longer) > 0 and len(self.random_selection_watching) == 0:
            common_hashtags = set(self.posts_with_hashtag_to_watch_longer) & \
                              set(self.get_hashtags_of_current_post())
            if common_hashtags.__len__() > 0:
                self.logger.warning(f"Post {self.current_post_href} contains hashtags for which posts shall be watched "
                                    f"longer. Thus post watched longer by test user {self.test_user_id} in test run "
                                    f"{self.test_run_id}.")
                return True
            else:
                return False
        else:
            return False

    def follow_creator(self):
        """
        Follow the creator of the current post.
        :return:
        """
        current_follow_button = self.driver.find_element_by_xpath(f"//*[@href='{self.current_post_href}']/../../../div"
                                                                  f"[contains(@class, 'item-follow-wrapper')]//button")
        actions = ActionChains(self.driver)
        actions.move_to_element(current_follow_button).perform()
        self.driver.execute_script("window.scrollBy(0,-70);")
        if current_follow_button.text != 'Following':  # check if already following, if not follow
            current_follow_button.click()
            self.verify_creator_followed(current_follow_button)
            partition = self.current_post_href.rpartition('/')
            post_id = partition[len(partition) - 1]
            if post_id not in self.creators_followed:
                self.creators_followed.append(post_id)
                self.logger.warning(f"Content creator of post {post_id} was followed and stored in temp_list "
                                    f"accordingly.")
            time.sleep(1)
        else:
            self.logger.warning(f"It seems as if user {self.test_user_id} already follows content creator of post"
                                f"{self.current_post_href}.")

    def verify_creator_followed(self, current_follow_button):
        """
        Verify that the follow button was successfully pressed.
        :return:
        """
        if current_follow_button.text != "Following":
            self.follow_creator()
        else:
            self.logger.warning(f"Content creator of post {self.current_post_href} is indeed followed by test user "
                                f"{self.test_user_id} in test run {self.test_run_id}.")

    def get_duration_for_post(self, time):
        """
        Retrieve the duration in seconds for a certain post from the set of requests.
        :return:
        """
        try:
            # check if duration already stored, if so use it
            duration_of_current_post = self.database.get_duration(post_id=self.current_post_id)
            if duration_of_current_post != 0:
                return duration_of_current_post * time

            # if not already stored, get duration from requests, if post not in request data, update request data and
            # check again, only update 3 times to avoid infinite loop
            else:
                not_found = True
                updated = 0
                while not_found and updated < 4:
                    if len(self.durations_from_posts) == 0:
                        self.get_durations_from_requests()
                    elif self.current_post_id in self.durations_from_posts.keys():
                        not_found = False
                        return self.durations_from_posts.get(self.current_post_id) * time
                    else:
                        updated += 1
                        self.get_durations_from_requests()
        except:
            self.logger.warning(
                f"Retrieving duration of post {self.current_post_href} for test user {self.test_user_id}"
                f" in test run {self.test_run_id} failed.")

    def get_durations_from_requests(self):
        """
        Get durations from requests.
        :return:
        """
        for request in self.driver.requests:
            # look for correct request url
            # check if request actually holds data, if not don't use it
            if "https://m.tiktok.com/api/recommend/item_list/?aid=1988&app_name=tiktok_web&device_platform=web_pc" \
                    in request.url and 'body' in request.response.__dir__():
                if len(request.response.body) > 0:
                    data = json.loads(request.response.body)['itemList']
                    for item in data:
                        self.durations_from_posts[item.get('id')] = item.get('video').get('duration')

    def get_current_post(self):
        """
        Get the ID of the post that is currently being watched.
        :return: post_id, href
        """
        dom_element = self.driver.find_element_by_xpath('//video/../../..')
        partition = dom_element.get_attribute('href').rpartition('/')
        post_id = partition[len(partition) - 1]
        href = dom_element.get_attribute('href')
        return post_id, href, dom_element

    def move_to_next_post_trigger_requests(self):
        next_post = self.driver.find_element_by_xpath('//video/../../../../../../../following-sibling::div')
        self.driver.execute_script("return arguments[0].scrollIntoView(true);", next_post)

    def get_country_prefix(self, country_prefix_name):
        """
        returns numerous prefix of country for getting verification code from Twilio
        json structure example:
        {
            "United States": "+1",
            "United Kingdom": "+44"
        }
        :param country_prefix_name:
        :return:
        """
        file_path = (self.base_path / "../utilities/country_prefix.json").resolve()
        with open(file_path, "r") as f:
            prefixes = json.load(f)
        return prefixes.get(country_prefix_name)

import json
import os
import random
import time

import psycopg2 as psycopg2
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys

from src.WebHelper import *
from src.Proxy import *
from src.DataStoring import *
from src.TestRun import TestRun

# countries = ['US', 'GB', 'FR', 'DE', 'CA', 'CH', 'CH']
# c1 = 'DE'
# c2 = 'DE'
# proxy_auth_username = 'auamyynt-dest'
# proxy_auth_password = 'j5u77rwhbdnj'
# proxy_host1, proxy_port1 = get_db_proxy(c1)
# proxy_host2, proxy_port2 = get_db_proxy(c2, {'proxy_host': proxy_host1, 'proxy_port': proxy_port1})

# test_data = {
#     "testuserinfo": [
#         {"testuserid": 4, "email": "bertman@mailinator.com", "password": "%J0ftE999yQVg2",
#          "browser_language": "de", "proxy":
#              {
#                  'proxy_username': proxy_auth_username, 'proxy_password': proxy_auth_password,
#                  'proxy_host': proxy_host1, 'proxy_port': proxy_port1,
#                  'country': c1
#              }
#          },
#         {"testuserid": 5, "email": "loc2021@mailinator.com", "password": "%@NreeHIwb*55O5@zD48",
#          "browser_language": "tr", "proxy":
#              {
#                  'proxy_username': proxy_auth_username, 'proxy_password': proxy_auth_password,
#                  'proxy_host': proxy_host1, 'proxy_port': proxy_port1,
#                  'country': c1
#              }
#          },
#         {"testuserid": 8, "email": "loc2021@mailinator.com", "password": "%@NreeHIwb*55O5@zD48",
#          "browser_language": "en", "proxy":
#              {
#                  'proxy_username': proxy_auth_username, 'proxy_password': proxy_auth_password,
#                  'proxy_host': proxy_host1, 'proxy_port': proxy_port1,
#                  'country': c1
#              }
#          },
#         {"testuserid": 9, "email": "loc2021@mailinator.com", "password": "%@NreeHIwb*55O5@zD48",
#          "browser_language": "es", "proxy":
#              {
#                  'proxy_username': proxy_auth_username, 'proxy_password': proxy_auth_password,
#                  'proxy_host': proxy_host1, 'proxy_port': proxy_port1,
#                  'country': c1
#              }
#          }
#     ],
#     "description": "same location, different languages, no user accounts"}
#
# test_data_2 = {
#     "testuserinfo": [
#         {"testuserid": 5, "email": "loc2021@mailinator.com", "password": "%@NreeHIwb*55O5@zD48",
#          "browser_language": "tr", "proxy":
#              {
#                  'proxy_username': proxy_auth_username, 'proxy_password': proxy_auth_password,
#                  'proxy_host': proxy_host1, 'proxy_port': proxy_port1,
#                  'country': c1
#              }
#          },
#     ],
#     "description": "same location, different languages, no user accounts"}

c1 = 'US'
c2 = 'CA'
proxy_auth_username = 'PLACEHOLDER'
proxy_auth_password = 'PLACEHOLDER'
proxy_host1, proxy_port1 = get_db_proxy(c1)
# test_data = {"testuserid": 35, "phone_number": "7862148574", "password": "IOw2z*W282&X", "browser_language": "en",
#          "country_phone_number_prefix": "United States", "time_to_look_at_post": 2,
#          "number_of_posts_to_like_per_batch": [0, 0, 0], "collecting_data_for_first_posts": True,
#          "proxy":
#              {'proxy_username': proxy_auth_username, 'proxy_password': proxy_auth_password,
#               'proxy_host': '185.95.157.159', 'proxy_port': '6180', 'country': c1}
#          }

test_data = [
        {"test_user_id": 11, "phone_number": "5039664089", "password": "IOw2z*W282&X", "browser_language": "en",
         "country_phone_number_prefix": "United States", "time_to_look_at_post": 2,
         "number_of_posts_to_like_per_batch": [15, 5], "collecting_data_for_first_posts": False,
         "proxy":
             {'proxy_username': proxy_auth_username, 'proxy_password': proxy_auth_password,
              'proxy_host': proxy_host1, 'proxy_port': proxy_port1, 'country': c1}
         }
        # {"test_user_id": 15, "phone_number": "1798297886", "password": "k@pywYE7l8", "browser_language": "en",
        #  "country_phone_number_prefix": "Germany", "time_to_look_at_post": 2,
        #  "number_of_posts_to_like_per_batch": [0, 0, 0], "collecting_data_for_first_posts": False,
        #  "proxy":
        #      {'proxy_username': proxy_auth_username, 'proxy_password': proxy_auth_password,
        #       'proxy_host': proxy_host1, 'proxy_port': proxy_port1, 'country': c1}
        #  }
    ]


with TestRun(test_data=test_data) as test_run:
    start = time.time()
    test_data = test_data[0]
    test_data['test_run_id'] = test_run.test_run_id
    base_path = Path(__file__).parent
    file_path = (base_path / f"../Data Analysis/console_logs/console_log_{test_data.get('test_run_id')}_user_"
                             f"{test_data.get('test_user_id')}.log").resolve()
    logging.basicConfig(filename=file_path, format='%(asctime)s %(message)s', filemode='w')
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)
    logger.info(f'Starting execution for testuser {test_data.get("test_user_id")}.')
    helper = WebHelper(test_user_id=test_data.get('test_user_id'),
                       test_run_id=test_data.get('test_run_id'),
                       logger=logger,
                       proxy=test_data.get('proxy'),
                       browser_language=test_data.get("browser_language"))
    helper.login_user_phone(test_data.get('phone_number'), test_data.get('country_phone_number_prefix'))
    data_storing = DataStoring(helper=helper,
                               logger=logger,
                               number_of_batches=len(test_data.get('number_of_posts_to_like_per_batch')),
                               test_user_id=test_data.get('test_user_id'),
                               test_run_id=test_data.get('test_run_id'))
    data_storing.get_separate_posts_data(collecting_data_for_first_posts=test_data.get("collecting_data_for_first_posts"))
    data_storing.get_request_posts_data(test_data.get('time_to_look_at_post'),
                                        test_data.get('number_of_posts_to_like_per_batch'))
    helper.database.unflag_proxy(proxy_host=test_data['proxy']['proxy_host'],
                                 proxy_port=test_data['proxy']['proxy_port'])
    helper.close_driver()
    data_storing.store_collected_data()
    duration = time.time() - start
    test_data['duration'] = (duration / 60)
    logger.info(f'Execution for testuser {test_data.get("test_user_id")} completed in {duration} seconds '
                f'({duration / 60} minutes).')


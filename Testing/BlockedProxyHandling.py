import random

import seleniumwire

from seleniumwire import webdriver
from pathlib import Path



def start_session():
    proxy = {
        'proxy_username': 'PLACEHOLDER', 'proxy_password': 'PLACEHOLDER',
        'proxy_host': 'PLACEHOLDER', 'proxy_port': 'PLACEHOLDER',
        'country': 'FR'
    }
    try:
        # bypassing detection of automated software testing
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--lang={browser_language}'.format(browser_language='en'))

        # open incognito page to remove any different_posts_noise from tracked cookies or browsing history, according to paper from
        # Aniko Hannak et. al.
        chrome_options.add_argument('incognito')

        # use proxy if provided:
        options = {}
        if proxy is not None:
            url = "{proxy_username}:{proxy_password}@{proxy_host}:{proxy_port}".format(
                proxy_username=proxy['proxy_username'], proxy_password=proxy['proxy_password'],
                proxy_host=proxy['proxy_host'], proxy_port=proxy['proxy_port'])
            options = {
                'proxy': {
                    'http': 'http://' + url,
                    'https': 'https://' + url,
                    'no_proxy': 'localhost,127.0.0.1'
                }
            }

        # initializing web driver
        base_path = Path(__file__).parent
        file_path = (base_path / "../utilities/chromedriver.exe").resolve()
        driver = webdriver.Chrome(chrome_options=chrome_options, seleniumwire_options=options,
                                  executable_path=file_path)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.get('https://m.tiktok.com')
    except (ConnectionAbortedError, seleniumwire.thirdparty.mitmproxy.exceptions.TcpDisconnect) as err:
        print(err)
        print('\n New driver session with new proxy initialized.')
        # here I would run some new code to create a new session with another proxy address


if __name__ == "__main__":
    start_session()
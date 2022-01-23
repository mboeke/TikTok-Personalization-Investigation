"""
File access Webshare.io API to get required proxy data
"""

import requests
from src.DatabaseHelper import *

APIKEY = "PLACEHOLDER"

base_path = Path(__file__).parent
file_path = (base_path / "../utilities/db_credentials.json").resolve()
with open(file_path) as file:
    db_credentials = json.load(file)

conn = psycopg2.connect(
    host=db_credentials.get('host'),
    database=db_credentials.get('database'),
    user=db_credentials.get('user'),
    password=db_credentials.get('password'))
cur = conn.cursor()


def proxy(countries):
    country_string = countries[0]
    if len(countries) > 1:
        for country in countries[1:]:
            country_string = country_string + "-" + country
    response = requests.get("https://proxy.webshare.io/api/proxy/list/?countries=" + country_string,
                            headers={"Authorization": "Token %s" % APIKEY})
    proxy_data = {}
    proxy_data['username'] = 'PLACEHOLDER'
    proxy_data['password'] = 'PLACEHOLDER'
    proxies = {}
    for proxy in response.json()['results']:
        if proxy['country_code'] in proxies:
            proxies[proxy['country_code']] = proxies.get(proxy['country_code']) + [
                [proxy['proxy_address'], proxy['ports']['http']]]
        else:
            proxies[proxy['country_code']] = [[proxy['proxy_address'], proxy['ports']['http']]]
    proxy_data['proxies'] = proxies
    return proxy_data


def get_db_proxy(country):
    database = DatabaseHelper()
    proxy_host, proxy_port = database.get_active_proxy_from_db(country)
    return proxy_host, proxy_port


def update_proxy_db():
    response = requests.get("https://proxy.webshare.io/api/proxy/list/", headers={"Authorization": "Token %s" % APIKEY})
    sql = """insert into proxies(host, port, is_blocked, country, currently_used, start_usage)
    values(%s,%s,%s,%s,%s, current_timestamp) on conflict on constraint proxies_pkey do nothing"""
    for proxy in response.json()['results']:
        cur.execute(sql, (proxy.get('proxy_address'), proxy['ports'].get('http'),
                          'false', proxy.get('country_code'), 'false',))
        conn.commit()
    cur.close()


def find_disposable_proxy(country):

    disposable_proxies = []
    proxy_data = proxy([country]).get('proxies').get(country)
    sql = """select host, port from proxies where user_using_this_proxy is null and country = %s"""
    cur.execute(sql, (country,))
    results = cur.fetchall()
    for host_port in results:
        host = host_port[0].strip()
        port = host_port[1].strip()
        if host in list(host[0] for host in proxy_data) and port in list(str(host[1]) for host in proxy_data):
            disposable_proxies.append([host, port])
    return disposable_proxies


def get_new_proxy(country):
    # get new proxy
    proxy_data = proxy([country]).get('proxies').get(country)
    sql = """select host from proxies"""
    cur.execute(sql, )
    results = cur.fetchall()
    for host_port in proxy_data:
        if host_port[0] not in list(result[0].strip() for result in results):
            host = host_port[0]
            port = host_port[1]
            return host, port


def delete_proxy_in_db(host, port):
    # delete disposable proxy
    sql = """delete from d1rpgcvqcran0q.public.proxies where host = %s and port = %s"""
    cur.execute(sql, (host, port,))
    conn.commit()


def update_db_for_user(host, port, user):
    sql = """update proxies set user_using_this_proxy = %s 
    where host = %s and port = %s"""
    cur.execute(sql, (user, host, port,))
    conn.commit()


def proxies_maintenance():
    response = requests.get("https://proxy.webshare.io/api/proxy/list", headers={"Authorization": f"Token {APIKEY}"})
    all_proxies = response.json()

    sql1 = """select host, port from d1rpgcvqcran0q.public.proxies where legacy != true"""
    cur.execute(sql1, )
    results = cur.fetchall()
    db_proxies = []
    for item in results:
        host = item[0].strip()
        port = item[1].strip()
        db_proxies.append([host, port])

    proxy_not_in_db = []
    for proxy in all_proxies.get('results'):
        if proxy.get('proxy_address') not in list(host[0] for host in db_proxies):
            proxy_not_in_db.append([proxy.get('proxy_address'),
                                    proxy.get('ports').get('http'),
                                    proxy.get('country_code')])

    proxy_not_available = []
    for proxy_db in db_proxies:
        if proxy_db[0] not in list(proxy.get('proxy_address') for proxy in all_proxies.get('results')):
            proxy_not_available.append([proxy_db[0], proxy_db[1]])

    # update db: add all missing proxies
    sql2 = """insert into d1rpgcvqcran0q.public.proxies(host, port, country) values(%s,%s,%s)
    on conflict on constraint proxies_pkey do nothing"""
    for proxy in proxy_not_in_db:
        cur.execute(sql2, (proxy[0], proxy[1], proxy[2],))
        conn.commit()

    # update db: delete all proxies that no longer exist on webshare.io
    for proxy in proxy_not_available:
        delete_proxy_in_db(proxy[0], proxy[1])

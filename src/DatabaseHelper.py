import json
from pathlib import Path

import psycopg2
from langdetect import detect
from psycopg2.extras import execute_values


class DatabaseHelper:
    """
    class inserts extracted data to database
    - creates database connection
    - stores data
    """

    def __init__(self):
        self.conn = None
        self.cur = None
        self.get_database_connection()
        self.base_path = Path(__file__).parent
        self.current_music_internal_id = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur is not None:
            self.cur.close()

    def get_database_connection(self):
        """
        Establishes database connection
        :return:
        """
        base_path = Path(__file__).parent
        file_path = (base_path / "../utilities/db_credentials.json").resolve()
        with open(file_path) as file:
            db_credentials = json.load(file)

        self.conn = psycopg2.connect(
            host=db_credentials.get('host'),
            database=db_credentials.get('database'),
            user=db_credentials.get('user'),
            password=db_credentials.get('password'))
        self.cur = self.conn.cursor()

    def check_if_post_row_exists(self, post_id, test_user_id, test_run_id):
        """
        Check if post that shall be stored next already exists, if it does, do not increase post_position
        :return: true if post already exists, else return false
        """
        sql_check_post = """select -- from d1rpgcvqcran0q.public.posts where id = %s and testuserid = %s and 
        --testrunid = %s"""
        self.cur.execute(sql_check_post, (post_id, test_user_id, test_run_id,))
        result = self.cur.fetchall()
        if len(result) > 0:
            if result[0][0] == 1:
                return True
        else:
            return False

    def store_data(self, data, post_position, batch_position, test_user_id, test_run_id):
        """
        Storing data of certain post, FYI: deleting data must be in the following order: 1st post_hashtag_relation,
        2nd hashtag, 3rd post, 4th author, 5th music
        :param batch_position:
        :param test_user_id:
        :param test_run_id:
        :param post_position:
        :param data:
        :return:
        """

        def get_label(iso):
            file_path = (self.base_path / "../Request Data/LanguageList.json").resolve()
            with open(file_path) as file:
                language_file = json.load(file)
            for item in language_file.get('$languageList'):
                if item.get('value') == iso:
                    return item.get('label')
                else:
                    return 'Unknown'

        def get_language(description):
            try:
                if description != '':
                    return detect(description)
                else:
                    return ''
            except:
                return ''

        try:
            if test_run_id is None:  # stop code execution if testrunid is None
                raise Exception("TestrunID is None.")

            # handle music_id
            try:
                music_id = int(data['music'].get('id'))
            except ValueError as e:
                print(data)
                print(e)
                music_id = 0
                print(f"Storing the music_id = 0 as no actual id was given for post {int(data.get('id'))}.")

            # save plan: save author, hashtag, music, post, post_hashtag_relation
            ## AUTHOR
            # store author
            author = """insert into d1rpgcvqcran0q.public.authors(id, nickname, uniqueid, followercount, followingcount,
                heart, heartcount, videocount, diggcount) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict on constraint authors_pkey do nothing"""
            self.cur.execute(author, (
                int(data['author'].get('id')),  # authorid
                data['author'].get('nickname'),  # nickname
                data['author'].get('uniqueId'),  # uniqueid
                data['authorStats'].get('followerCount'),  # followercount
                data['authorStats'].get('followingCount'),  # followingcount
                data['authorStats'].get('heart'),  # heart
                data['authorStats'].get('heartCount'),  # heartcount
                data['authorStats'].get('videoCount'),  # videocount
                data['authorStats'].get('diggCount'),  # diggCount
            ))
            self.conn.commit()

            ## HASHTAGS
            # store hashtags with corresponding postid
            hashtag = """insert into d1rpgcvqcran0q.public.hashtags(id, name, iscommerce) values(%s,%s,%s)
                    on conflict on constraint hashtags_pkey do nothing"""
            for tag in data.get('textExtra', []):  # if 'textExtra' element doesn't exist continue
                if tag.get('hashtagId') != '':
                    self.cur.execute(hashtag, (
                        int(tag.get('hashtagId')),
                        tag.get('hashtagName'),
                        tag.get('isCommerce'),
                    ))
                    self.conn.commit()

            ## MUSIC
            if self.check_for_music_internal_id(music_id=music_id):
                music = """insert into d1rpgcvqcran0q.public.music(id, duration_sec, title) values(%s,%s,%s)
                on conflict on constraint music_pkey do nothing"""
                try:
                    self.cur.execute(music, (
                        music_id,  # musicid
                        data['music'].get('duration'),  # music_duration_sec
                        data['music'].get('title'),  # music_title
                    ))
                    self.conn.commit()
                except ValueError as e:
                    print(e)
            else:
                print(f"Music data for post {int(data.get('id'))} not stored as music_id {music_id} "
                      f"already exists in music table.")

            ## POSTS
            # when passing data to sql query string they all need to use the %s placeholder, psycopg converts in
            # SQL representation

            posts = """insert into d1rpgcvqcran0q.public.posts(id,desc_iteminfo,fullurl,language_iso_long,language_label,
                video_druation_sec,likes_diggcount,sharecount,commentcount,playcount,musicid,authorid,testrunid,
                post_position,isAd,testuserid,batch_position,music_internal_id) 
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                on conflict on constraint posts_pkey do nothing;"""
            self.cur.execute(posts, (
                int(data.get('id')),  # id
                data.get('desc', ''),  # desc_iteminfo
                'https://www.tiktok.com/@' + data['author'].get('uniqueId') + "/video/" + data.get('id'),  # fullurl
                get_language(data.get('desc', '')),  # language_iso_long: here using langdetect library to detect
                get_label(get_language(data.get('desc', ''))),  # language_label
                data['video'].get('duration'),  # video_druation_sec
                data['stats'].get('diggCount'),  # likes_diggcount: diggCount = likes
                data['stats'].get('shareCount'),  # sharecount
                data['stats'].get('commentCount'),  # commentcount
                data['stats'].get('playCount'),  # playcount
                music_id,  # musicid foreign key
                int(data['author'].get('id')),  # authorid foreign key
                test_run_id,  # test_run_id foreign key
                post_position,  # post_position
                data.get('isAd'),  # isAd
                test_user_id,  # current test_user_id
                batch_position,  # batch number in which post appeared
                self.current_music_internal_id,  # store current internal music id
            ))
            self.conn.commit()

            ## POST_HASHTAG_RELATION
            post_hashtag = """insert into d1rpgcvqcran0q.public.post_hashtag_relation(postid, hashtagid, testrunid) 
                values(%s,%s,%s) on conflict on constraint post_hashtag_relation_pkey do nothing"""
            for tag in data.get('textExtra', []):
                if tag.get('hashtagId') != '':
                    self.cur.execute(post_hashtag, (
                        int(data.get('id')),  # postid foreign key
                        int(tag.get('hashtagId')),  # hashtagid foreign key
                        test_run_id,  # testrunid foreign key
                    ))
                    self.conn.commit()
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as cursor_error:
            print(cursor_error)
            print("Instantiating db connection and trying to store data again.")
            self.get_database_connection()
            self.store_data(data, post_position, batch_position, test_user_id, test_run_id)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise Exception("Post data could not be stored")

    def check_for_music_internal_id(self, music_id):
        """
        Check if music table already lists an entry for the given music_id, if so, do not store music data.
        :param music_id:
        :return: store music if True, else do not store
        """
        try:
            sql_retrieve_music_internal_id = """select internal_id from d1rpgcvqcran0q.public.music where id = %s 
            limit 1"""
            self.cur.execute(sql_retrieve_music_internal_id, (music_id,))
            sql_retrieve_music_internal_id_result = self.cur.fetchall()
            if len(sql_retrieve_music_internal_id_result) == 0:
                return True
            else:
                self.current_music_internal_id = sql_retrieve_music_internal_id_result[0][0]
                return False
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise Exception("Music internal id could not be retrieved.")

    def get_active_proxy_from_db(self, country):
        """
        Return any proxy address of a specific country that is not yet blocked.
        :param proxy_already_in_use: if another proxy of the same country shall be taken, it should be a different one,
        thus we need to assure that, format {proxy_host: x, proxy_port: y}
        :param country:
        :return: host and port in string format
        """
        try:
            proxy = """select host, port from d1rpgcvqcran0q.public.proxies where is_blocked = false and 
                currently_used = false and country = %s and user_using_this_proxy is null limit 1"""
            self.cur.execute(proxy, (country,))
            result = self.cur.fetchall()
            if len(result) > 0:
                proxy_host = result[0][0].strip()
                proxy_port = result[0][1].strip()
            else:
                raise Exception(f'No unblocked and unused proxy exists in {country}.\n')
            self.update_proxy_db_record(proxy_host, proxy_port)
            return str(proxy_host), str(proxy_port)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def unflag_proxy(self, proxy_host, proxy_port):
        """
        Once test complete, currently used proxy shall be unflagged.
        :param proxy:
        :return:
        """
        try:
            sql = """update d1rpgcvqcran0q.public.proxies set last_usage = current_timestamp, currently_used = false 
                where host = %s and port = %s"""
            self.cur.execute(sql, (proxy_host, proxy_port))
            self.conn.commit()
            print(f"Proxy {proxy_host}:{proxy_port} unflagged.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def update_proxy_db_record(self, proxy_host, proxy_port):
        """
        Updating last_usage column for proxy currently in use.
        :param proxy_host:
        :param proxy_port:
        :return:
        """
        try:
            proxy = """update d1rpgcvqcran0q.public.proxies set last_usage = current_timestamp, currently_used = true 
                where host = %s and port = %s"""
            self.cur.execute(proxy, (proxy_host, proxy_port,))
            self.conn.commit()
            print(f"Proxy {proxy_host}:{proxy_port} flagged.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def deactivate_proxy_in_db(self, proxy_host, proxy_port):
        """
        Deactivates the proxy by setting is_blocked parameter true for given host and port
        :param proxy_host:
        :param proxy_port:
        :return:
        """
        try:
            proxy = """update d1rpgcvqcran0q.public.proxies set last_usage = current_timestamp, is_blocked = true 
                where host = %s and port = %s"""
            self.cur.execute(proxy, (proxy_host, proxy_port,))
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def update_liked_post(self, postid, testuserid, testrunid):
        """
        Update liked_post table with post_id that was liked, just recently.
        :return:
        """
        try:
            sql = """insert into d1rpgcvqcran0q.public.liked_post(post_id, testuser_id, testrun_id, liked) 
                values(%s,%s,%s,True) on conflict on constraint liked_post_pkey do nothing"""
            self.cur.execute(sql, (postid, testuserid, testrunid))
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise Exception(f"Data of liked post {postid} could not be stored.")

    def update_followed_post(self, author_id, post_id, test_user_id, test_run_id):
        """
        Update the author_followed table with the content creator a test user started to follow in a specific test run.
        :param author_id:
        :param post_id:
        :param test_user_id:
        :param test_run_id:
        :return:
        """
        try:
            sql_author_followed = """insert into d1rpgcvqcran0q.public.author_followed(author_id, test_user_id, 
                test_run_id, post_id, followed) values(%s,%s,%s,%s,True)"""
            self.cur.execute(sql_author_followed, (author_id, test_user_id, test_run_id, post_id,))
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise Exception(f"Following the content creator of {post_id} could not be stored.")

    def get_previous_verification_code(self, test_user_id):
        """
        Retrieve the previously working verification code
        :param test_user_id:
        :return: verification code stored in usermanager table
        """
        sql_get_verification_code = """select previous_verification_code from d1rpgcvqcran0q.public.usermanager
        where userid = %s"""
        self.cur.execute(sql_get_verification_code, (test_user_id,))
        result = self.cur.fetchall()[0][0]
        return result

    def update_verification_code(self, verification_code, test_user_id):
        """
        Update the recently received verification code if it login worked!
        :param verification_code:
        :param test_user_id:
        :return: nothing
        """
        sql_update_verification_code = """update d1rpgcvqcran0q.public.usermanager set previous_verification_code = %s
        where userid = %s"""
        self.cur.execute(sql_update_verification_code, (verification_code, test_user_id,))
        self.conn.commit()

    def get_password(self, test_user_id):
        """
        Retrieve password for test user.
        :param test_user_id:
        :return:
        """
        sql_password = """select password from d1rpgcvqcran0q.public.usermanager where userid = %s"""
        self.cur.execute(sql_password, (test_user_id,))
        sql_password_result = self.cur.fetchall()[0][0].strip()
        if len(sql_password_result) > 0:
            password = sql_password_result
        else:
            password = ''
        return password

    def get_phone_number(self, test_user_id):
        sql_get_phone_number = """select distinct phone_number from usermanager where userid = %s"""
        self.cur.execute(sql_get_phone_number, (test_user_id,))
        sql_get_phone_number_result = self.cur.fetchall()[0][0].strip()
        if len(sql_get_phone_number_result) > 0:
            phone_number = sql_get_phone_number_result
        else:
            phone_number = ''
        return phone_number

    def get_country_phone_number_prefix(self, test_user_id):
        sql_get_country_phone_number_prefix = """select country_phone_number_prefix
         from usermanager where userid = %s"""
        self.cur.execute(sql_get_country_phone_number_prefix, (test_user_id,))
        sql_get_country_phone_number_prefix_result = self.cur.fetchall()[0][0].strip()
        if len(sql_get_country_phone_number_prefix_result) > 0:
            country_phone_number_prefix = sql_get_country_phone_number_prefix_result
        else:
            country_phone_number_prefix = ''
        return country_phone_number_prefix

    def get_proxy_host(self, test_user_id):
        sql_get_proxy_host = """select host from usermanager where userid = %s"""
        self.cur.execute(sql_get_proxy_host, (test_user_id,))
        sql_get_proxy_host_result = self.cur.fetchall()[0][0].strip()
        if len(sql_get_proxy_host_result) > 0:
            proxy_host = sql_get_proxy_host_result
        else:
            proxy_host = ''
        return proxy_host

    def get_proxy_port(self, test_user_id):
        sql_get_proxy_port = """select port from usermanager where userid = %s"""
        self.cur.execute(sql_get_proxy_port, (test_user_id,))
        sql_get_proxy_port_result = self.cur.fetchall()[0][0].strip()
        if len(sql_get_proxy_port_result) > 0:
            proxy_port = sql_get_proxy_port_result
        else:
            proxy_port = ''
        return proxy_port

    def get_proxy_country(self, test_user_id):
        sql_get_proxy_country = """select country from usermanager where userid = %s"""
        self.cur.execute(sql_get_proxy_country, (test_user_id,))
        sql_get_proxy_country_result = self.cur.fetchall()[0][0].strip()
        if len(sql_get_proxy_country_result) > 0:
            proxy_country = sql_get_proxy_country_result
        else:
            proxy_country = ''
        return proxy_country

    def get_duration(self, post_id):
        """
        Retrieve the duration of a post, if the post_id exists, if not return 0 as the duration
        :param post_id:
        :return:
        """
        sql_get_duration = """select video_druation_sec from posts where id = %s limit 1"""
        self.cur.execute(sql_get_duration, (post_id,))
        duration = self.cur.fetchall()
        if len(duration) > 0:
            if duration[0][0] is None:
                return 0
            else:
                return duration[0][0]
        else:
            return 0

    def store_longer_watched_post(self, post_id, test_user_id, test_run_id, time_watched, percentage_watched):
        """
        Store the post id, test run id, and test user id where the bot watched a post longer than usually.
        :return:
        """
        try:
            sql_store_longer_watched_post = """insert into longer_watched_posts(post_id, test_user_id, test_run_id, 
            time_watched, percentage_watched) values(%s,%s,%s,%s,%s)"""
            self.cur.execute(sql_store_longer_watched_post, (post_id, test_user_id, test_run_id, time_watched,
                                                             percentage_watched,))
            self.conn.commit()
        except:
            print(f"Longer watched post {post_id} data could not be stored.")

    def update_internal_music_ids(self):
        """
        Update all those rows in posts table for which not music internal id has been stored yet.
        :return:
        """
        sql_update_internal_music_id = """select distinct musicid from posts where music_internal_id is null"""
        self.cur.execute(sql_update_internal_music_id, ())
        sql_update_internal_music_id_results = self.cur.fetchall()
        for post_row_music_id in sql_update_internal_music_id_results:
            sql_get_music_internal_id = """select internal_id from music where id = %s limit 1"""
            self.cur.execute(sql_get_music_internal_id, (post_row_music_id[0],))
            sql_get_music_internal_id_results = self.cur.fetchall()[0][0]
            sql_update_posts_with_internal_music_id = """update posts set music_internal_id = %s where musicid = %s"""
            self.cur.execute(sql_update_posts_with_internal_music_id, (sql_get_music_internal_id_results,
                                                                       post_row_music_id,))
            self.conn.commit()

    def get_cookie_values(self, cookies, test_user_id):
        values = []
        for cookie in cookies:
            if 'domain' in cookie.keys():
                domain = cookie.get('domain')
            else:
                domain = None
            if 'expiry' in cookie.keys():
                expiry = cookie.get('expiry')
            else:
                expiry = None
            if 'httpOnly' in cookie.keys():
                httpOnly = cookie.get('httpOnly')
            else:
                httpOnly = None
            if 'name' in cookie.keys():
                name = cookie.get('name')
            else:
                name = None
            if 'path' in cookie.keys():
                path = cookie.get('path')
            else:
                path = None
            if 'secure' in cookie.keys():
                secure = cookie.get('secure')
            else:
                secure = None
            if 'samesite' in cookie.keys():
                samesite = cookie.get('samesite')
            else:
                samesite = None
            if 'value' in cookie.keys():
                value = cookie.get('value')
            else:
                value = None
            values.append({
                'user_id': test_user_id,
                'domain': domain,
                'expiry': expiry,
                'httpOnly': httpOnly,
                'name': name,
                'path': path,
                'secure': secure,
                'samesite': samesite,
                'value': value
            })
        return values

    def add_entry_user_cookies_db(self, cookies, test_user_id):
        """
        Add given cookies for current test user.
        :param cookies:
        :return:
        """
        sql_add_cookies = """insert into user_cookies(user_id, domain, expiry, httponly, name, path, secure, samesite, 
        value) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        values = self.get_cookie_values(cookies, test_user_id)
        for value in values:
            self.cur.execute(sql_add_cookies, (value.get('user_id'), value.get('domain'), value.get('expiry'),
                                               value.get('httpOnly'), value.get('name'), value.get('path'),
                                               value.get('secure'), value.get('samesite'), value.get('value'), ))
            self.conn.commit()

    def update_user_cookies_db(self, cookies, test_user_id):
        """
        Update given cookies in user_cookies db for current test user.
        :param cookies:
        :return:
        """
        sql_update_cookie = """update user_cookies set domain = %s, expiry = %s, httponly = %s, name = %s, path = %s, 
        secure = %s, samesite = %s, value = %s where user_id = %s"""
        values = self.get_cookie_values(cookies, test_user_id)
        for value in values:
            self.cur.execute(sql_update_cookie, (value.get('domain'), value.get('expiry'),
                                                 value.get('httpOnly'), value.get('name'), value.get('path'),
                                                 value.get('secure'), value.get('samesite'), value.get('value'),
                                                 value.get('user_id'), ))
            self.conn.commit()

    def get_cookies_db(self, test_user_id):
        """
        Retrieve cookie object for current test_user from user_cookie table.
        Attention: if a column is null there doesn't exist a value for that parameter, thus parameter must not be part
        of dictionary.
        :return: cookies = [dictionaries] ; dictionary = {domain: , expiry: , etc.}
        """
        sql_retrieve_cookies = """select domain, expiry, httponly, name, path, secure, samesite, 
        value from user_cookies where user_id = %s"""
        cookies = []
        self.cur.execute(sql_retrieve_cookies, (test_user_id, ))
        results = self.cur.fetchall()
        if len(results) > 0:
            for item in results:
                cookie_attributes = {}

                domain = item[0]
                if domain is not None:
                    cookie_attributes['domain'] = domain.strip()

                expiry = item[1]
                if expiry is not None:
                    cookie_attributes['expiry'] = expiry

                httpOnly = item[2]
                if httpOnly is not None:
                    cookie_attributes['httpOnly'] = httpOnly

                name = item[3]
                if name is not None:
                    cookie_attributes['name'] = name.strip()

                path = item[4]
                if path is not None:
                    cookie_attributes['path'] = path.strip()

                secure = item[5]
                if secure is not None:
                    cookie_attributes['secure'] = secure

                samesite = item[6]
                if samesite is not None:
                    cookie_attributes['samesite'] = samesite.strip()

                value = item[7]
                if value is not None:
                    cookie_attributes['value'] = value.strip()

                cookies.append(cookie_attributes)
            return cookies
        else:
            return []



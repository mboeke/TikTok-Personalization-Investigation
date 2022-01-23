
from src.DatabaseHelper import *


class TestRun:
    """
    class inserts extracted data to database
    - creates test run
    :param test_data:
        {
        "testuserinfo": [
            {"testuserid": 1, "email": "bertman@mailinator.com", "password": "%J0ftE999yQVg2"},
            {"testuserid": 2, "email": "loc2021@mailinator.com", "password": "%@NreeHIwb*55O5@zD48"}
        ],
        "description": "filtering different_posts_noise, same location, same language, two different accounts",
        "proxy": str(proxy_US.get('proxy_host')) + ":" + str(proxy_US.get('proxy_port')),
        "browser_language": "en"
        }
    """

    def __init__(self, test_data):
        self.test_run_id = None
        self.database = DatabaseHelper()
        self.test_data = test_data
        self.create_test_run()

    def __enter__(self):
        print(f"TestRun {self.test_run_id} started.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"TestRun {self.test_run_id} executed.")

    def create_test_run(self):
        """
        Create test run data to store collected data correctly
        :param
        :return:
        """
        try:
            # get test run id, set test_run_id as set
            self.database.cur.execute("""
                with next_id as (
                    select * from testrunids
                    where set = false
                    order by id asc
                    limit 1
                )
    
                update testrunids
                set set = true
                where id = (select id from next_id)""")
            self.database.conn.commit()

            # get id
            self.database.cur.execute("""
                select id from testrunids
                where set = true
                order by id desc
                limit 1""")
            self.test_run_id = self.database.cur.fetchone()[0]

            for item in self.test_data:
                test = """insert into testrun(id,testuserid,ip_used,country,browser_language)
                values(%s,%s,%s,%s,%s) on conflict on constraint testrun_pkey do nothing"""
                self.database.cur.execute(test, (
                    self.test_run_id,  # id
                    item.get('test_user_id'),  # test_user_id
                    str(item.get("proxy").get('proxy_host')) + ':' +
                    str(item.get('proxy').get('proxy_port')),  # ip_used
                    item.get('proxy').get("country"),
                    item.get("browser_language"),
                ))
                self.database.conn.commit()
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as cursor_error:
            print(cursor_error)
            print("Instantiating db connection and trying to create test run again.")
            self.database = DatabaseHelper()
            self.create_test_run()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise Exception("Test run could not be created.")

    def store_test_duration(self, duration, test_user_id):
        try:
            sql = """update testrun 
            set duration = (%s) where id = (%s) and testuserid = (%s)"""
            self.database.cur.execute(sql, (duration, self.test_run_id, test_user_id))
            self.database.conn.commit()
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as cursor_error:
            print(cursor_error)
            print("Instantiating db connection and trying to store test run data again.")
            self.database = DatabaseHelper()
            self.store_test_duration(duration, test_user_id)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise Exception("Post data could not be stored")

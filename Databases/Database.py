# Developed by NickolaQ Trekov
import logging

import psycopg2
from psycopg2._psycopg import OperationalError
from app.config import Settings


settings = Settings()
logger = logging.getLogger(__name__)


class Database_stock:
    """
    Database base
    """

    def __init__(self, host: str, database: str, user: str, password: str):
        try:
            # self.con = psycopg2.connect(host=host, database=database, user=user, password=password)
            self.database = database
            self.host = host
            self.user = user
            self.password = password
            # self.cur = self.con.cursor()
        except:
            logger.error(f'DB: {database}|ERROR| CONNECT TO DATABASE')

    def get(self, query):
        try:
            connection = psycopg2.connect(host=self.host, database=self.database,
                                          user=self.user, password=self.password, connect_timeout=5)
            with connection.cursor() as cursor:
                cursor.execute(query)
                logger.warning('DB: {}|GET|executed get transaction:\n {}'.format(self.database, query))
                result = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                dict_ = []
                if result:
                    for row in result:
                        dict_.append(dict(zip(colnames, row)))
                    logger.warning('DB: {}|GET|response data:\n{}'.format(self.database, dict_))
                    return dict_
        except OperationalError as e:
            logger.error(f"DB: {self.database}|GET| Error: {e}")
            return None
        except TimeoutError:
            logger.error(f"DB: {self.database}| TIMEOUT ERROR")
            return None
        finally:
            connection.close()

    def get_one(self,query):
        result = self.get(query)
        if result != 0:
            return result[0]
        else:
            return result


Shiptor_Database = Database_stock(host=settings.shiptor_host,
                                  database="shiptor",
                                  user=settings.user,
                                  password=settings.password)
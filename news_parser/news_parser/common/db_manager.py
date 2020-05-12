from typing import Optional
from .types import DBParamsDTO
import psycopg2


class DB:
    def __init__(self, params: DBParamsDTO):
        self._db: Optional[psycopg2._psycopg.connection] = None
        self.cur: Optional[psycopg2._psycopg.cursor] = None
        self.connect(params)

    def connect(self, params: DBParamsDTO):
        self._db = psycopg2.connect(
            host=params.host,
            port=params.port,
            dbname=params.name,
            user=params.user,
            password=params.password
        )
        self.cur = self._db.cursor()

    def disconnect(self):
        self.cur.close()
        self._db.close()
        self.cur, self._db = None, None

    def commit(self):
        self._db.commit()

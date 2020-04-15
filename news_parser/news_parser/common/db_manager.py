from typing import Optional
from .types import TypeDBParams
import sqlite3


class DBException(Exception):
    pass


class DB:
    _db: Optional[sqlite3.Connection] = None
    cur: Optional[sqlite3.Cursor] = None

    def __init__(self, params: TypeDBParams):
        self.connect(params)

    def connect(self, params: TypeDBParams):
        try:
            self._db = sqlite3.connect(params.name)
            self.cur = self._db.cursor()
        except sqlite3.DatabaseError as e:
            self._db, self.cur = None, None
            raise DBException(str(e))

    def disconnect(self):
        self.cur.close()
        self._db.close()
        self.cur, self._db = None, None

    def commit(self):
        self._db.commit()

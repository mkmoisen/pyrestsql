from tests.peewee_core import TestPeewee, TestPeeweeSimple, setup_peewee_database
from peewee import SqliteDatabase


def setup_peewee_sqlite_database():
    db = SqliteDatabase('sqlite_test.db', pragmas={'foreign_keys': 'ON'})

    return setup_peewee_database(db)


class TestPeeweeSqlite(TestPeewee):
    def setup_database(self):
        return setup_peewee_sqlite_database()


class TestPeeweeSimpleSqlite(TestPeeweeSimple):
    def setup_database(self):
        return setup_peewee_sqlite_database()

    def user_id_foreign_key_error(self):
        return {'unknown': 'No resource with this value exists.'}

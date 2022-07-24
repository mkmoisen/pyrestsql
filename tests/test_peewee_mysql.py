from tests.peewee_core import TestPeewee, TestPeeweeSimple, setup_peewee_database
from peewee import MySQLDatabase


def setup_peewee_mysql_database():
    db = MySQLDatabase(
        'mysql', user='mysql', password='mysql',
        autoconnect=False
    )

    return setup_peewee_database(db)


class TestPeeweeMysql(TestPeewee):
    def setup_database(self):
        return setup_peewee_mysql_database()


class TestPeeweeSimpleMysql(TestPeeweeSimple):
    def setup_database(self):
        return setup_peewee_mysql_database()

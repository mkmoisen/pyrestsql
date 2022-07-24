from tests.peewee_core import TestPeewee, TestPeeweeSimple, setup_peewee_database
from peewee import PostgresqlDatabase



def setup_peewee_postgres_database():
    db = PostgresqlDatabase(
        'pyrestsql', user='pyrestsql', password='pyrestsql',
        autoconnect=False
    )

    return setup_peewee_database(db)


class TestPeeweePostgres(TestPeewee):
    def setup_database(self):
        return setup_peewee_postgres_database()


class TestPeeweeSimplePostgres(TestPeeweeSimple):
    def setup_database(self):
        return setup_peewee_postgres_database()

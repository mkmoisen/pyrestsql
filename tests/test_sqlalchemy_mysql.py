import sqlalchemy
from tests.sqlalchemy_core import setup_sqlalchemy_database, TestSQLAlchemy, TestSQLAlchemySimple


def setup_sqlalchemy_mysql_database():
    engine = sqlalchemy.create_engine("mysql+pymysql://mysql:mysql@localhost/mysql", echo=True, future=True)

    return setup_sqlalchemy_database(engine)


class TestSQLAlchemyMysql(TestSQLAlchemy):
    def setup_database(self):
        return setup_sqlalchemy_mysql_database()


class TestSQLAlchemySimpleMysql(TestSQLAlchemySimple):
    def setup_database(self):
        return setup_sqlalchemy_mysql_database()

import sqlalchemy
from tests.sqlalchemy_core import setup_sqlalchemy_database, TestSQLAlchemy, TestSQLAlchemySimple


def setup_sqlalchemy_postgres_database():
    engine = sqlalchemy.create_engine(
        "postgresql+psycopg2://pyrestsql:pyrestsql@localhost:5432/pyrestsql",
        echo=True, future=True
    )

    return setup_sqlalchemy_database(engine)


class TestSQLAlchemyPostgres(TestSQLAlchemy):
    def setup_database(self):
        return setup_sqlalchemy_postgres_database()


class TestSQLAlchemySimplePostgres(TestSQLAlchemySimple):
    def setup_database(self):
        return setup_sqlalchemy_postgres_database()



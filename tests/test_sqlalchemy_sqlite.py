import sqlalchemy
from tests.sqlalchemy_core import setup_sqlalchemy_database, TestSQLAlchemy, TestSQLAlchemySimple


def setup_sqlalchemy_sqlite_database():
    engine = sqlalchemy.create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text('pragma foreign_keys=on'))

    return setup_sqlalchemy_database(engine)


class TestSQLAlchemySqlite(TestSQLAlchemy):
    def setup_database(self):
        return setup_sqlalchemy_sqlite_database()


class TestSQLAlchemySimpleSqlite(TestSQLAlchemySimple):
    def setup_database(self):
        return setup_sqlalchemy_sqlite_database()

    def user_id_foreign_key_error(self):
        return {'unknown': 'No resource with this value exists.'}

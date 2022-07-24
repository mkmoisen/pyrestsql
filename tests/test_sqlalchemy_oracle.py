import sqlalchemy
from tests.sqlalchemy_core import setup_sqlalchemy_database, TestSQLAlchemy, TestSQLAlchemySimple

def setup_sqlalchemy_oracle_database():
    engine = sqlalchemy.create_engine("oracle://xxmd:xxMD123$@MATTHEW", echo=True, future=True)
    return setup_sqlalchemy_database(engine)


class TestSQLAlchemyOracle(TestSQLAlchemy):
    def setup_database(self):
        return setup_sqlalchemy_oracle_database()


class TestSQLAlchemySimpleOracle(TestSQLAlchemySimple):
    def setup_database(self):
        return setup_sqlalchemy_oracle_database()

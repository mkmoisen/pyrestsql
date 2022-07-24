from datetime import datetime

from flask import Flask
from pyrestsql.api.sqlalchemy import SqlAlchemyApi as SqlAlchemyApi, insert_where as insert_where_sqlalchemy
from pyrestsql.api.sqlalchemy.simple import SimpleModelApi as SqlAlchemySimpleModelApi
import marshmallow
import sqlalchemy
import sqlalchemy.orm

from sqlalchemy import select, update, delete
from tests.core import TestBase


def setup_sqlalchemy_database(engine):
    Base = sqlalchemy.orm.declarative_base()

    class User(Base):
        __tablename__ = 'test_users'
        id = _make_id_column(engine, __tablename__)
        email = sqlalchemy.Column(sqlalchemy.String(30), nullable=False, unique=True)

    class UserAddress(Base):
        __tablename__ = 'test_user_addresses'
        user_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('test_users.id', ondelete='CASCADE'), primary_key=True)
        street = sqlalchemy.Column(sqlalchemy.String(30), nullable=False)

    class Project(Base):
        __tablename__ = 'test_projects'
        id = _make_id_column(engine, __tablename__)
        name = sqlalchemy.Column(sqlalchemy.String(30), nullable=False)
        user_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('test_users.id', ondelete='CASCADE'), nullable=False)

    print('drop_all start', datetime.now())
    Base.metadata.drop_all(engine)
    print('drop_all end', datetime.now())
    # TODO for Oracle, drop_all is taking 15 seconds??
    Base.metadata.create_all(engine)

    Session = sqlalchemy.orm.sessionmaker(engine)

    models = {
        'User': User,
        'UserAddress': UserAddress,
        'Project': Project,
    }

    return Session, models


def _make_id_column(engine, table_name):
    if not str(engine.url).startswith('oracle'):
        return sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    return sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.Sequence(f'{table_name}_seq'), primary_key=True)


def setup_sqlalchemy_app(Session, models):
    app = Flask('peewee_postgres')
    app.url_map.strict_slashes = False
    app.testing = True

    User = models['User']
    UserAddress = models['UserAddress']
    Project = models['Project']

    class UserApi(SqlAlchemyApi):
        url_prefix = '/api/users/'

        def queryset(self):
            return sqlalchemy.select(User)

        def serializer_class(self):
            class Serializer(marshmallow.Schema):
                id = marshmallow.fields.Int(dump_only=True)
                email = marshmallow.fields.Str(required=True)

            return Serializer

    class UserAddressApi(SqlAlchemyApi):
        url_prefix = '/api/user-addresses/'

        model = UserAddress

        def serializer_class(self):
            class Serializer(marshmallow.Schema):
                user_id = marshmallow.fields.Int(required=True)
                street = marshmallow.fields.Str(required=True)

            return Serializer

    class ProjectApi(SqlAlchemyApi):
        url_prefix = '/api/projects'

        def queryset(self):
            return sqlalchemy.select(Project)

        def serializer_class(self):
            class Serializer(marshmallow.Schema):
                id = marshmallow.fields.Int(dump_only=True)
                name = marshmallow.fields.Str(required=True)
                user_id = marshmallow.fields.Int(required=True)

            return Serializer

    UserApi.register_app(app, Session)
    UserAddressApi.register_app(app, Session)
    ProjectApi.register_app(app, Session)

    return app


def setup_sqlalchemy_simple_app(Session, models):
    app = Flask('peewee_postgres')
    app.url_map.strict_slashes = False
    app.testing = True

    User = models['User']
    UserAddress = models['UserAddress']
    Project = models['Project']

    user_api = SqlAlchemySimpleModelApi(url_prefix='/api/users/')

    class UserSerializer(marshmallow.Schema):
        id = marshmallow.fields.Int(dump_only=True)
        email = marshmallow.fields.Str(required=True)

    @user_api.get(schema=UserSerializer)
    def get_user(pk):
        return select(User).where(User.id == pk)

    @user_api.get_many(schema=UserSerializer)
    def get_many_users():
        return select(User)

    @user_api.post(schema=UserSerializer)
    def post_user(payload):
        return insert_where_sqlalchemy(User, **payload)

    @user_api.patch(schema=UserSerializer)
    def patch_user(pk, payload):
        return update(User).values(**payload).where(User.id == pk)

    @user_api.delete()
    def delete_user(pk):
        return delete(User).where(User.id == pk)

    user_address_api = SqlAlchemySimpleModelApi(url_prefix='/api/user-addresses/')

    class UserAddressSerializer(marshmallow.Schema):
        user_id = marshmallow.fields.Int(required=True)
        street = marshmallow.fields.Str(required=True)

    @user_address_api.get(schema=UserAddressSerializer)
    def get_user_address(pk):
        return select(UserAddress).where(UserAddress.user_id == pk)

    @user_address_api.post(schema=UserAddressSerializer)
    def post_user_address(payload):
        return insert_where_sqlalchemy(UserAddress, **payload)

    @user_address_api.patch(schema=UserAddressSerializer)
    def patch_user_address(pk, payload):
        return update(UserAddress).values(**payload).where(UserAddress.user_id == pk)

    @user_address_api.delete()
    def delete_user_address(pk):
        return delete(UserAddress).where(UserAddress.user_id == pk)

    project_api = SqlAlchemySimpleModelApi(url_prefix='/api/projects/')

    class ProjectSerializer(marshmallow.Schema):
        id = marshmallow.fields.Int(dump_only=True)
        name = marshmallow.fields.Str(required=True)
        user_id = marshmallow.fields.Int(required=True)

    @project_api.get(schema=ProjectSerializer)
    def get_project(pk):
        return select(Project).where(Project.id == pk)

    @project_api.get_many(schema=ProjectSerializer)
    def get_many_projects():
        return select(Project)

    @project_api.post(schema=ProjectSerializer)
    def post_project(payload):
        return insert_where_sqlalchemy(Project, **payload)

    @project_api.patch(schema=ProjectSerializer)
    def patch_project(pk, payload):
        return update(Project).values(**payload).where(Project.id == pk)

    @project_api.delete()
    def delete_project(pk):
        return delete(Project).where(Project.id == pk)

    user_api.register_app(app, Session)
    user_address_api.register_app(app, Session)
    project_api.register_app(app, Session)

    return app


class TestSQLAlchemy(TestBase):
    def setup_database(self):
        db, models = None, None
        raise NotImplementedError

    def setup_app(self, Session, models):
        return setup_sqlalchemy_app(Session, models)

    def init(self):
        self.Session, self.models = self.setup_database()
        self.app = self.setup_app(self.Session, self.models)
        self.testclient = self.app.test_client()


class TestSQLAlchemySimple(TestSQLAlchemy):
    def setup_app(self, db, models):
        return setup_sqlalchemy_simple_app(db, models)

from tests.core import TestBase

from peewee import Model, CharField, ForeignKeyField
from flask import Flask
from pyrestsql.api.peewee import Api as PeeweeApi, insert_where as insert_where_peewee
from pyrestsql.api.peewee.simple import SimpleModelApi as PeeweeSimpleModelApi
import marshmallow


def setup_peewee_database(db):
    class BaseModel(Model):
        class Meta:
            database = db
            legacy_table_names = False

    class User(BaseModel):
        email = CharField(max_length=30, null=False, unique=True)

        class Meta:
            table_name = 'test_users'

    class UserAddress(BaseModel):
        user = ForeignKeyField(User, primary_key=True, on_delete='CASCADE')
        street = CharField(max_length=30, null=False)

        class Meta:
            table_name = 'test_user_addresses'

    class Project(BaseModel):
        name = CharField(max_length=30, null=False)
        user = ForeignKeyField(User, null=False, on_delete='CASCADE')

        class Meta:
            table_name = 'test_projects'

    with db:
        db.drop_tables([User, UserAddress, Project])
        db.create_tables([User, UserAddress, Project])

    models = {
        'User': User,
        'UserAddress': UserAddress,
        'Project': Project,
    }

    return db, models


def setup_peewee_app(db, models):
    app = Flask('peewee_postgres')
    app.url_map.strict_slashes = False
    app.testing = True

    User = models['User']
    UserAddress = models['UserAddress']
    Project = models['Project']

    class UserApi(PeeweeApi):
        url_prefix = '/api/users/'

        def queryset(self):
            return User.select()

        def serializer_class(self):
            class Serializer(marshmallow.Schema):
                id = marshmallow.fields.Int(dump_only=True)
                email = marshmallow.fields.Str(required=True)

            return Serializer

    class UserAddressApi(PeeweeApi):
        url_prefix = '/api/user-addresses/'

        model = UserAddress

        def serializer_class(self):
            class Serializer(marshmallow.Schema):
                user_id = marshmallow.fields.Int(required=True)
                street = marshmallow.fields.Str(required=True)

            return Serializer

    class ProjectApi(PeeweeApi):
        url_prefix = '/api/projects/'

        def queryset(self):
            return Project.select()

        def serializer_class(self):
            class Serializer(marshmallow.Schema):
                id = marshmallow.fields.Int(dump_only=True)
                name = marshmallow.fields.Str(required=True)
                user_id = marshmallow.fields.Int(required=True)

            return Serializer

    UserApi.register_app(app, db)
    UserAddressApi.register_app(app, db)
    ProjectApi.register_app(app, db)

    return app


def setup_peewee_simple_app(db, models):
    app = Flask('peewee_postgres')
    app.url_map.strict_slashes = False
    app.testing = True

    User = models['User']
    UserAddress = models['UserAddress']
    Project = models['Project']

    user_api = PeeweeSimpleModelApi(url_prefix='/api/users/')

    class UserSerializer(marshmallow.Schema):
        id = marshmallow.fields.Int(dump_only=True)
        email = marshmallow.fields.Str(required=True)

    @user_api.get(schema=UserSerializer)
    def get_user(pk):
        return User.select().where(User.id == pk)

    @user_api.get_many(schema=UserSerializer)
    def get_many_users():
        return User.select()

    @user_api.post(schema=UserSerializer)
    def post_user(payload):
        return insert_where_peewee(User, **payload)

    @user_api.patch(schema=UserSerializer)
    def patch_user(pk, payload):
        return User.update(**payload).where(User.id == pk)

    @user_api.delete()
    def delete_user(pk):
        return User.delete().where(User.id == pk)

    user_address_api = PeeweeSimpleModelApi(url_prefix='/api/user-addresses/')

    class UserAddressSerializer(marshmallow.Schema):
        user_id = marshmallow.fields.Int(required=True)
        street = marshmallow.fields.Str(required=True)

    @user_address_api.get(schema=UserAddressSerializer)
    def get_user_address(pk):
        return UserAddress.select().where(UserAddress.user_id == pk)

    @user_address_api.post(schema=UserAddressSerializer)
    def post_user_address(payload):
        return insert_where_peewee(UserAddress, **payload)

    @user_address_api.patch(schema=UserAddressSerializer)
    def patch_user_address(pk, payload):
        return UserAddress.update(**payload).where(UserAddress.user_id == pk)

    @user_address_api.delete()
    def delete_user_address(pk):
        return UserAddress.delete().where(UserAddress.user_id == pk)

    project_api = PeeweeSimpleModelApi(url_prefix='/api/projects/')

    class ProjectSerializer(marshmallow.Schema):
        id = marshmallow.fields.Int(dump_only=True)
        name = marshmallow.fields.Str(required=True)
        user_id = marshmallow.fields.Int(required=True)

    @project_api.get(schema=ProjectSerializer)
    def get_project(pk):
        return Project.select().where(Project.id == pk)

    @project_api.get_many(schema=ProjectSerializer)
    def get_many_projects():
        return Project.select()

    @project_api.post(schema=ProjectSerializer)
    def post_project(payload):
        return insert_where_peewee(Project, **payload)

    @project_api.patch(schema=ProjectSerializer)
    def patch_project(pk, payload):
        return Project.update(**payload).where(Project.id == pk)

    @project_api.delete()
    def delete_project(pk):
        return Project.delete().where(Project.id == pk)

    user_api.register_app(app, db)
    user_address_api.register_app(app, db)
    project_api.register_app(app, db)

    return app


class TestPeewee(TestBase):

    def setup_database(self):
        db, models = None, None
        raise NotImplementedError

    def setup_app(self, db, models):
        return setup_peewee_app(db, models)

    def init(self):
        self.db, self.models = self.setup_database()
        self.app = self.setup_app(self.db, self.models)
        self.testclient = self.app.test_client()


class TestPeeweeSimple(TestPeewee):
    def setup_app(self, db, models):
        return setup_peewee_simple_app(db, models)

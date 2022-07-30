import inspect
import re

import psycopg2
from flask import Blueprint, jsonify, Response, request
from pyrestsql.exc import RestError, EntityNotFound, BadInput
from marshmallow import ValidationError, Schema
from marshmallow.schema import SchemaMeta
import logging

logger = logging.getLogger(__name__)


class ApiMetaClass(type):
    def __new__(mcs, name, bases, dct):

        # TODO I dont see any purpose in doing anything before class creation
        # TODO all the things that are being done here could also be done after class creation
        mcs.pre_class_creation(name, bases, dct)

        cls = super().__new__(mcs, name, bases, dct)

        cls.post_class_creation()

        return cls

    @classmethod
    def pre_class_creation(mcs, name, bases, dct):
        mcs.ensure_url_prefix(dct)

        mcs.ensure_model(name, bases, dct)

        mcs.ensure_pagination(dct)

    @staticmethod
    def ensure_not_tuple(value):
        """
        Sole purpose of this is to ignore an accidental comma:

            url_prefix = '/api/foos/,

        """
        if isinstance(value, tuple) and len(value) == 1:
            return value[0]

        return value

    @classmethod
    def ensure_url_prefix(mcs, dct):
        url_prefix = dct.get('url_prefix')
        if url_prefix:
            dct['url_prefix'] = mcs.ensure_not_tuple(url_prefix)

    @classmethod
    def ensure_model(mcs, name, bases, dct):
        model = dct.get('model')
        if model:
            dct[model] = mcs.ensure_not_tuple(model)

        # TODO ensure class instance and not model lol?

    @staticmethod
    def ensure_object_instance(value):
        if inspect.isclass(value):
            return value()

        return value

    @classmethod
    def ensure_pagination(mcs, dct):
        pagination = dct.get('pagination')
        if pagination:
            dct['pagination'] = mcs.ensure_object_instance(pagination)

    @classmethod
    def copy_set(mcs, bases, dct, key):
        return mcs.copy_container(bases, dct, key, set)

    @classmethod
    def copy_list(mcs, bases, dct, key):
        return mcs.copy_container(bases, dct, key, list)

    @classmethod
    def copy_container(mcs, bases, dct, key, type_):
        assert type_ in (list, set)

        container = dct.pop(key, type_())
        if not container:
            container = type_()

        container = type_(container)

        if not container:
            container = next((type_(getattr(base, key)) for base in bases if hasattr(base, key)), type_())

        dct[key] = container

    def post_class_creation(cls):
        if cls.__name__ in ('BaseApi', '_FileApi'):
            return
        cls.require_model_or_queryset()
        cls.add_missing_model_or_queryset()
        cls.ensure_apis()
        cls.ensure_filterset_fields()
        cls.wrap_perform_create_for_integrity_errors()
        cls.wrap_perform_update_for_integrity_errors()

    def require_model_or_queryset(cls):
        if cls.url_prefix is None:
            return

        obj = cls()

        if cls.model is None and obj.queryset() is None:
            raise Exception(
                f"Class {cls.__name__} must either define model or return a not-None value from queryset()"
                f", because url_prefix is defined"
            )

    def add_missing_model_or_queryset(cls):
        raise NotImplementedError

    def ensure_apis(cls):
        cls.copy_apis()
        cls.validate_apis()

    def copy_apis(cls):
        if cls.apis is None:
            cls.apis = set()

        if isinstance(cls.apis, str):
            cls.apis = {cls.apis}

        cls.apis = set(cls.apis)

    def validate_apis(cls):
        valid_apis = {'GET', 'GET_MANY', 'POST', 'PATCH', 'DELETE'}

        invalid_apis = [
            api for api in cls.apis
            if api not in valid_apis
        ]

        if invalid_apis:
            raise NotImplementedError(f'{cls.__name__}.apis contained invalid api(s): {", ".join(invalid_apis)}')

    def ensure_filterset_fields(cls):
        cls.copy_filterset_fields()
        cls.ensure_filterset_class()
        cls.generate_filterset_instance()

    def copy_filterset_fields(cls):
        if cls.filterset_fields is None:
            cls.filterset_fields = []

        if isinstance(cls.filterset_fields, str):
            cls.filterset_fields = [cls.filterset_fields]

        cls.filterset_fields = list(cls.filterset_fields)

    def ensure_filterset_class(cls):
        raise NotImplementedError

    def generate_filterset_instance(cls):
        raise NotImplementedError

    def wrap_perform_create_for_integrity_errors(cls):
        raise NotImplementedError

    def wrap_perform_update_for_integrity_errors(cls):
        raise NotImplementedError


class ErrorHandler:
    def register_errorhandlers(self, app):
        app.errorhandler(404)(self._handle_404)
        app.errorhandler(Exception)(self._handle_uncaught_exception)
        app.errorhandler(RestError)(self._handle_rest_error)
        app.errorhandler(ValidationError)(self._handle_marshmallow_validation_error)

    def _handle_404(self, ex):
        """
        This will be triggered when the user types in a URL that doesn't match any of our routes
        """
        logger.exception('The URL requested was incorrect')
        messages = str(ex)
        if hasattr(ex, 'messages'):
            messages = ex.messages
        error_response = {
            'error': messages,
        }
        return jsonify(error_response), 404

    def _handle_uncaught_exception(self, ex):
        print('hello', ex)
        logger.exception(ex)
        error_response = {
            'error': str(ex)
        }
        return jsonify(error_response), 500

    def _handle_rest_error(self, ex):
        logger.exception(ex)
        response = {
            'error': ex.messages
        }
        return jsonify(response), ex.code

    def _handle_marshmallow_validation_error(self, ex: ValidationError):
        response = {
            'error': ex.messages
        }
        return jsonify(response), 400


class IntegrityErrorManager:
    def __init__(self, **kwargs):
        self.integrity_error_handler = None
        self.determine_handler()

    def determine_handler(self):
        raise NotImplementedError

    def handle(self, ex, **kwargs):
        raise NotImplementedError


class BaseApi(metaclass=ApiMetaClass):
    url_prefix = None

    model = None

    apis = ['GET', 'GET_MANY', 'POST', 'PATCH', 'DELETE']

    pagination = None

    filterset_class = None
    filterset_fields = None
    filterset = None

    blueprint = None

    error_handler_class = ErrorHandler
    integrity_error_manager_class = IntegrityErrorManager

    def __init__(self, api=None):
        self.api = api

    @classmethod
    def register_app(cls, app, error_handler=None, integrity_error_manager=None):
        blueprint = Blueprint(cls.__name__, __name__)

        cls.blueprint = blueprint

        if 'GET' in cls.apis:
            blueprint.get(f'{cls.url_prefix}/<int:pk>')(cls()._dispatch('GET'))

        if 'GET_MANY' in cls.apis:
            blueprint.get(cls.url_prefix)(cls()._dispatch('GET_MANY'))

        if 'POST' in cls.apis:
            blueprint.post(cls.url_prefix)(cls()._dispatch('POST'))

        if 'PATCH' in cls.apis:
            blueprint.patch(f'{cls.url_prefix}/<int:pk>')(cls()._dispatch('PATCH'))

        if 'DELETE' in cls.apis:
            blueprint.delete(f'{cls.url_prefix}/<int:pk>')(cls()._dispatch('DELETE'))

        cls.error_handler = error_handler or cls.error_handler_class()
        cls.error_handler.register_errorhandlers(app)

        cls.integrity_error_manager = integrity_error_manager or cls.integrity_error_manager_class()

        app.register_blueprint(blueprint)

        return blueprint

    def _dispatch(self, api):
        self.api = api

        return {
            'GET': self.get,
            'GET_MANY': self.get_many,
            'POST': self.post,
            'PATCH': self.patch,
            'DELETE': self.delete
        }[self.api]

    def _ensure_schema(self, schema_class):
        if callable(schema_class):
            schema_class = schema_class()

        if isinstance(schema_class, SchemaMeta):
            return schema_class()

        return schema_class

    def get_documentation(self):
        return _documentation(
            self.get,
            self.get_queryset,
            self.get_permissions,
        )

    def get_many_documentation(self):
        return _documentation(
            self.get_many,
            self.get_many_queryset,
            self.get_many_permissions,
        )

    def post_documentation(self):
        return _documentation(
            self.post,
            self.perform_create,
            self.post_permissions,
        )

    def patch_documentation(self):
        return _documentation(
            self.patch,
            self.perform_update,
            self.patch_queryset,
            self.patch_permissions,
        )

    def delete_documentation(self):
        return _documentation(
            self.delete,
            self.perform_delete,
            self.delete_queryset,
            self.delete_permissions,
        )

    def queryset(self):
        raise NotImplementedError()

    def get_queryset(self):
        raise NotImplementedError()

    def get_many_queryset(self):
        raise NotImplementedError()

    def get_permissions(self, queryset):
        raise NotImplementedError()

    def get_many_permissions(self, queryset):
        raise NotImplementedError()

    def post_permissions(self, payload):
        raise NotImplementedError()

    def patch_permissions(self, queryset, payload):
        raise NotImplementedError()

    def delete_permissions(self, queryset):
        raise NotImplementedError()

    def serializer_class(self) -> Schema:
        raise NotImplementedError()

    def post_serializer_class(self) -> Schema:
        return self.serializer_class()

    def patch_serializer_class(self) -> Schema:
        return self.serializer_class()

    def get_serializer_class(self) -> Schema:
        return self.serializer_class()

    def get_many_serializer_class(self) -> Schema:
        return self.serializer_class()

    def get(self, pk):
        raise NotImplementedError()

    def get_object(self, pk):
        raise NotImplementedError()

    def get_response(self, obj):
        raise NotImplementedError()

    def get_many(self):
        raise NotImplementedError()

    def get_many_objects(self):
        raise NotImplementedError()

    def get_many_response(self, objs, meta=None):
        raise NotImplementedError()

    def post(self, json=None):
        raise NotImplementedError()

    def post_payload(self, json=None):
        raise NotImplementedError()

    def post_response(self, obj):
        raise NotImplementedError()

    def perform_create(self, payload):
        raise NotImplementedError()

    def patch(self, pk, json=None):
        raise NotImplementedError()

    def patch_payload(self, json=None):
        raise NotImplementedError()

    def perform_update(self, pk, payload):
        raise NotImplementedError()

    def patch_queryset(self, pk, payload):
        raise NotImplementedError()

    def patch_response(self, obj):
        raise NotImplementedError()

    def delete(self, pk):
        raise NotImplementedError()

    def delete_response(self):
        raise NotImplementedError()

    def perform_delete(self, pk):
        raise NotImplementedError()

    def delete_queryset(self, pk):
        raise NotImplementedError()


def _documentation(*funcs):
    return '\n\n'.join(
        doc for doc in [
            func.__doc__
            for func in funcs
        ]
        if doc
    )


class IntegrityErrorHandler:
    def __init__(self, ex, *args, **kwargs):
        if self._is_unique_violation(ex):
            keys = self._parse_unique_violation(ex)
            self._raise_unique_key_violation(keys)
        elif self._is_foreign_key_violation(ex):
            keys = self._parse_foreign_key_violation(ex)
            self._raise_foreign_key_violation(keys)
        elif self._is_not_null_violation(ex):
            column_name = self._parse_not_null_violation(ex)
            self._raise_not_null_violation(column_name)
        else:
            raise ex

    def _is_unique_violation(self, ex):
        raise NotImplementedError

    def _is_foreign_key_violation(self, ex):
        raise NotImplementedError

    def _is_not_null_violation(self, ex):
        raise NotImplementedError

    def _parse_unique_violation(self, ex):
        raise NotImplementedError

    def _parse_foreign_key_violation(self, ex):
        raise NotImplementedError

    def _parse_not_null_violation(self, ex):
        raise NotImplementedError

    def _raise_foreign_key_violation(self, column_names):
        raise EntityNotFound({column_name: 'No resource with this value exists.' for column_name in column_names})

    def _raise_unique_key_violation(self, column_names):
        raise BadInput({column_name: 'Duplicates are not permitted.' for column_name in column_names})

    def _raise_not_null_violation(self, column_name):
        raise BadInput({column_name: 'Missing data for required field.'})


class NullIntegrityErrorHandler(IntegrityErrorHandler):  # noqa
    def __init__(self, ex, **kwargs):  # noqa
        raise ex


class PostgresqlIntegrityErrorHandler(IntegrityErrorHandler):
    _unique_constraint_regex = re.compile(r'Key \((.+)\)=\((.+)\) already exists.')
    _foreign_constraint_regex = re.compile(r'Key \((.+)\)=\((.+)\) is not present')

    def _is_unique_violation(self, ex):
        return psycopg2.errors.lookup(ex.orig.pgcode) is psycopg2.errors.UniqueViolation

    def _is_foreign_key_violation(self, ex):
        return psycopg2.errors.lookup(ex.orig.pgcode) is psycopg2.errors.ForeignKeyViolation

    def _is_not_null_violation(self, ex):
        return psycopg2.errors.lookup(ex.orig.pgcode) is psycopg2.errors.NotNullViolation

    def _parse_unique_violation(self, ex):
        return self._parse_postgres_violation(ex, self._unique_constraint_regex)

    def _parse_foreign_key_violation(self, ex):
        return self._parse_postgres_violation(ex, self._foreign_constraint_regex)

    def _parse_not_null_violation(self, ex):
        postgres_error = ex.orig
        return postgres_error.diag.column_name or 'unknown'

    def _parse_postgres_violation(self, ex, regex):
        keys, values = ['unknown'], []

        match = regex.match(ex.orig.diag.message_detail)
        if match:
            keys, values = match.groups()
            keys, values = keys.split(', '), values.split(', ')

        return keys


class MysqlIntegrityErrorHandler(IntegrityErrorHandler):
    """
    These regex's will work for PyMySQL and MySQL Connector Python.

    It would also work for mysqlclient, however mysqlclient raises OperationalError not IntegrityError
    """

    _null_constraint_regex = re.compile(
        r".*Column '(.+)' cannot be null"
    )

    _foreign_constraint_regex = re.compile(
        r".*Cannot add or update a child row: a foreign key constraint fails.*FOREIGN KEY \((.+)\) REFERENCES"
    )

    _unique_constraint_regex = re.compile(r".*Duplicate entry '.*' for key '[^.]+.(.+)'")

    _unique_constraint_table_and_name_regex = re.compile(
        r"Duplicate entry '.+' for key '(.+)\.(.+)'"
    )

    def _is_unique_violation(self, ex):
        return ex.orig.args[0] == 1062

    def _is_foreign_key_violation(self, ex):
        return ex.orig.args[0] == 1452

    def _is_not_null_violation(self, ex):
        return ex.orig.args[0] == 1048

    def _parse_unique_violation(self, ex):
        """
        In Mysql, a unique violation will return <table_name>.<constraint_name>, where <constraint_name>
        may be equal to the column name, if the unique constraint was created on a single column and not given a name.

        If the unique constraint is on two or more columns and not named, the constraint_name defaults to the name
        of the first column.
        """
        table_name, constraint_name = self._parse_unique_constraint_table_and_name(ex)

        if not table_name or not constraint_name:
            return ['unknown']

        return self._query_constraint_columns(table_name, constraint_name)

    def _parse_unique_constraint_table_and_name(self, ex):
        table_name = constraint_name = None

        match = self._unique_constraint_table_and_name_regex.match(ex.orig.args[1])
        if match:
            table_name, constraint_name = match.groups()

        return table_name, constraint_name

    def _query_constraint_columns(self, table_name, constraint_name):
        """
        Subclasses should override and query the database for the column names
        """
        return [constraint_name]

    def _parse_foreign_key_violation(self, ex):
        columns = ['unknown']
        match = self._foreign_constraint_regex.match(ex.orig.args[1])
        if match:
            columns = match.groups()[0]  # columns is "`foo`, `bar`"
            columns = columns.split(', ')  # columns is ["`foo`", "`bar`"]
            columns = [
                column[1:-1]  # column is "`foo`", this removes the back ticks and returns "foo"
                for column in columns
            ]
        return columns

    def _parse_not_null_violation(self, ex):
        column_name = 'unknown'

        match = self._null_constraint_regex.match(ex.orig.args[1])
        if match:
            column_name = match.groups()[0]

        return column_name


class SqliteIntegrityErrorHandler(IntegrityErrorHandler):
    _unique_constraint_regex = re.compile(r"UNIQUE constraint failed: (.+)")
    _null_constraint_regex = re.compile(r"NOT NULL constraint failed: [^.]+\.(.+)")

    def _is_unique_violation(self, ex):
        return ex.orig.args[0].startswith('UNIQUE constraint failed')

    def _is_foreign_key_violation(self, ex):
        return ex.orig.args[0].startswith('FOREIGN KEY constraint failed')

    def _is_not_null_violation(self, ex):
        return ex.orig.args[0].startswith('NOT NULL constraint failed')

    def _parse_unique_violation(self, ex):
        keys = ['unknown']
        match = self._unique_constraint_regex.match(ex.orig.args[0])
        if match:
            keys = match.groups()[0]  # keys is 'foo.c1, foo.c2'
            keys = keys.split(', ')  # keys is ['foo.c1', 'foo.c2']
            keys = [
                key[key.find('.') + 1:]  # key is 'c1'
                for key in keys
            ]

        return keys

    def _parse_foreign_key_violation(self, ex):

        return ['unknown']  # sqlite doesn't provide the foreign key column

    def _parse_not_null_violation(self, ex):
        column_name = 'unknown'

        match = self._null_constraint_regex.match(ex.orig.args[0])
        if match:
            column_name = match.groups()[0]

        return column_name


class OracleIntegrityErrorHandler(IntegrityErrorHandler):
    _null_constraint_regex = re.compile(r"""ORA-01400: cannot insert NULL into \([^.]+\.[^.]+\."(.+)"\)""")
    _unique_constraint_name_regex = re.compile(r"ORA-00001: unique constraint \([^.]+\.(.+)\) violated")
    _foreign_key_constraint_name_regex = re.compile(
        r"ORA-02291: integrity constraint \([^.]+\.(.+)\) violated - parent key not found"
    )

    def _is_unique_violation(self, ex):
        error = ex.orig.args[0]
        return error.code == 1

    def _is_foreign_key_violation(self, ex):
        error = ex.orig.args[0]
        return error.code == 2291

    def _is_not_null_violation(self, ex):
        error = ex.orig.args[0]
        return error.code == 1400

    def _parse_unique_violation(self, ex):
        error = ex.orig.args[0]
        unique_constraint_name = self._parse_unique_constraint_name(error.message)
        return self._query_constraint_columns(unique_constraint_name)

    def _parse_unique_constraint_name(self, message):
        return self._parse_constraint_name(self._unique_constraint_name_regex, message)

    def _parse_foreign_key_constraint_name(self, message):
        return self._parse_constraint_name(self._foreign_key_constraint_name_regex, message)

    def _parse_constraint_name(self, regex, message):
        match = regex.match(message)
        if match:
            return match.groups()[0]

    def _parse_foreign_key_violation(self, ex):
        error = ex.orig.args[0]
        foreign_key_contraint_name = self._parse_foreign_key_constraint_name(error.message)
        return self._query_constraint_columns(foreign_key_contraint_name)

    def _parse_not_null_violation(self, ex):
        error = ex.orig.args[0]

        column_name = 'unknown'

        match = self._null_constraint_regex.match(error.message)
        if match:
            column_name = match.groups()[0].lower()

        return column_name

    def _query_constraint_columns(self, constraint_name):
        """
        Subclasses should override this and query the database for the columns
        """
        return [constraint_name]


class _FileApi(BaseApi):
    """
    API for uploading Binary files to the Database.

    Set `file_column_name` equal to the column name of the table that will store the binary file
    """

    apis = {'GET', 'PATCH', 'DELETE'}

    def serializer_class(self):
        """
        Subclasses should not override this.

        We do not use json serialization/deserialization here, instead using raw bytes.
        """
        return None

    def queryset(self):
        return None

    # The name of the column that contains the binary file
    file_column_name = None

    def get_response(self, obj):
        if getattr(obj, self.file_column_name) is None:
            raise EntityNotFound()

        response = Response(getattr(obj, self.file_column_name))

        return response, 200

    def patch_payload(self, json=None):
        return {
            self.file_column_name: request.get_data()
        }

    def delete(self, pk):
        self.perform_update(pk, {self.file_column_name: None})

        return jsonify({}), 200

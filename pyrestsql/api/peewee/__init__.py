import peewee
from flask import request, jsonify
from pyrestsql.api import (BaseApi, PostgresqlIntegrityErrorHandler, MysqlIntegrityErrorHandler,
                              SqliteIntegrityErrorHandler, NullIntegrityErrorHandler, _FileApi, ApiMetaClass,
                              ErrorHandler, IntegrityErrorManager, )
from pyrestsql.exc import AuthorizationError, EntityNotFound
from pyrestsql.api.peewee.filters import FilterSet
from pyrestsql.api.peewee.pagination import Pagination
from marshmallow.schema import Schema
from peewee import Select, callable_, Insert, PostgresqlDatabase, SqliteDatabase, MySQLDatabase
import logging

logger = logging.getLogger(__name__)


def _primary_key_field(model):
    return model._meta.primary_key


def insert_where(model, where=None, **kwargs):
    kwargs = _populate_insert_defaults(model, kwargs)

    fields, values = zip(*kwargs.items())

    ins = model.insert_from(
        Select(columns=values).where(
            where
        ),
        fields
    )

    return ins


def _populate_insert_defaults(model, insert):
    for field, default in model._meta.defaults.items():
        if field.name not in insert:
            val = default
            if callable_(val):
                val = val()
            insert[field.name] = val

    return insert


def execute_insert(db, query):
    if _is_returning_supported(db):
        return _execute_returning_dml(query)

    return _execute_nonreturning_insert(db, query)


def _is_returning_supported(db):
    """
    Note that sqlite actually supports RETURNING starting in 3.35.0. However, in later versions some bugs were added.
    For now this will assume sqlite cannot support RETURNING.
    """
    if isinstance(db, PostgresqlDatabase):
        return True

    if isinstance(db, MySQLDatabase):
        conn = db.connection()
        if hasattr(conn, 'get_server_info') and 'MariaDB' in conn.get_server_info():
            return True

    return False


def _execute_returning_dml(query):
    query = query.returning(query.model)
    rows = list(query.execute())
    if not rows:
        return None

    # TODO what about case where user executes a bulk update/insert/delete?
    return rows[0]


def _execute_nonreturning_insert(db, query):
    cursor = db.execute(query)

    primary_key_value = _inserted_primary_key_value(query) or cursor.lastrowid

    obj = query.model.get(primary_key_value)

    return obj


def _inserted_primary_key_value(query):
    """
    Given an INSERT ... FROM SELECT, returns the value of the primary key if included.
    Normally, the primary key column is not included in the INSERT for serial generated primary keys.
    In this case, this function returns None

    """
    insert_values = {field: value for field, value in zip(query._columns, query._insert._returning)}

    primary_key_field = _primary_key_field(query.model)

    primary_key_value = insert_values.get(primary_key_field)

    return primary_key_value


def execute_update(db, query):
    if _is_returning_supported(db):
        return _execute_returning_dml(query)

    return _execute_nonreturning_update(query)


def _execute_nonreturning_update(query):
    is_updated = query.execute()

    if not is_updated:
        return

    pk_field, pk_value = _updated_primary_key(query)

    sel = query.model.select().where(
        pk_field == pk_value
    )

    obj = list(sel.execute())[0]

    return obj


def _updated_primary_key(query):
    """
    Return the (primary_key_field, primary_key_value) given an update statement

    ```
    upd = Foo.update(bar=1).where(Foo.id == 1, Foo.baz == 2, fn.exists(...))
    assert _updated_primary_key(upd) == Foo.id, 1
    ```
    """
    stack = [(query._where.lhs, query._where.rhs)]

    while stack:
        left, right = stack.pop()

        if isinstance(left, peewee.Expression):
            stack.append((right.lhs, right.rhs))
            stack.append((left.lhs, left.rhs))
            continue

        if not isinstance(left, peewee.Field):
            left, right = right, left

        if not isinstance(left, peewee.Field):
            continue

        if not left.primary_key:
            continue

        return left, right

    return None, None


class PeeweeApiMetaClass(ApiMetaClass):
    def add_missing_model_or_queryset(cls):
        if cls.url_prefix is None:
            return

        obj = cls()

        if cls.model is None:
            cls.model = obj.queryset().model

        if obj.queryset() is None:
            cls.queryset = lambda self: cls.model.select()

    def ensure_filterset_class(cls):
        if cls.filterset_class is None:
            cls.filterset_class = FilterSet

    def generate_filterset_instance(cls):
        obj = cls()
        cls.filterset = cls.filterset_class(cls.filterset_fields, obj.get_many_queryset())

    def wrap_perform_create_for_integrity_errors(cls):
        def post_decorator(func):
            def _post_decorator(self, payload):
                try:
                    return func(self, payload)
                except peewee.IntegrityError as ex:
                    self.integrity_error_manager.handle(ex, payload=payload)

            return _post_decorator

        cls.perform_create = post_decorator(cls.perform_create)

    def wrap_perform_update_for_integrity_errors(cls):
        def patch_decorator(func):
            def _patch_decorator(self, pk, payload):
                try:
                    return func(self, pk, payload)
                except peewee.IntegrityError as ex:
                    self.integrity_error_manager.handle(ex, payload=payload)

            return _patch_decorator

        cls.perform_update = patch_decorator(cls.perform_update)


class PeeweeIntegrityErrorManager(IntegrityErrorManager):
    def __init__(self, db=None, model=None, **kwargs):
        self.db = db
        self.model = model
        super().__init__(**kwargs)

    def determine_handler(self):
        if self.integrity_error_handler is not None:
            return

        self.integrity_error_handler = NullIntegrityErrorHandler

        if isinstance(self.db, peewee.PostgresqlDatabase):
            self.integrity_error_handler = PostgresqlIntegrityErrorHandler

        if isinstance(self.db, peewee.MySQLDatabase):
            self.integrity_error_handler = PeeweeMysqlIntegrityErrorHandler

        if isinstance(self.db, peewee.SqliteDatabase):
            self.integrity_error_handler = PeeweeSqliteIntegrityErrorHandler

    def handle(self, ex, payload=None):
        self.integrity_error_handler(ex, db=self.db, model=self.model, payload=payload)


class Api(BaseApi, metaclass=PeeweeApiMetaClass):
    url_prefix = None

    model = None

    apis = ['GET', 'GET_MANY', 'POST', 'PATCH', 'DELETE']

    pagination = Pagination()

    filterset_class = FilterSet
    filterset_fields = None
    filterset = None

    blueprint = None

    error_handler_class = ErrorHandler
    integrity_error_manager_class = PeeweeIntegrityErrorManager

    db = None

    def __init__(self, api=None):
        super().__init__(api)

    @classmethod
    def register_app(cls, app, db):
        cls.db = db
        integrity_error_manager = cls.integrity_error_manager_class(db=db, model=cls.model)
        return super().register_app(app, integrity_error_manager=integrity_error_manager)

    def queryset(self) -> peewee.Select:
        if self.model is not None:
            return self.model.select()

        return None

    def _primary_key_field(self):
        return _primary_key_field(self.model)

    def get_queryset(self) -> peewee.Select:
        return self.queryset()

    def get_many_queryset(self) -> peewee.Select:
        return self.queryset()

    def get_permissions(self, queryset) -> peewee.Select:
        return queryset

    def get_many_permissions(self, queryset) -> peewee.Select:
        return self.get_permissions(queryset)

    def post_permissions(self, payload):
        return None

    def patch_permissions(self, queryset, payload) -> peewee.Select:
        return self.get_permissions(queryset)

    def delete_permissions(self, queryset):
        return self.get_permissions(queryset)

    def serializer_class(self) -> Schema:
        raise NotImplementedError

    def post_serializer_class(self) -> Schema:
        return self.serializer_class()

    def patch_serializer_class(self) -> Schema:
        return self.serializer_class()

    def get_serializer_class(self) -> Schema:
        return self.serializer_class()

    def get_many_serializer_class(self) -> Schema:
        return self.serializer_class()

    def get(self, pk):
        obj = self.get_object(pk)

        return self.get_response(obj)

    def get_object(self, pk):
        query = self.get_permissions(self.get_queryset())

        query = query.where(
            self._primary_key_field() == pk
        )

        with self.db:
            if (obj := query.get_or_none()) is None:
                raise EntityNotFound()

        return obj

    def get_response(self, obj):
        serializer = self._ensure_schema(self.get_serializer_class)
        return jsonify(serializer.dump(obj)), 200

    def get_many(self):
        objs, meta = self.get_many_objects()

        return self.get_many_response(objs, meta)

    def get_many_objects(self):
        query = self.get_many_permissions(self.get_many_queryset())

        query = self.filterset.apply_filters(query, request.args)

        query, meta = self.pagination.paginate(query)

        with self.db:
            objs = list(query)

        self.pagination.add_count_meta(objs, meta)

        return objs, meta

    def get_many_response(self, objs, meta=None):
        serializer = self._ensure_schema(self.get_many_serializer_class)

        meta = meta or {}

        results = {
            'items': serializer.dump(objs, many=True),
            **meta
        }

        return jsonify(results), 200

    def post(self, json=None):
        payload = self.post_payload(json)

        obj = self.perform_create(payload)

        return self.post_response(obj)

    def post_payload(self, json=None):
        json = json or request.json

        serializer = self._ensure_schema(self.post_serializer_class)

        payload = serializer.load(json)

        return payload

    def post_response(self, obj):
        serializer = self._ensure_schema(self.post_serializer_class)

        obj = serializer.dump(obj)

        return jsonify(obj), 201

    def perform_create(self, payload):
        with self.db:
            query = insert_where(
                self.model,
                **payload,
                where=self.post_permissions(payload)
            )

            obj = execute_insert(self.db, query)

            if obj is None:
                raise AuthorizationError()

        return obj

    def patch(self, pk, json=None):
        if not (payload := self.patch_payload(json)):
            return jsonify({}), 200

        obj = self.perform_update(pk, payload)

        return self.patch_response(obj)

    def patch_payload(self, json=None):
        json = json or request.json

        serializer = self._ensure_schema(self.patch_serializer_class)

        return serializer.load(json, partial=True)

    def perform_update(self, pk, payload):
        query = self.patch_queryset(pk, payload)

        query = self.patch_permissions(query, payload)

        with self.db:
            obj = execute_update(self.db, query)

            if not obj:
                raise AuthorizationError()

        return obj

    def patch_queryset(self, pk, payload):
        model = self.model

        query = model.update(
            **payload
        ).where(
            self._primary_key_field() == pk
        )

        return query

    def patch_response(self, obj):
        serializer = self._ensure_schema(self.patch_serializer_class)

        obj = serializer.dump(obj)

        return jsonify(obj), 200

    def delete(self, pk):
        self.perform_delete(pk)

        return self.delete_response()

    def delete_response(self):
        return jsonify({}), 200

    def perform_delete(self, pk):
        query = self.delete_permissions(self.delete_queryset(pk))

        with self.db:
            is_deleted = query.execute()

        if not is_deleted:
            raise AuthorizationError()

    def delete_queryset(self, pk):
        model = self.queryset().model

        query = model.delete().where(
            self._primary_key_field() == pk
        )

        return query


class PeeweeMysqlIntegrityErrorHandler(MysqlIntegrityErrorHandler):
    def __init__(self, ex, db, **kwargs):
        self.db = db
        super().__init__(ex)

    def _query_constraint_columns(self, table_name, constraint_name):
        with self.db:
            res = self.db.execute_sql(
                '''
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE 1=1
                        AND table_schema = database()
                        AND table_name = %s
                        AND constraint_name = %s
                    ORDER BY ordinal_position
                ''', [
                    table_name,
                    constraint_name,
                ]
            )

            return [
                row[0]
                for row
                in res.fetchall()
            ]


class PeeweeSqliteIntegrityErrorHandler(SqliteIntegrityErrorHandler):
    def __init__(self, ex, db, model=None, payload=None, **kwargs):
        self.model = model
        self.payload = payload or {}
        super().__init__(ex)

    def _parse_foreign_key_violation(self, ex):
        """
        If the ORM model has only one foreign key, this will assume that column was at fault and return it.

        In sqlite, foreign exceptions do not report the constraint or the column that caused the error, so we can
        never know precisely which column was responsible.

        This is a reasonable guess that should work in most cases.
        """
        if self.model is None:
            return ['unknown']

        foreign_key_column_names = [
            getattr(self.model, key).column.name
            for key in self.payload
            if (
                   hasattr(self.model, key)
                   and isinstance(getattr(self.model, key), peewee.ForeignKeyField)
            )
        ]

        if len(foreign_key_column_names) == 1:
            return foreign_key_column_names

        return ['unknown']

class FileApi(Api, _FileApi):
    pass
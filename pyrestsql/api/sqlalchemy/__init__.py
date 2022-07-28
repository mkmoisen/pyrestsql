import re

from flask import jsonify, request
from pyrestsql.api import (BaseApi, PostgresqlIntegrityErrorHandler, MysqlIntegrityErrorHandler,
                              SqliteIntegrityErrorHandler, OracleIntegrityErrorHandler, NullIntegrityErrorHandler,
                              _FileApi, ApiMetaClass, IntegrityErrorManager, ErrorHandler, )
from pyrestsql.exc import AuthorizationError, EntityNotFound
from pyrestsql.api.sqlalchemy.filters import FilterSet
from pyrestsql.api.sqlalchemy.pagination import Pagination
from sqlalchemy import select, insert, update, delete, text, literal, bindparam, Column
from sqlalchemy.dialects import oracle
from sqlalchemy.exc import IntegrityError

from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.sql.elements import BindParameter, BooleanClauseList

try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None


def _primary_key_field(model):
    if hasattr(model, '__table__'):
        field = model.__table__.primary_key.columns[0]
        return getattr(model, field.name)

    return model.primary_key.columns[0]


def insert_where(model, from_=None, where=None, **kwargs):
    kwargs = _sqlalchemy_insert_values_workaround(model, **kwargs)

    if from_ is None:
        sel = select(
            *[bindparam(key, value) for key, value in kwargs.items()]
        )
    else:
        sel = select(*kwargs.values())

    if where:
        sel = sel.where(where)

    ins = insert(model).from_select(
        kwargs.keys(),
        sel
    )

    return ins


def _sqlalchemy_insert_values_workaround(model, **kwargs):
    """
    Map the ORM field names to the Database column names in order to account for SQLAlchemy 1.4 issue
    where insert(model) doesn't automatically do this.

    update(model) is fine, it doesn't need this.
    """
    return {
        getattr(model, orm_field_name).name: value
        for orm_field_name, value in kwargs.items()
    }


def execute_insert(session, query):
    dialect = session.bind.dialect
    is_postgres = isinstance(dialect, PGDialect)
    is_oracle = isinstance(dialect, OracleDialect)

    if is_postgres:
        return _execute_postgres_dml(session, query)

    if is_oracle:
        return _execute_insert_where_oracle(session, query)

    return _execute_insert_where_lastrowid(session, query)


def _execute_postgres_dml(session, query):
    query = _sqlalchemy_returning_work_around(query)

    res = session.execute(query)

    row = res.scalars().one_or_none()
    # TODO multi update/insert will fail if you call one_or_none
    # TODO could instead call fetchall()[0]
    return row


def _model_from_table(table):
    return select(table).column_descriptions[0]['type']


def _sqlalchemy_returning_work_around(query):
    """
    Using query.returning(Model) will return a Core Row, not an ORM model

    This will execute the identical update/insert query, but will return an ORM model.
    It also handles the case where the ORM model uses column_property()/different column names in ORM vs DB.
    """
    model = _model_from_table(query.table)

    query = query.returning(*select(model).selected_columns)

    query = select(model).from_statement(query)

    return query


def _execute_insert_where_lastrowid(session, query):
    res = session.execute(query)

    if not res.rowcount:
        return None

    pk = res.lastrowid
    # TODO for multi insert this res.lastrowid returns the last pk, different behavior than postgres

    primary_key_column = _primary_key_field(query.table)

    pk = next(
        (
            c.value
            for c in query.select.selected_columns
            if (
                isinstance(c, BindParameter)
                and
                c.key == primary_key_column.name
        )
        ), pk)

    obj = session.execute(
        select(query.table).where(
            _primary_key_field(query.table) == pk
        )
    ).scalars().one()

    return obj


def _execute_insert_where_oracle(session, query):
    sequence_value_or_rowcount = _oralce_insert_plsql_block(session, query)

    if not sequence_value_or_rowcount:
        return None

    pk = _oracle_inserted_primary_key_value(query, sequence_value_or_rowcount)

    obj = session.execute(
        select(query.table).where(
            _primary_key_field(query.table) == pk
        )
    ).scalars().one()

    return obj


def _oracle_inserted_primary_key_value(query, sequence_value_or_rowcount):
    primary_key_column = _primary_key_field(query.table)
    sequence_name = primary_key_column.default and primary_key_column.default.name

    if sequence_name:
        return sequence_value_or_rowcount

    return next(
        (
            c.value
            for c in query.select.selected_columns
            if (
                isinstance(c, BindParameter)
                and
                c.key == primary_key_column.name
        )
        ), None)


def _oralce_insert_plsql_block(session, query):
    """
    Return the primary key value (or the rowcount if not using sequence) for Oracle INSERT ... SELECT.

    This is an ugly workaround, but it works.
    Oracle does not support returning of the primary key for INSERT ... SELECT in normal SQL.
    You can however use an anonymous PLSQL block with bind variables.
    :return:
    """
    _check_cx_Oracle()
    conn = session.connection().connection
    cur = conn.cursor()
    sequence_value_or_rowcount = cur.var(cx_Oracle.NUMBER)

    sql = query.compile(dialect=oracle.dialect(), compile_kwargs={'literal_binds': True})

    primary_key_column = _primary_key_field(query.table)
    sequence_name = primary_key_column.default and primary_key_column.default.name

    row_count_expression = 'SQL%ROWCOUNT'
    if sequence_name:
        row_count_expression = f'{sequence_name}.currval'

    plsql = f'''
            BEGIN
                {sql};
                
                IF SQL%ROWCOUNT > 0 THEN
                    :sequence_value_or_rowcount := {row_count_expression};
                ELSE
                    :sequence_value_or_rowcount := 0;
                END IF;
                
            END;
        '''

    session.execute(plsql, {'sequence_value_or_rowcount': sequence_value_or_rowcount})

    return sequence_value_or_rowcount.getvalue()


def _check_cx_Oracle():
    if not cx_Oracle:
        raise ImportError("No module named 'cx_Oracle'")



def execute_update(session, query):
    dialect = session.bind.dialect
    is_postgres = isinstance(dialect, PGDialect)

    if is_postgres:
        return _execute_postgres_dml(session, query)

    return _non_postgres_execute_update(session, query)


def _non_postgres_execute_update(session, query):
    is_updated = session.execute(query).rowcount

    if not is_updated:
        return

    pk_field, pk_value = _updated_primary_key(query)

    # TODO, this assumes update is on a single row with an identifiable pk
    # TODO what if user in simple api is passing in multi row update?
    # TODO this shouldn't fail

    obj = session.execute(
        select(query.table).where(
            pk_field == pk_value
        )
    ).scalars().one()

    return obj


def _updated_primary_key(query):
    if isinstance(query.whereclause, BooleanClauseList):
        left, right = _updated_primary_key_from_boolean_clause_list(query.whereclause.clauses)
    else:
        left, right = _updated_primary_key_from_binary_expression(query.whereclause)

    return left, right


def _updated_primary_key_from_boolean_clause_list(clauses):
    for clause in clauses:
        left, right = _updated_primary_key_from_binary_expression(clause)
        if left is not None:
            return left, right

    return None, None


def _updated_primary_key_from_binary_expression(expression):
    left, right = expression.get_children()

    if not isinstance(left, Column):
        left, right = right, left

    if not isinstance(left, Column):
        return None, None

    if left.primary_key:
        return left, right

    return None, None


class SqlAlchemyApiMetaClass(ApiMetaClass):
    def add_missing_model_or_queryset(cls):
        if cls.url_prefix is None:
            return

        obj = cls()

        if cls.model is None:
            cls.model = obj.queryset().column_descriptions[0]['type']

        if obj.queryset() is None:
            cls.queryset = lambda self: select(cls.model)

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
                except IntegrityError as ex:
                    self.integrity_error_manager.handle(ex, payload=payload)

            return _post_decorator

        cls.perform_create = post_decorator(cls.perform_create)

    def wrap_perform_update_for_integrity_errors(cls):
        def patch_decorator(func):
            def _patch_decorator(self, pk, payload):
                try:
                    return func(self, pk, payload)
                except IntegrityError as ex:
                    self.integrity_error_manager.handle(ex, payload=payload)

            return _patch_decorator

        cls.perform_update = patch_decorator(cls.perform_update)


class SqlAlchemyIntegrityErrorManager(IntegrityErrorManager):
    def __init__(self, Session=None, model=None, **kwargs):
        self.Session = Session
        self.model = model
        super().__init__(**kwargs)

    def determine_handler(self):
        if self.integrity_error_handler is not None:
            return

        dialect = self.Session.kw['bind'].dialect

        self.integrity_error_handler = NullIntegrityErrorHandler

        if isinstance(dialect, PGDialect):
            self.integrity_error_handler = PostgresqlIntegrityErrorHandler

        if isinstance(dialect, MySQLDialect):
            self.integrity_error_handler = SQLAlchemyMysqlIntegrityErrorHandler

        if isinstance(dialect, SQLiteDialect):
            self.integrity_error_handler = SqlalchemySqliteIntegrityErrorHandler

        if isinstance(dialect, OracleDialect):
            self.integrity_error_handler = SQLAlchemyOracleIntegrityErrorHandler

    def handle(self, ex, payload=None):
        self.integrity_error_handler(ex, Session=self.Session, model=self.model, payload=payload)


class SqlAlchemyApi(BaseApi, metaclass=SqlAlchemyApiMetaClass):
    url_prefix = None

    model = None

    apis = ['GET', 'GET_MANY', 'POST', 'PATCH', 'DELETE']

    pagination = Pagination()

    filterset_class = FilterSet
    filterset_fields = None
    filterset = None

    error_handler_class = ErrorHandler
    integrity_error_manager_class = SqlAlchemyIntegrityErrorManager

    blueprint = None
    Session = None

    def __init__(self, api=None):
        super().__init__(api)

    @classmethod
    def register_app(cls, app, Session):
        cls.Session = Session
        integrity_error_manager = cls.integrity_error_manager_class(Session=Session, model=cls.model)
        super().register_app(app, integrity_error_manager=integrity_error_manager)

    def queryset(self):
        if self.model is not None:
            return select(self.model)

        return None

    def _primary_key_field(self):
        return _primary_key_field(self.model)

    def get_queryset(self):
        return self.queryset()

    def get_many_queryset(self):
        return self.queryset()

    def get_permissions(self, queryset):
        return queryset

    def get_many_permissions(self, queryset):
        return self.get_permissions(queryset)

    def post_permissions(self, payload):
        return None

    def patch_permissions(self, queryset, payload):
        return self.get_permissions(queryset)

    def delete_permissions(self, queryset):
        return self.get_permissions(queryset)

    def serializer_class(self):
        raise NotImplementedError

    def post_serializer_class(self):
        return self.serializer_class()

    def patch_serializer_class(self):
        return self.serializer_class()

    def get_serializer_class(self):
        return self.serializer_class()

    def get_many_serializer_class(self):
        return self.serializer_class()

    def get(self, pk):
        obj = self.get_object(pk)

        return self.get_response(obj)

    def get_object(self, pk):
        query = self.get_permissions(self.get_queryset())

        primary_key_field = self._primary_key_field()

        query = query.where(
            primary_key_field == pk
        )

        with self.Session(expire_on_commit=False) as session:
            if (obj := session.execute(query).scalars().one_or_none()) is None:
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

        query = self.filterset.apply_filters(request.args, query)

        query, meta = self.pagination.paginate(query)

        with self.Session(expire_on_commit=False) as session:
            objs = session.execute(query).scalars().fetchall()

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
        with self.Session(expire_on_commit=False) as session:
            query = insert_where(
                self.model,
                **payload,
                where=self.post_permissions(payload)
            )

            obj = execute_insert(session, query)

            if obj is None:
                raise AuthorizationError()

            session.commit()

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

        with self.Session(expire_on_commit=False) as session:
            obj = execute_update(session, query)

        if not obj:
            raise AuthorizationError()

        return obj

    def patch_queryset(self, pk, payload):
        model = self.model

        primary_key_field = self._primary_key_field()

        query = update(model).where(
            primary_key_field == pk
        ).values(
            **payload
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

        with self.Session() as session:
            is_deleted = session.execute(query).rowcount

        if not is_deleted:
            raise AuthorizationError()

    def delete_queryset(self, pk):
        primary_key_field = self._primary_key_field()

        query = delete(self.model).where(
            primary_key_field == pk
        )

        return query


class SQLAlchemyMysqlIntegrityErrorHandler(MysqlIntegrityErrorHandler):
    def __init__(self, ex, Session, **kwargs):
        self.Session = Session
        super().__init__(ex)

    def _query_constraint_columns(self, table_name, constraint_name):
        with self.Session() as session:
            res = session.execute(
                text('''
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE 1=1
                        AND table_schema = database()
                        AND table_name = :table_name
                        AND constraint_name = :constraint_name
                    ORDER BY ordinal_position
                '''), {
                    'table_name': table_name,
                    'constraint_name': constraint_name
                }
            )

            return [
                row.COLUMN_NAME
                for row
                in res.fetchall()
            ]


class SQLAlchemyOracleIntegrityErrorHandler(OracleIntegrityErrorHandler):
    def __init__(self, ex, Session, **kwargs):
        self.Session = Session
        super().__init__(ex)

    def _query_constraint_columns(self, constraint_name):
        with self.Session() as session:
            result = session.execute(
                text('''
                    WITH constraint_columns AS (
                        SELECT column_name, position
                        FROM user_cons_columns
                        WHERE 1=1
                            AND constraint_name = :constraint_name
                    )
                    SELECT column_name, position 
                    FROM constraint_columns
                    UNION 
                    SELECT column_name, column_position position
                        FROM user_ind_columns
                        WHERE 1=1
                            AND index_name = :constraint_name
                            AND NOT EXISTS (
                                SELECT 1
                                FROM constraint_columns
                            )
                    ORDER BY position
                '''),
                {'constraint_name': constraint_name.upper()}
            )

        return [
            row.column_name.lower()
            for row
            in result.fetchall()
        ]


class SqlalchemySqliteIntegrityErrorHandler(SqliteIntegrityErrorHandler):
    def __init__(self, ex, Session, model=None, payload=None, **kwargs):
        self.model = model
        self.payload = payload
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
            getattr(self.model, key).name
            for key in self.payload
            if (
                hasattr(self.model, key)
                and getattr(self.model, key).foreign_keys
            )
        ]

        if len(foreign_key_column_names) == 1:
            return foreign_key_column_names

        return ['unknown']



class FileApi(SqlAlchemyApi, _FileApi):
    pass
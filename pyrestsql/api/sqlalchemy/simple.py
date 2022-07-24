from flask import request
from pyrestsql.exc import EntityNotFound, AuthorizationError
from pyrestsql.api.simple_api import SimpleApi
from pyrestsql.api.sqlalchemy import execute_insert, execute_update, SqlAlchemyIntegrityErrorManager, FilterSet
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.sql import Select, Update, Delete, Insert


class SimpleModelApi(SimpleApi):
    integrity_error_manager_class = SqlAlchemyIntegrityErrorManager

    def __init__(self, url_prefix):
        super().__init__(url_prefix)
        self.Session = None
        self.integrity_error_manager = None

    def register_app(self, app, Session=None, integrity_error_manager=None, **kwargs):
        super().register_app(app, **kwargs)
        self.Session = Session
        self.integrity_error_manager = integrity_error_manager or self.integrity_error_manager_class(Session=self.Session)

    def exception(self, ex):
        if isinstance(ex, IntegrityError):
            self.integrity_error_manager.handle(ex)
        elif isinstance(ex, NoResultFound):
            raise EntityNotFound()

        super().exception(ex)

    def get_response(self, obj, schema):
        if isinstance(obj, Select):
            query = obj

            with self.Session(expire_on_commit=False) as session:
                obj = session.execute(query).scalars().one_or_none()

            if not obj:
                raise EntityNotFound()

        return super().get_response(obj, schema)

    def get_many_response(self, objs, schema, filterset_fields=None):
        if isinstance(objs, Select):
            query = objs

            if filterset_fields:
                query = FilterSet(
                    filterset_fields,
                    query
                ).apply_filters(query, request.args)

            with self.Session(expire_on_commit=False) as session:
                objs = session.execute(query).scalars().fetchall()

        return super().get_response(objs, schema)

    def post_response(self, obj, schema):
        if isinstance(obj, Insert):
            query = obj

            with self.Session(expire_on_commit=False) as session:
                obj = execute_insert(session, query)

                if not obj:
                    raise AuthorizationError()

                session.commit()

        # TODO how to check if obj is a sqlalchemy model

        return super().post_response(obj, schema)

    def patch_response(self, obj, schema):
        if isinstance(obj, Update):
            query = obj

            with self.Session(expire_on_commit=False) as session:
                obj = execute_update(session, query)

                if not obj:
                    raise AuthorizationError()

                session.commit()

            obj = schema.dump(obj)

        return super().patch_response(obj, schema)

    def delete_response(self, obj):
        if isinstance(obj, Delete):
            query = obj

            with self.Session(expire_on_commit=False) as session:
                is_deleted = session.execute(query).rowcount

                if not is_deleted:
                    raise AuthorizationError()

                session.commit()

            obj = {}

        return super().delete_response(obj)

import peewee
from flask import request
from pyrestsql.admin.admin_model import PeeweeAdminModel
from pyrestsql.api.peewee import execute_insert, execute_update, PeeweeIntegrityErrorManager, FilterSet
from pyrestsql.exc import EntityNotFound, AuthorizationError
from pyrestsql.api.simple_api import SimpleApi
from peewee import ModelSelect, ModelUpdate, ModelDelete, ModelInsert
from pyrestsql.swagger.swagger_model import SwaggerSimpleConverter


class AdminConverter:
    admin_model_class = PeeweeAdminModel

    def __init__(self, simple_api, **kwargs):
        self.simple_api = simple_api

    def admin(self, model, name=None, **kwargs):
        name = name or model.__name__

        return self.admin_model_class(
            model=model,
            name=name,
            get_many_serializer_class=self.simple_api.get_many_serializer_class(),
            get_serializer_class=self.simple_api.get_serializer_class,
            post_serializer_class=self.simple_api.post_serializer_class,
            patch_serializer_class=self.simple_api.patch_serializer_class,
            get_many_url_for=self._url_for('GET_MANY'),
            get_url_for=self._url_for('GET'),
            post_url_for=self._url_for('POST'),
            patch_url_for=self._url_for('PATCH'),
            delete_url_for=self._url_for('DELETE'),
            db=self.simple_api.db,
        )

    def _function_name(self, api):
        functions = list(self.simple_api.apis[api].values())
        if not functions:
            return None

        return functions[0].__name__

    def _url_for(self, api):
        function_name = self._function_name(api)
        if not function_name:
            return None

        blue_print_name = self.simple_api.blueprint.name

        return f'{blue_print_name}.{function_name}'


class SimpleModelApi(SimpleApi):
    integrity_error_manager_class = PeeweeIntegrityErrorManager
    admin_converter_class = AdminConverter
    swagger_converter_class = SwaggerSimpleConverter
    filterset_class = FilterSet

    def __init__(self, url_prefix, default_schema=None):
        super().__init__(url_prefix, default_schema)
        self.db = None
        self.integrity_error_manager = None

    def admin(self, model, name=None, admin_converter_class=None, **kwargs):
        admin_converter_class = admin_converter_class or self.admin_converter_class
        return admin_converter_class(self).admin(model, name, **kwargs)

    def swagger(self, name, swagger_converter_class=None, **kwargs):
        swagger_converter_class = swagger_converter_class or self.swagger_converter_class
        return swagger_converter_class(self).swagger(name, **kwargs)

    def register_app(self, app, db=None, integrity_error_manager=None, **kwargs):
        super().register_app(app, **kwargs)
        self.db = db
        self.integrity_error_manager = integrity_error_manager or self.integrity_error_manager_class(db=self.db)

    def exception(self, ex):
        if isinstance(ex, peewee.IntegrityError):
            self.integrity_error_manager.handle(ex)
        elif isinstance(ex, peewee.DoesNotExist):
            raise EntityNotFound()

        super().exception(ex)

    def get_response(self, obj, schema):
        if isinstance(obj, ModelSelect):
            query = obj

            with self.db:
                obj = query.get_or_none()

            if not obj:
                raise EntityNotFound()

        if isinstance(obj, peewee.Model):
            obj = schema.dump(obj)

        return super().get_response(obj, schema)

    def get_many_response(self, objs, schema, filterset_fields=None):
        if isinstance(objs, ModelSelect):
            query = objs

            if filterset_fields:
                query = self.filterset_class(filterset_fields, query)(request.args)

            with self.db:
                objs = list(query)

        return super().get_many_response(objs, schema)

    def post_response(self, obj, schema):
        if isinstance(obj, ModelInsert):
            query = obj

            with self.db:
                obj = execute_insert(self.db, query)

            if not obj:
                raise AuthorizationError()

        elif isinstance(obj, ModelSelect):
            query = obj

            with self.db:
                obj = query.get_or_none()

            if not obj:
                raise AuthorizationError()

        if isinstance(obj, peewee.Model):
            obj = schema.dump(obj)

        return super().post_response(obj, schema)

    def patch_response(self, obj, schema):
        if isinstance(obj, ModelUpdate):
            query = obj

            with self.db:
                obj = execute_update(self.db, query)

                if not obj:
                    raise AuthorizationError()

        if isinstance(obj, peewee.Model):
            obj = schema.dump(obj)

        return super().patch_response(obj, schema)

    def delete_response(self, obj):
        if isinstance(obj, ModelDelete):
            query = obj

            with self.db:
                is_deleted = query.execute()

                if not is_deleted:
                    raise AuthorizationError()

            obj = {}

        return super().delete_response(obj)

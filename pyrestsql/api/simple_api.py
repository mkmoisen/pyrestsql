from flask import Blueprint, jsonify, Response, request
from collections import defaultdict
import functools

from pyrestsql.api import ErrorHandler
from marshmallow.schema import SchemaMeta


class SimpleApi:
    error_handler_class = ErrorHandler

    def __init__(self, url_prefix, default_schema=None):
        self.url_prefix = url_prefix
        if default_schema is not None:
            default_schema = self.ensure_schema(default_schema)
        self.default_schema = default_schema
        self.apis = defaultdict(dict)
        self.blueprint = None
        self.error_handler = self.error_handler_class()
        self.integrity_error_manager = None
        self.get_serializer_class = lambda: default_schema
        self.get_many_serializer_class = lambda: default_schema
        self.patch_serializer_class = lambda: default_schema
        self.post_serializer_class = lambda: default_schema


    def register_app(self, app, error_handler=None, **kwargs):
        blueprint = Blueprint(self.url_prefix, __name__)
        self.blueprint = blueprint

        for api, urls in self.apis.items():
            for url, func in urls.items():
                if api in ('GET'):
                    blueprint.get(url)(func)
                if api == 'GET_MANY':
                    blueprint.get(url)(func)
                elif api == 'POST':
                    blueprint.post(url)(func)
                elif api == 'PATCH':
                    blueprint.patch(url)(func)
                elif api == 'DELETE':
                    blueprint.delete(url)(func)

        self.error_handler = error_handler or self.error_handler
        self.error_handler.register_errorhandlers(blueprint)

        app.register_blueprint(blueprint)

        return blueprint

    def ensure_schema(self, schema):
        if isinstance(schema, SchemaMeta):
            return schema()
        return schema

    def exception(self, ex):
        raise ex

    def get(self, schema=None, url=None):
        schema = schema or self.default_schema
        url = url or f'{self.url_prefix}/<int:pk>'
        schema = self.ensure_schema(schema)
        self.get_serializer_class = lambda: schema

        def _get(func):
            @functools.wraps(func)
            def __get(*args, **kwargs):
                try:
                    result = func(*args, **kwargs)
                    return self.get_response(result, schema)
                except Exception as ex:
                    self.get_exception(ex)

            self.apis['GET'][url] = __get
            return __get

        return _get

    def get_response(self, obj, schema):
        if isinstance(obj, (Response, tuple)):
            return obj, 200
            # TODO should be able to return {"id": 1}, 200; also {"id": 1}, 200, {"X-Noob": "you"}
            # If first param is a dct then we can jsonify if
        return jsonify(schema.dump(obj)), 200

    def get_exception(self, ex):
        self.exception(ex)

    def get_many(self, schema=None, url=None, filterset_fields=None):
        schema = schema or self.default_schema
        url = url or self.url_prefix
        filterset_fields = filterset_fields or []
        schema = self.ensure_schema(schema)
        self.get_many_serializer_class = lambda: schema

        def _get_many(func):
            @functools.wraps(func)
            def __get_many(*args, **kwargs):
                try:
                    result = func(*args, **kwargs)
                    return self.get_many_response(result, schema, filterset_fields)
                except Exception as ex:
                    self.get_many_exception(ex)

            self.apis['GET_MANY'][url] = __get_many
            return __get_many

        return _get_many

    def get_many_response(self, objs, schema, filterset_fields=None):
        if isinstance(objs, (Response, tuple)):
            return objs, 200
        if isinstance(objs, dict):
            return objs
        result = {
            'items': schema.dump(objs, many=True)
        }
        return jsonify(result), 200

    def get_many_exception(self, ex):
        self.exception(ex)

    def post(self, schema=None, output_schema=None, url=None):
        schema = schema or self.default_schema
        output_schema = output_schema or schema
        url = url or self.url_prefix
        schema = self.ensure_schema(schema)
        output_schema = self.ensure_schema(output_schema)
        self.post_serializer_class = lambda: schema

        def _post(func):
            @functools.wraps(func)
            def __post(*args, **kwargs):
                try:
                    payload = self.post_payload(schema)
                    result = func(*args, payload=payload, **kwargs)
                    return self.post_response(result, output_schema)
                except Exception as ex:
                    self.post_exception(ex)

            self.apis['POST'][url] = __post
            return __post

        return _post

    def post_payload(self, schema):
        payload = schema.load(request.json)

        return payload

    def post_response(self, obj, schema):
        return jsonify(obj), 201

    def post_exception(self, ex):
        self.exception(ex)

    def patch(self, schema=None, url=None):
        schema = schema or self.default_schema
        url = url or f'{self.url_prefix}/<int:pk>'
        schema = self.ensure_schema(schema)
        self.patch_serializer_class = lambda: schema

        def _patch(func):
            @functools.wraps(func)
            def __patch(*args, **kwargs):
                try:
                    payload = self.patch_payload(schema)
                    result = func(*args, payload=payload, **kwargs)
                    return self.patch_response(result, schema)
                except Exception as ex:
                    self.patch_exception(ex)

            self.apis['PATCH'][url] = __patch
            return __patch

        return _patch

    def patch_payload(self, schema):
        return schema.load(request.json, partial=True)

    def patch_response(self, obj, schema):
        if not obj:
            return jsonify({}), 200
        return jsonify(obj), 200

    def patch_exception(self, ex):
        self.exception(ex)

    def delete(self, schema=None, url=None):
        schema = schema or self.default_schema
        url = url or f'{self.url_prefix}/<int:pk>'
        schema = self.ensure_schema(schema)

        def _delete(func):
            @functools.wraps(func)
            def __delete(*args, **kwargs):
                try:
                    result = func(*args, **kwargs)
                    return self.delete_response(result)
                except Exception as ex:
                    self.delete_exception(ex)

            self.apis['DELETE'][url] = __delete
            return __delete

        return _delete

    def delete_response(self, obj):
        status_code = 200
        headers = {}
        if not obj:
            return jsonify({}), status_code, headers
        if isinstance(obj, Response):
            return obj, status_code
        if isinstance(obj, tuple):
            if obj[0] == Response:
                return obj
            if len(obj) > 1:
                code = obj[1]
            if len(obj) > 2:
                headers = obj[2]
        return jsonify(obj), status_code, headers

    def delete_exception(self, ex):
        self.exception(ex)
from flask import jsonify, request
from pyrestsql.api import BaseApi, _get_error_constraint_name, _get_error_field_value
from pyrestsql.exc import AuthorizationError
from pyrestsql.api.pony.filters import FilterSet
from pyrestsql.api.pony.pagination import Pagination


from pony.orm import select, delete, db_session


class Api(BaseApi):
    url_prefix = None

    model = None

    apis = ['GET', 'GET_MANY', 'POST', 'PATCH', 'DELETE']

    pagination = Pagination()

    filterset_class = FilterSet
    filterset_fields = None
    filterset = None

    blueprint = None
    Session = None

    def __init__(self, api=None):
        super().__init__(api)

    # TODO do we need pony's db
    #@classmethod
    #def register_app(cls, app, Session):
    #    cls.Session = Session
    #    super().register_app(app)

    def queryset(self):
        if self.model is not None:
            return select(obj for obj in self.model)

        return None

    def _primary_key_field(self):
        return self.model._pk_columns_[0]

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
            lambda model: getattr(model, primary_key_field) == pk
        )

        with db_session:
            if (obj := query.get()) is None:
                raise AuthorizationError()

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

        with db_session:
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

        try:
            pk = self.perform_create(payload)
        except IntegrityError as ex:
            return self.handle_integrity_error(ex)

        return self.post_response(pk)

    def post_payload(self, json=None):
        json = json or request.json

        serializer = self._ensure_schema(self.post_serializer_class)

        payload = serializer.load(json)

        return payload

    def post_response(self, pk):
        primary_key_field = self._primary_key_field()

        return jsonify({primary_key_field: pk}), 201

    def perform_create(self, payload):
        with self.db:
            # TODO
            pk = self.queryset().model.insert_where(
                **payload,
                where=self.post_permissions(payload)
            )

        if pk is None:
            raise AuthorizationError()

        return pk

    def patch(self, pk, json=None):
        if not (payload := self.patch_payload(json)):
            return jsonify({}), 200

        try:
            self.perform_update(pk, payload)
        except IntegrityError as ex:
            return self.handle_integrity_error(ex)

        return self.patch_response()

    def patch_payload(self, json=None):
        json = json or request.json

        serializer = self._ensure_schema(self.patch_serializer_class)

        return serializer.load(json, partial=True)

    def perform_update(self, pk, payload):
        #query = self.patch_queryset(pk, payload)

        #query = self.patch_permissions(query, payload)

        obj = self.model[pk]
        obj.set(**payload)

        with db_session:
            is_updated = session.execute(query).rowcount

        if not is_updated:
            raise AuthorizationError()

    def patch_queryset(self, pk, payload):
        model = self.model

        primary_key_field = self._primary_key_field()

        query = update(model).where(
            primary_key_field == pk
        ).values(
            **payload
        )

        return query

    def patch_response(self):
        return jsonify({}), 200

    def delete(self, pk):
        self.perform_delete(pk)

        return self.delete_response()

    def delete_response(self):
        return jsonify({}), 200

    def perform_delete(self, pk):
        query = self.delete_permissions(self.delete_queryset(pk))

        with db_session:
            is_deleted = session.execute(query).rowcount

        if not is_deleted:
            raise AuthorizationError()

    def delete_queryset(self, pk):
        model = self.queryset().model

        primary_key_field = self._primary_key_field()

        query = delete(model).where(
            primary_key_field == pk
        )

        return query

    def handle_integrity_error(self, ex):
        constraint_name = _get_error_constraint_name(ex)
        bad_value = _get_error_field_value(ex)

        raise ex



@api.get('/<int:pk>', ProjectSchema)
def get_project(pk):
    project = ProjectSchema[pk]
    if project.owner != g.owner:
        raise AuthorizationError()

    return project

@api.patch('/<int:pk>', ProjectSchema)
def patch_project(pk, payload):
    project = ProjectSchema[pk]
    if project.owner != g.owner:
        raise AuthorizationError()

    project.set(**payload)
    commit()
    return project


@api.get('<int:pk>', ProjectSchema)
def get_project(pk):
    return Project.select().where(Project.id == pk, Project.owner == g.owner)


@api.patch('<int:pk>', ProjectSchema)
def patch_project(pk, payload):
    return Project.update(**payload).where(Project.id == pk, Project.owner == g.owner)
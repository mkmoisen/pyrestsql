import marshmallow
import operator

from marshmallow import Schema
from marshmallow.schema import SchemaMeta
from typing import NamedTuple


class Filter(NamedTuple):
    key: str
    operator: callable
    column: object
    format: str = None


class _FilterSet:
    def __init__(self, filterset_fields, query):
        self.query = query
        self.filterset_fields = self.ensure_fields(filterset_fields)

        self.filterset_schema = self.make_serializer_class()

    def __call__(self, query_params, query=None):
        return self.apply_filters(query_params, query)

    def ensure_fields(self, filterset_fields) -> dict:
        new_fields = {}

        query_fields = self.make_query_fields()

        self.validate_string_fields(filterset_fields, query_fields)

        for field in filterset_fields:
            field = self.ensure_field(field, query_fields)

            new_fields[field.key] = field

        return new_fields

    def validate_string_fields(self, filterset_fields, query_fields):
        invalid_field_names = [
            field for field in filterset_fields
            if isinstance(field, str) and field not in query_fields
        ]

        if invalid_field_names:
            raise Exception(f'Cannot find these filterset_fields on the queryset: {", ".join(invalid_field_names)}')

    def make_query_fields(self):
        if self.query is None:
            return {}

        return self._make_query_fields()

    def _make_query_fields(self):
        raise NotImplementedError()

    def ensure_field(self, field, query_fields):
        if isinstance(field, str):
            return self.make_field_from_string(field, query_fields)

        return field

    def make_field_from_string(self, key, query_fields):
        column = query_fields[key]

        return Filter(
            key,
            operator.eq,
            column
        )

    def make_serializer_class(self):
        serializer_attributes = {
            'Meta': type('Meta', (), {'unknown': marshmallow.EXCLUDE})
        }

        for field in self.filterset_fields.values():
            serializer_attributes[field.key] = self.make_serializer_attribute(field)

        return SchemaMeta(
            'FilterSchema',
            (Schema,),
            serializer_attributes
        )

    def make_serializer_attribute(self, field):
        marshmallow_field_class = self.make_marshmellow_field_class(field.column)

        marshmallow_kwargs = self.make_marshmellow_kwargs(field)

        return marshmallow_field_class(required=False, allow_none=True, **marshmallow_kwargs)

    def make_marshmellow_field_class(self, column):
        raise NotImplementedError()

    def make_marshmellow_kwargs(self, field):
        return {}

    def parse_query_params(self, query_params=None):
        return self.filterset_schema().load(query_params)

    def apply_filters(self, query_params=None, query=None):
        if query is None:
            query = self.query
        #query = query or self.query

        if not self.filterset_fields:
            return query

        query_params = self.parse_query_params(query_params)

        query = self._apply_filters(query_params, query)

        return query

    def _apply_filters(self, query_params, query):
        for key, value in query_params.items():
            filter = self.filterset_fields[key]
            query = query.where(
                filter.operator(filter.column, value)
            )

        return query


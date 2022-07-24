import sqlalchemy
from keelblok.api.api.filters import _FilterSet
from marshmallow import fields


class FilterSet(_FilterSet):
    def _make_query_fields(self):
        selected_columns = self.query.selected_columns

        query_fields = {
            column.name: column
            for column in selected_columns
        }

        return query_fields

    def make_marshmellow_field_class(self, column):
        if isinstance(column, sqlalchemy.Integer):
            marshmallow_field = fields.Int
        elif isinstance(column, sqlalchemy.Numeric):
            marshmallow_field = fields.Number
        elif isinstance(column, sqlalchemy.Date):
            marshmallow_field = fields.DateTime,  # TODO what about UTC ...
        elif isinstance(column, sqlalchemy.DateTime):
            marshmallow_field = fields.Date,  # TODO what about UTC ...
        elif isinstance(column, sqlalchemy.Time):
            marshmallow_field = fields.Time
        else:
            marshmallow_field = fields.Str

        return marshmallow_field

    def make_marshmellow_kwargs(self, field):
        marshmallow_kwargs = {}
        if isinstance(field.column, (sqlalchemy.Date, sqlalchemy.DateTime, sqlalchemy.Time)):
            marshmallow_kwargs['format'] = field.format

        return marshmallow_kwargs



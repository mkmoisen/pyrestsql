import peewee
from pyrestsql.api.filters import _FilterSet
from marshmallow import fields


class FilterSet(_FilterSet):
    def _make_query_fields(self):
        selected_columns = self.query._returning

        query_fields = {
            column.column_name: column
            if hasattr(column, 'column_name') else column._alias
            for column in selected_columns
        }

        return query_fields

    def make_marshmellow_field_class(self, column):
        if isinstance(column, (peewee.IntegerField, peewee.ForeignKeyField)):
            marshmallow_field = fields.Int
        elif isinstance(column, (peewee.FloatField, peewee.DecimalField)):
            marshmallow_field = fields.Number
        elif isinstance(column, peewee.DateField):
            marshmallow_field = fields.DateTime,  # TODO what about UTC ...
        elif isinstance(column, peewee.DateTimeField):
            marshmallow_field = fields.Date,  # TODO what about UTC ...
        elif isinstance(column, peewee.TimeField):
            marshmallow_field = fields.Time
        else:
            marshmallow_field = fields.Str

        return marshmallow_field

    def make_marshmellow_kwargs(self, field):
        marshmallow_kwargs = {}
        if isinstance(field.column, (peewee.DateField, peewee.DateTimeField, peewee.TimeField)):
            marshmallow_kwargs['format'] = field.format

        return marshmallow_kwargs



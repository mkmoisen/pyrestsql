from datetime import datetime

import peewee
from flask import flash, current_app, url_for
from flask_admin._compat import as_unicode
from flask_admin.actions import action
from flask_admin.babel import gettext, lazy_gettext, ngettext
from flask_admin.model import BaseModelView
from pyrestsql.api.simple_api import SimpleApi
from pyrestsql.exc import AuthenticationError, AuthorizationError, EntityNotFound
from wtforms import fields as wtforms_fields
from marshmallow import fields as marshmallow_fields
import logging
import wtforms

from wtforms import form
from flask_admin.form import widgets

logger = logging.getLogger(__name__)


class ForeignKeyField(wtforms.IntegerField):
    def __init__(self, model, **kwargs):
        super().__init__(**kwargs)
        self.model = model

    def has_groups(self):
        return False

    def iter_groups(self):
        raise NotImplementedError()

    def iter_choices(self):
        raise NotImplementedError()


class ModelForeignKeyField(ForeignKeyField):
    def __init__(self, model, **kwargs):
        self.foreign_key_model = kwargs.pop('foreign_key_model')
        super().__init__(model, **kwargs)
        self.model_class = self.model.model
        self.db = self.model.db  # TODO remove replace with rest call I guess or have a Peewee version

    def iter_choices(self):
        yield ('', 'null', False)

        with self.db:
            for obj in self.foreign_key_model.select():
                yield obj.id, str(obj), obj.id == self.data


class ModelConverter:
    foreign_key_field_class = ModelForeignKeyField

    def __init__(self, model):
        self.model = model
        self.model_class = self.model.model

    defaults = {
        marshmallow_fields.Str: wtforms_fields.StringField,
        marshmallow_fields.DateTime: wtforms_fields.StringField,  # wtforms_fields.DateTimeField,
        marshmallow_fields.Int: wtforms_fields.IntegerField,
    }

    def convert(self, field):
        field_type = None
        for marshmallow_field, wtform_field in self.defaults.items():
            if isinstance(field, marshmallow_field):
                field_type = wtform_field
                break

        if field_type is None:
            raise AttributeError("There is not possible conversion for '%s'" % type(field))

        kwargs = {
            'label': field.name,
            'filters': [],
            'validators': [],
        }

        if not field.required:
            kwargs['validators'].append(wtforms.validators.Optional())
            # Necessary to convert the wtform "" to None before passing to rest api
            kwargs['filters'].append(lambda value: None if value == "" else value)

        if field_type is wtforms_fields.DateTimeField:
            # Necessary to convert the wtform datetime value into a string before passing to rest api
            # kwargs['filters'].append(lambda dt: (dt.isoformat() if dt is not None else None))
            kwargs['filters'].append(lambda dt: (dt.isoformat() if isinstance(dt, datetime) else dt))
            kwargs['format'] = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d'T'%H:%M:%S", "%Y-%m-%d"]
            # kwargs['widget'] = widgets.DateTimePickerWidget()

        # TODO this is peewee oriented need another method...why not serializer class?
        columns = self.model_class._meta.columns
        if (column := columns.get(field.name)) and isinstance(column, peewee.ForeignKeyField):
            field_type = self.foreign_key_field_class
            kwargs['validators'].append(wtforms.validators.Optional())
            kwargs['widget'] = widgets.Select2Widget()
            kwargs['model'] = self.model
            kwargs['foreign_key_model'] = column.rel_model

        return field_type(**kwargs)

class ApiModelView(BaseModelView):
    model_form_converter = ModelConverter

    def get_pk_value(self, model):
        return getattr(model, self.get_pk_name())

    def get_pk_name(self):
        return self.model().get_pk_name()

    def scaffold_list_columns(self):
        serializer_class = self.model().get_many_serializer_class

        return [
            field_name
            for field_name, field in serializer_class.fields.items()
            if not field.load_only
        ]

    def scaffold_sortable_columns(self):
        return self.scaffold_list_columns()

    def get_edit_form(self):
        return self.get_form('edit')

    def get_create_form(self):
        return self.get_form('create')

    def get_form(self, form_type=None):
        return self.scaffold_form(form_type)

    def scaffold_form(self, form_type=None):
        result = self.model_form(
            base_class=self.form_base_class,
            only=self.form_columns,
            exclude=self.form_excluded_columns,
            field_args=self.form_args,
            allow_pk=bool(self.form_columns),
            form_type=form_type
        )

        if self.form_extra_fields:
            for name, field in self.form_extra_fields.items():
                setattr(result, name, form.recreate_field(field))

        return result

    def model_form(self, base_class=form.Form, allow_pk=False, only=None, exclude=None,
                       field_args=None, converter=None, form_type=None):

        field_dict = self.model_fields(allow_pk, only, exclude, field_args, converter, form_type)

        return type(self.model.__name__ + 'Form', (base_class,), field_dict)

    def model_fields(self, allow_pk=False, only=None, exclude=None,
                     field_args=None, converter=None, form_type=None):
        """
        Generate a dictionary of fields for a given Peewee model.

        See `model_form` docstring for description of parameters.
        """

        converter = converter or self.model_form_converter(self.model)
        if form_type == 'edit':
            serializer = self.model.patch_serializer_class
        elif form_type == 'create':
            serializer = self.model.post_serializer_class
        else:
            serializer = self.model.get_serializer_class

        field_dct = {}
        for name, field in serializer().fields.items():
            if not field.dump_only:
                field_dct[name] = converter.convert(field)

        return field_dct

    def scaffold_list_form(self, widget=None, validators=None):
        pass

    def api_headers(self):
        return {}

    def raise_if_response_error(self, response):
        if response.status_code == 401:
            raise AuthenticationError(response.json)
        elif response.status_code == 403:
            raise AuthorizationError(response.json)
        elif response.status_code == 404:
            raise EntityNotFound(response.json)
        elif not str(response.status_code).startswith('2'):
            raise Exception(response.json)

    def get_list(self, page, sort_field, sort_desc, search, filters, page_size=None):
        try:
            test_client = current_app.test_client()

            _url_for = self.model.get_many_url_for
            if _url_for is None:
                raise Exception('GET_MANY api not supported for this model')

            response = test_client.get(url_for(_url_for), headers=self.api_headers())

            self.raise_if_response_error(response)

            #response, code = api_class.get_many()
            count = None
            # Not ideal of course: this is peewee specific?
            model_class = self.model.model
            objs = [
                model_class(**obj)
                for obj in response.json['items']
            ]
            return count, objs
        except Exception as ex:
            if not self.handle_view_exception(ex):
                print('handling exception lol')
                flash(gettext('Failed to get record. %(error)s', error=str(ex)), 'error')
                logger.exception('Failed to get record.')

            return 0, []

    def get_one(self, id):
        test_client = current_app.test_client()
        try:
            _url_for = self.model.get_url_for
            if _url_for is None:
                raise Exception('GET api not supported for this model')

            response = test_client.get(url_for(_url_for, pk=id), headers=self.api_headers())

            self.raise_if_response_error(response)

            model_class = self.model.model

            return model_class(**response.json)
        except Exception as ex:
            if not self.handle_view_exception(ex):
                print('handling exception lol')
                flash(gettext('Failed to get record. %(error)s', error=str(ex)), 'error')
                logger.exception('Failed to get record.')

            return None

    def create_model(self, form):
        test_client = current_app.test_client()
        try:
            _url_for = self.model.post_url_for
            if _url_for is None:
                raise Exception('POST api not supported for this model')

            response = test_client.post(url_for(_url_for), headers=self.api_headers(), json=form.data)

            self.raise_if_response_error(response)

            model_class = self.model.model

            model = model_class(**{self.get_pk_name(): response.json[self.get_pk_name()]})
        except Exception as ex:
            if not self.handle_view_exception(ex):
                flash(gettext('Failed to create record. %(error)s', error=str(ex)), 'error')
                logger.exception('Failed to create record.')
            return False

        else:
            self.after_model_change(form, model, True)

        return model

    def update_model(self, form, model):
        test_client = current_app.test_client()
        try:
            form.populate_obj(model)
            self._on_model_change(form, model, False)

            _url_for = self.model.patch_url_for
            if _url_for is None:
                raise Exception('PATCH api not supported for this model')

            response = test_client.patch(url_for(_url_for, pk=model.id), headers=self.api_headers(), json=form.data)

            self.raise_if_response_error(response)

            # For peewee have to save inline forms after model was saved
            # save_inline(form, model)  # TODO ? no clue lol
        except Exception as ex:
            if not self.handle_view_exception(ex):
                flash(gettext('Failed to update record. %(error)s', error=str(ex)), 'error')
                logger.exception('Failed to update record.')

            return False
        else:
            self.after_model_change(form, model, False)

        return True

    def delete_model(self, model):
        test_client = current_app.test_client()
        try:
            self.on_model_delete(model)

            _url_for = self.model.url_for('DELETE')
            if _url_for is None:
                raise Exception('DELETE api not supported for this model')

            response = test_client.delete(url_for(_url_for, pk=model.id), headers=self.api_headers())

            self.raise_if_response_error(response)

        except Exception as ex:
            if not self.handle_view_exception(ex):
                flash(gettext('Failed to delete record. %(error)s', error=repr(ex)), 'error')
                logger.exception('Failed to delete record.')

            return False
        else:
            self.after_model_delete(model)

        return True

    @action('delete',
            lazy_gettext('Delete'),
            lazy_gettext('Are you sure you want to delete selected records?'))
    def action_delete(self, ids):
        model_class = self.model.model

        try:
            deleted_count = 0
            for id_ in ids:
                id_ = int(id_)  # TODO won't work if we ever use str primary key

                deleted_count += self.delete_model(model_class(**{self.get_pk_name(): id_}))

            flash(ngettext('Record was successfully deleted.',
                           '%(count)s records were deleted.',
                           deleted_count,
                           count=deleted_count), 'success' if deleted_count == len(ids) else 'warning')
        except Exception as ex:
            if not self.handle_view_exception(ex):
                flash(gettext('Failed to delete records. %(error)s', error=str(ex)), 'error')

    def _create_ajax_loader(self, name, options):
        pass

    def handle_view_exception(self, exc):
        logger.exception(exc)
        flash(as_unicode(repr(exc)), 'error')

        return True

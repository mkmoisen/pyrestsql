from flask import Blueprint, render_template, jsonify
import apispec


class Swagger:
    url_prefix = '/swagger/'
    title = 'Swagger'
    version = None
    info = None
    plugins = None
    servers = None
    openapi_version = '3.0.3'

    def __init__(self, app, base_api_class):
        blueprint = Blueprint(
            'swagger', __name__, static_folder='static', template_folder='templates', url_prefix=self.url_prefix
        )

        blueprint.get('/')(self.get_swagger_ui)
        blueprint.get('/spec/')(self.get_swagger_json_spec)

        app.register_blueprint(blueprint)

        self.base_api_class = base_api_class

        self.info = self.info or {'description': 'Swagger'}
        self.plugins = self.plugins or []
        self.servers = self.servers or []

    def get_swagger_ui(self):
        return render_template('swaggerui.html')

    def get_swagger_json_spec(self):
        return jsonify(self.generate_openapi_spec()), 200

    def generate_openapi_spec(self):
        if (marshmallow_plugin := apispec.ext.marshmallow.MarshmallowPlugin()) not in self.plugins:
            self.plugins.append(marshmallow_plugin)

        spec = apispec.APISpec(
            title=self.title,
            version=self.version,
            openapi_version=self.openapi_version,
            info=self.info,
            plugins=self.plugins,
            servers=self.servers,
        )

        self.add_security_scheme(spec)

        self.add_no_content_schema(spec)

        self.add_error_schema(spec)

        subclasses = list(reversed(self.base_api_class.__subclasses__()))

        while subclasses:
            subclass = subclasses.pop()
            subclasses.extend(list(reversed(subclass.__subclasses__())))

            if not subclass.blueprint:
                continue


            detail_operations = {}

            if 'GET' in subclass.apis:
                detail_operations['get'] = self.generate_get_spec(subclass, spec)

            if 'PATCH' in subclass.apis:
                detail_operations['patch'] = self.generate_patch_spec(subclass, spec)

            if 'DELETE' in subclass.apis:
                detail_operations['delete'] = self.generate_delete_spec(subclass, spec)

            spec.path(
                path=subclass.url_prefix + '{pk}',
                operations=detail_operations,
                summary=subclass.__name__,
                description=subclass.__doc__,
                parameters=[{
                    'in': 'path',
                    'name': 'pk',
                    'schema': {
                        'type': 'integer'
                    },
                    'required': True,
                    'description': 'Primary key of the resource'
                }]
            )

            collection_operations = {}

            if 'GET_MANY' in subclass.apis:
                collection_operations['get'] = self.generate_get_many_spec(subclass, spec)

            if 'POST' in subclass.apis:
                collection_operations['post'] = self.generate_post_spec(subclass, spec)

            spec.path(
                path=subclass.url_prefix,
                operations=collection_operations,
                summary=subclass.__name__,
                description=subclass.__doc__,
            )

        return spec.to_dict()

    def add_security_scheme(self, spec):
        pass

    def add_no_content_schema(self, spec):
        spec.components.schema(
            'no_content',
            {
                'properties': {
                }
            }
        )

    def add_error_schema(self, spec):
        # TODO responses is an alternate way to do this

        spec.components.schema(
            'error',
            {
                'properties': {
                    'error': {
                        'type': 'array',
                        'items': {
                            'type': 'string',
                        }
                    }
                }
            }
        )

    def get_schema_name(self, api_class):
        return f'get_{api_class.__name__}'

    def add_get_schema(self, api_class, spec):
        # TODO Not sure if this needs to be at the components level, since it will not be reused elsewhere
        spec.components.schema(self.get_schema_name(api_class), schema=api_class().get_serializer_class())

    def get_many_schema_name(self, api_class):
        return f'get_many_{api_class.__name__}'

    def add_get_many_schema(self, api_class, spec):
        spec.components.schema(self.get_many_schema_name(api_class), schema=api_class().get_many_serializer_class())

    def patch_schema_name(self, api_class):
        return f'patch_{api_class.__name__}'

    def add_patch_schema(self, api_class, spec):
        spec.components.schema(self.patch_schema_name(api_class), schema=api_class().patch_serializer_class())

    def post_schema_name(self, api_class):
        return f'post_{api_class.__name__}'

    def add_post_schema(self, api_class, spec):
        spec.components.schema(self.post_schema_name(api_class), schema=api_class().post_serializer_class())

    def generate_get_spec(self, api_class, spec):
        self.add_get_schema(api_class, spec)

        description = api_class.get_documentation()

        return {
            'summary': f'Gets one {api_class.__name__}',
            'description': description,
            'tags': [
                api_class.__name__
            ],
            'responses': {
                '200': {
                    'content': {
                        'application/json': {
                            'schema': self.get_schema_name(api_class)
                        }
                    },
                }
            }
        }

    def generate_patch_spec(self, api_class, spec):
        self.add_patch_schema(api_class, spec)

        description = api_class.patch_documentation()

        return {
            'summary': f'Patch one {api_class.__name__}',
            'description': description,
            'tags': [
                api_class.__name__
            ],
            'requestBody': {
                'description': api_class().patch_serializer_class().__doc__,
                'required': True,
                'content': {
                    'application/json': {
                        'schema': self.patch_schema_name(api_class)
                    }
                }
            },
            'responses': {
                '200': {
                    'content': {
                        'application/json': {
                            'schema': 'no_content',
                        }
                    }
                }
            }
        }

    def generate_delete_spec(self, api_class, spec):
        description = api_class.delete_documenation()

        return {
            'summary': f'Delete one {api_class.__name__}',
            'description': description,
            'tags': [
                api_class.__name__
            ],
            'responses': {
                '200': {
                    'content': {
                        'application/json': {
                            'schema': 'no_content',
                        }
                    }
                }
            }
        }

    def generate_get_many_spec(self, api_class, spec):
        self.add_get_many_schema(api_class, spec)

        description = api_class.get_many_documentation()

        # TODO queryparameters from filters

        return {
            'summary': f'Get many {api_class.__name__}',
            'security': [{'token': []}],
            'description': description,
            'tags': [
                api_class.__name__
            ],
            'responses': {
                '200': {
                    'content': {
                        'application/json': {
                            'type': 'array',
                            'schema': self.get_many_schema_name(api_class)  # TODO array of this schema
                        }
                    },
                }
            }
        }

    def generate_post_spec(self, api_class, spec):
        self.add_post_schema(api_class, spec)

        description = api_class.post_documentation()

        return {
            'summary': f'Post one {api_class.__name__}',
            'description': description,
            'tags': [
                api_class.__name__
            ],
            'requestBody': {
                'description': api_class().post_serializer_class().__doc__,
                'required': True,
                'content': {
                    'application/json': {
                        'schema': self.post_schema_name(api_class)
                    }
                }
            },
            'responses': {
                '201': {
                    'content': {
                        'application/json': {
                            'type': 'object',
                            'properties': {
                                'id': {
                                    'type': 'int',
                                }
                            }
                        }
                    }
                }
            }
        }

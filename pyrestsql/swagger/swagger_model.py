class SwaggerModel:
    def __init__(self, name, url_prefix, get_documentation, get_many_documentation, post_documentation, patch_documentation,
                 delete_documentation, default_schema, get_serializer_class, get_many_serializer_class, patch_serializer_class, patch_output_serializer_class,
                 post_serializer_class, post_output_serializer_class, filterset_fields, apis=None):
        self.name = name
        self.url_prefix = url_prefix
        self.get_documentation = get_documentation
        self.get_many_documentation = get_many_documentation
        self.post_documentation = post_documentation
        self.patch_documentation = patch_documentation
        self.delete_documentation = delete_documentation
        self.default_schema = default_schema
        self.get_serializer_class = get_serializer_class
        self.get_many_serializer_class = get_many_serializer_class
        self.patch_serializer_class = patch_serializer_class
        self.patch_output_serializer_class = patch_output_serializer_class
        self.post_serializer_class = post_serializer_class
        self.post_output_serializer_class = post_output_serializer_class
        self.filterset_fields = filterset_fields
        self.apis = apis or []


class SwaggerSimpleConverter:
    def __init__(self, simple_api, **kwargs):
        self.simple_api = simple_api

    def swagger(self, name):
        # TODO what about user custom APIS like GET /api/foos/bar/123
        apis = [
            api for api in ['GET', 'GET_MANY', 'POST', 'PATCH', 'DELETE']
            if self._has_api_function(api)
        ]

        return SwaggerModel(
            name=name,
            url_prefix=self.simple_api.url_prefix,
            get_documentation=self._documentation('GET'),
            get_many_documentation=self._documentation('GET_MANY'),
            post_documentation=self._documentation('POST'),
            patch_documentation=self._documentation('PATCH'),
            delete_documentation=self._documentation('DELETE'),
            default_schema=self.simple_api.default_schema,
            get_serializer_class=self.simple_api.get_serializer_class(),
            get_many_serializer_class=self.simple_api.get_many_serializer_class(),
            patch_serializer_class=self.simple_api.patch_serializer_class(),
            patch_output_serializer_class=self.simple_api.patch_output_serializer_class(),
            post_serializer_class=self.simple_api.post_serializer_class(),
            post_output_serializer_class=self.simple_api.post_output_serializer_class(),
            filterset_fields=self.simple_api.filterset_fields,
            apis=apis,
        )

    def _has_api_function(self, api):
        functions = list(self.simple_api.apis.get(api, {}).values())
        if functions:
            return functions[0]


    def _documentation(self, api):
        function = self._has_api_function(api)
        if function:
            return function.__doc__

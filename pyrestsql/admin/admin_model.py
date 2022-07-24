class AdminModel:
    def __init__(
            self, model, name, get_many_serializer_class, get_serializer_class, post_serializer_class, patch_serializer_class,
            get_many_url_for, get_url_for, post_url_for, patch_url_for, delete_url_for
    ):
        self.model = model
        self.__name__ = name or self.model.__name__
        self.get_many_serializer_class = get_many_serializer_class
        self.get_serializer_class = get_serializer_class
        self.post_serializer_class = post_serializer_class
        self.patch_serializer_class = patch_serializer_class
        self.get_many_url_for = get_many_url_for
        self.get_url_for = get_url_for
        self.post_url_for = post_url_for
        self.patch_url_for = patch_url_for
        self.delete_url_for = delete_url_for

    def __call__(self):
        return self

    def get_pk_name(self):
        raise NotImplementedError


class PeeweeAdminModel(AdminModel):
    def __init__(self, *args, db, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = db

    def get_pk_name(self):
        pk_field_name = self.model._meta.primary_key.column_name
        return pk_field_name

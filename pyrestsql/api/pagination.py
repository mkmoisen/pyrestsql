import marshmallow
from flask import request
from marshmallow import Schema, fields, validate
from marshmallow.schema import SchemaMeta


class _Pagination:
    count_key = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def paginate(self, query):
        meta = {}
        return query, meta

    def add_count_subquery(self, query):
        raise NotImplementedError()

    def _count_logic(self, query):
        count = 0
        query = self.add_count_subquery(query)
        return query, count

    def add_count_meta(self, objs, meta):
        return


class _LimitOffsetPagination(_Pagination):
    max_limit = None
    default_limit = None
    limit_key = 'limit'
    offset_key = 'offset'
    count_key = 'count'

    def __init__(
            self, default_limit=default_limit, max_limit=max_limit,
            limit_key=limit_key, offset_key=offset_key, count_key=count_key
    ):
        super().__init__()

        self.default_limit = default_limit
        self.max_limit = max_limit
        self.limit_key = limit_key
        self.offset_key = offset_key
        self.count_key = count_key

        self.serializer_class = SchemaMeta(
            f'{self.__class__.__name__}Schema',
            (Schema,),
            {
                self.limit_key: fields.Int(validate=[validate.Range(0, None)], load_default=self.default_limit),
                self.offset_key: fields.Int(validate=[validate.Range(0, None)], load_default=0),
                'Meta': type('Meta', (), {'unknown': marshmallow.EXCLUDE})
            }
        )

    def paginate(self, query):
        params = self.serializer_class().load(request.args)
        limit = params['limit']
        offset = params['offset']

        if self.max_limit:
            limit = min(limit, self.max_limit)

        query, count = self.limit_offset_logic(query, limit, offset)

        meta = {
            self.count_key: count,
            self.limit_key: limit,
            self.offset_key: offset,
        }

        return query, meta

    def limit_offset_logic(self, query, limit, offset):
        count = 0
        if limit:
            query, count = self._count_logic(query)

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        return query, count

    def add_count_meta(self, objs, meta):
        if not meta['limit']:
            meta[self.count_key] = len(objs)
        elif objs:
            meta[self.count_key] = objs[0]._api_total_count


class _LimitOffsetPaginationEagerCount(_LimitOffsetPagination):
    def _count_logic(self, query):
        raise NotImplementedError()

    def add_count_meta(self, objs, meta):
        if not meta['limit']:
            meta[self.count_key] = len(objs)


class _PageNumberPagination(_Pagination):
    page_key = 'page'
    page_size_key = 'page_size'
    count_key = 'count'

    page_size = 1000
    max_page_size = None

    def __init__(self, page_size=page_size, max_page_size=max_page_size, page_key=page_key, page_size_key=page_size_key, count_key=count_key):
        super().__init__()

        self.page_size = page_size
        self.page_key = page_key
        self.page_size_key = page_size_key
        self.count_key = count_key
        self.max_page_size = max_page_size

        serializer_attributes = {
            'Meta': type('Meta', (), {'unknown': marshmallow.EXCLUDE}),
            self.page_key: fields.Int(validate=[validate.Range(1, None)], load_default=None),
        }
        if self.max_page_size:
            serializer_attributes[self.page_size_key] = fields.Int(
                validate=[validate.Range(0, None)],
                load_default=self.page_size
            )

        self.serializer_class = SchemaMeta(
            'PaginationSchema',
            (Schema,),
            serializer_attributes
        )

    def paginate(self, query):
        params = self.serializer_class().load(request.args)
        page = params[self.page_key]
        page_size = params.get(self.page_size_key) or self.page_size
        if page and self.max_page_size:
            page_size = min(page_size, self.max_page_size)

        query, count = self.paginate_logic(query, page, page_size)

        meta = {
            self.count_key: count,
            self.page_key: page,
        }

        if self.page_size_key and page:
            meta[self.page_size_key] = page_size

        return query, meta

    def paginate_logic(self, query, page, page_size):
        raise NotImplementedError()

    def add_count_meta(self, objs, meta):
        if not meta['page']:
            meta[self.count_key] = len(objs)
        elif objs:
            meta[self.count_key] = objs[0]._api_total_count


class _PageNumberPaginationEagerCount(_PageNumberPagination):
    def _count_logic(self, query):
        raise NotImplementedError()

    def add_count_meta(self, objs, meta):
        if not meta['page']:
            meta[self.count_key] = len(objs)

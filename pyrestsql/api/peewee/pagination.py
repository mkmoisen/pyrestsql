from pyrestsql.api.pagination import (
    _Pagination, _LimitOffsetPagination, _LimitOffsetPaginationEagerCount,
    _PageNumberPagination, _PageNumberPaginationEagerCount,
)
from peewee import Select, fn, SQL


class Pagination(_Pagination):
    def add_count_subquery(self, query):
        return query.select_extend(
            Select([query.clone()], [fn.count(SQL('1'))]).alias('_api_total_count')
        )


class LimitOffsetPagination(_LimitOffsetPagination, Pagination):
    pass


class LimitOffsetPaginationEagerCount(_LimitOffsetPaginationEagerCount, LimitOffsetPagination):
    def __init__(self, *args, db=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = db

    def _count_logic(self, query):
        with self.db:
            count = query.count()

        return query, count


class PageNumberPagination(_PageNumberPagination, Pagination):
    def paginate_logic(self, query, page, page_size):
        count = 0
        if page:
            query, count = self._count_logic(query)

            query = query.paginate(page, page_size)

        return query, count


class PageNumberPaginationEagerCount(_PageNumberPaginationEagerCount, PageNumberPagination):
    def __init__(self, *args, db=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = db

    def _count_logic(self, query):
        with self.db:
            count = query.count()

        return query, count

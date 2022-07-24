from pyrestsql.api.pagination import (_Pagination, _LimitOffsetPagination, _LimitOffsetPaginationEagerCount,
                                         _PageNumberPagination, _PageNumberPaginationEagerCount, )
from sqlalchemy import select, func, text


class Pagination(_Pagination):
    def add_count_subquery(self, query):
        count = query.alias('count')

        return query.add_columns(
            select(func.count(text('1'))).select_from(count).label('_api_total_count')
        )


class LimitOffsetPagination(_LimitOffsetPagination, Pagination):
    pass


class LimitOffsetPaginationEagerCount(_LimitOffsetPaginationEagerCount, LimitOffsetPagination):
    def __init__(self, *args, Session=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.Session = Session

    def _count_logic(self, query):
        with self.Session() as session:
            count = select(func.count(text('1'))).select_from(query.subquery())

            count = session.execute(count).scalar()

        return query, count


class PageNumberPagination(_PageNumberPagination, Pagination):
    def paginate_logic(self, query, page, page_size):
        count = 0
        if page:
            query, count = self._count_logic(query)

            if page > 0:
                page -= 1

            query = query.limit(page_size)

            query = query.offset(page * page_size)

        return query, count


class PageNumberPaginationEagerCount(_PageNumberPaginationEagerCount, PageNumberPagination):
    def __init__(self, *args, Session=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.Session = Session

    def _count_logic(self, query):
        with self.Session() as session:
            count = select(func.count(text('1'))).select_from(query.subquery())

            count = session.execute(count).scalar()

        return query, count

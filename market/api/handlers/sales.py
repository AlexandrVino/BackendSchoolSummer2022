from aiohttp.web_response import Response
from aiohttp_apispec import docs, response_schema

from market.api.schema import ShopUnitResponseSchema
from market.db.schema import shop_units_table as shop_units_t
from market.utils.pg import SelectQuery

from .base import BaseImportView
from .query import CITIZENS_QUERY


class SalesView(BaseImportView):
    URL_PATH = r'/sales'

    @docs(summary='Отобразить товары со скидкой')
    @response_schema(ShopUnitResponseSchema())
    async def get(self):

        query = CITIZENS_QUERY.where(
            shop_units_t.c.import_id == self.import_id
        )
        body = SelectQuery(query, self.pg.transaction())
        return Response(body=body)

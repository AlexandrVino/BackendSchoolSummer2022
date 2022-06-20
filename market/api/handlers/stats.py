from urllib.parse import parse_qs, unquote, urlparse

from aiohttp.web_response import Response
from aiohttp_apispec import docs, response_schema

from market.api.schema import ShopUnitResponseSchema
from .base import BaseImportView


class StatsView(BaseImportView):
    URL_PATH = r'/node/{shop_unit_id:[\w, -]+}/statistic'

    @docs(summary='Отобразить товары со скидкой')
    @response_schema(ShopUnitResponseSchema())
    async def get(self):
        kwargs = parse_qs(urlparse(unquote(str(self.request.url))).query)
        print(kwargs, unquote(str(self.request.url)))
        return Response(body={})

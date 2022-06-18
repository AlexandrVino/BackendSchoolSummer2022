from http import HTTPStatus

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_response import Response
from aiohttp_apispec import docs, request_schema, response_schema
from sqlalchemy import select

from market.api.schema import GetShopUnitResponseSchema, GetShopUnitSchema
from market.db.schema import shop_units_table
from .base import BaseImportView


class NodeView(BaseImportView):
    URL_PATH = r'/nodes/{shop_unit_id:[\w, -]+}'

    @docs(summary='Получить объект со всеми дочерними')
    @request_schema(GetShopUnitSchema())
    @response_schema(GetShopUnitResponseSchema(), code=HTTPStatus.OK.value)
    async def get(self):

        select_query = select(shop_units_table).where(shop_units_table.c.shop_unit_id == self.shop_unit_id)
        shop_unit = await self.get_obj(select_query)

        if not shop_unit:
            raise HTTPNotFound()

        shop_unit = await self.get_obj_tree(shop_unit)

        return Response(body=shop_unit)

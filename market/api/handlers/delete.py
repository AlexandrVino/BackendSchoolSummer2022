from http import HTTPStatus

from aiohttp.web_response import Response
from aiohttp_apispec import docs, request_schema, response_schema

from market.api.schema import GetShopUnitResponseSchema, GetShopUnitSchema, SQL_REQUESTS
from .base import BaseImportView


class DeleteView(BaseImportView):
    URL_PATH = r'/delete/{shop_unit_id:[\w, -]+}'

    @docs(summary='Получить объект со всеми дочерними')
    @request_schema(GetShopUnitSchema())
    @response_schema(GetShopUnitResponseSchema(), code=HTTPStatus.OK.value)
    async def delete(self):
        ides_to_req, _ = await self.get_relative_ides(self.shop_unit_id)
        ides_to_req = tuple(ides_to_req)
        sql_request = SQL_REQUESTS['delete_by_ides'].format(ides_to_req, ides_to_req, ides_to_req)
        await self.pg.execute(sql_request)

        return Response(status=HTTPStatus.OK)
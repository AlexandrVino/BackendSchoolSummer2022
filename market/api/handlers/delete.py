from http import HTTPStatus

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_response import Response
from aiohttp_apispec import docs

from market.api.utils import get_item_tree, SQL_REQUESTS
from market.api.handlers.base import BaseImportView


class DeleteView(BaseImportView):
    URL_PATH = r'/delete/{shop_unit_id:[\w, -]+}'

    @docs(summary='Получить объект со всеми дочерними')
    async def delete(self) -> Response:
        """
        :return: Response
        Метод удаления элемента с каким-либо id из всех таблиц
        """

        ides_to_req, _ = await get_item_tree(self.shop_unit_id, self.pg)
        if not ides_to_req:
            raise HTTPNotFound()
        ides_to_req = tuple(ides_to_req)
        sql_request = SQL_REQUESTS['delete_by_ides'].format(
            ides_to_req, ides_to_req, ides_to_req, ides_to_req).replace(',)', ')')
        await self.pg.execute(sql_request)

        return Response(status=HTTPStatus.OK)

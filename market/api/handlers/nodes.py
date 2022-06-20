from aiohttp.web_response import Response
from aiohttp_apispec import docs

from market.api.handlers.base import BaseImportView


class NodeView(BaseImportView):
    URL_PATH = r'/nodes/{shop_unit_id:[\w, -]+}'

    @docs(summary='Получить объект со всеми дочерними')
    async def get(self) -> Response:
        """
        :return: Response
        Метод получения дерева элемента
        """

        shop_unit = await self.get_obj_tree()
        return Response(body=shop_unit)

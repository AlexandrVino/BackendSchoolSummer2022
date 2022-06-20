from aiohttp.web_response import Response
from aiohttp_apispec import docs

from .base import BaseImportView


class NodeView(BaseImportView):
    URL_PATH = r'/nodes/{shop_unit_id:[\w, -]+}'

    @docs(summary='Получить объект со всеми дочерними')
    async def get(self):
        shop_unit = await self.get_obj_tree()
        return Response(body=shop_unit)

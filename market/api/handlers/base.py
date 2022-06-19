import logging

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_urldispatcher import View
from asyncpgsa import PG

from api.schema import build_tree_json, edit_json_to_answer, get_item_tree, get_obj_tree_by_id, get_total_price, \
    SQL_REQUESTS

log = logging.getLogger(__name__)


class BaseView(View):
    URL_PATH: str

    @property
    def pg(self) -> PG:
        log.debug('Registering handler %r as %r', self.request.app.keys(), type(self.request.app))
        return self.request.app['pg']

    async def get_obj(self, query):
        return await self.pg.fetchrow(query)


class BaseImportView(BaseView):
    @property
    def shop_unit_id(self):
        return str(self.request.match_info.get('shop_unit_id'))

    async def get_obj_tree(self):
        """
        :return:
        """

        return await get_obj_tree_by_id(self.shop_unit_id, self.pg)

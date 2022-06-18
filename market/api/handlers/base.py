import logging

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_urldispatcher import View
from asyncpgsa import PG

from api.schema import build_tree_json, edit_json_to_answer, get_item_tree, get_total_price, SQL_REQUESTS

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

    async def get_relative_ides(self, shop_unit_id):
        return await get_item_tree(shop_unit_id, self.pg)

    async def get_obj_tree(self):
        """
        :return:
        """

        ides_to_req, ides = await self.get_relative_ides(self.shop_unit_id)
        if not ides_to_req:
            raise HTTPNotFound()

        sql_request = SQL_REQUESTS['get_by_ides'].format(tuple(ides_to_req))

        records = await self.pg.fetch(sql_request)
        records = {record.get('shop_unit_id'): dict(record) for record in records}
        ans = records.get(self.shop_unit_id)

        await build_tree_json(ans, ides, records)
        await get_total_price(ans)

        return await edit_json_to_answer(ans)

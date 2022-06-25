from http import HTTPStatus
from urllib.parse import parse_qs, unquote, urlparse

from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_response import Response
from aiohttp_apispec import docs
from sqlalchemy import and_, select

from market.db.schema import history_table, shop_units_table
from market.api.utils import datetime_to_str, edit_json_to_answer, str_to_datetime
from market.api.handlers.base import BaseImportView


class StatsView(BaseImportView):
    URL_PATH = r'/node/{shop_unit_id:[\w, -]+}/statistic'

    @docs(summary='Отобразить историю изменения товара')
    async def get(self) -> Response:
        """
        :return: Response
        Метод получения истории изменений элемента, цена которых менялась с date_start до date_end
        """

        # парсим url
        kwargs = parse_qs(urlparse(unquote(str(self.request.url))).query)

        try:
            date_end = str_to_datetime(kwargs['dateEnd'][0])
            date_start = str_to_datetime(kwargs['dateStart'][0])
        except (ValueError, KeyError):
            return Response(status=HTTPStatus.BAD_REQUEST)

        # sql получения истории обновления цены товара/категории
        sql_request = select(history_table).where(
            and_(
                date_start <= history_table.c.update_date,
                history_table.c.update_date <= date_end,
                history_table.c.shop_unit_id == self.shop_unit_id
            )
        )

        prices = [[record.get('price'), record.get('update_date')] for record in await self.pg.fetch(sql_request)]

        # получаем сам объект и добавляем его историю в обновлений
        ans = await self.pg.fetchrow(
            shop_units_table.select().where(shop_units_table.c.shop_unit_id == self.shop_unit_id))
        ans = ans and dict(ans)
        if ans is None:
            raise HTTPNotFound()
        del ans['date']
        ans['stats'] = [{'update_date': datetime_to_str(update_date), 'price': price} for price, update_date in prices]
        ans['price'] = ans['stats'][-1]['price'] if ans['stats'] else None

        return Response(body=await edit_json_to_answer(ans))

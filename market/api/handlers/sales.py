from datetime import timedelta
from http import HTTPStatus
from json import dumps
from urllib.parse import parse_qs, unquote, urlparse

from aiohttp.web_response import Response
from aiohttp_apispec import docs
from sqlalchemy import and_, select

from market.db.schema import history_table, shop_units_table
from market.api.utils import datetime_to_str, edit_json_to_answer, str_to_datetime
from market.api.handlers.base import BaseImportView


class SalesView(BaseImportView):
    URL_PATH = r'/sales'

    @docs(summary='Отобразить товары со скидкой')
    async def get(self) -> Response:
        """
        :return: Response
        Метод получения элемента (-ов), цена которых менялась за последние 24 часа
        """

        try:
            date = str_to_datetime(parse_qs(urlparse(unquote(str(self.request.url))).query)['date'][0])
        except (ValueError, KeyError):
            return Response(status=HTTPStatus.BAD_REQUEST)

        sql_request = shop_units_table.select().where(
            and_(
                shop_units_table.c.type == 'offer',
                shop_units_table.c.shop_unit_id.in_(
                    select(history_table.c.shop_unit_id).where(
                        and_(
                            history_table.c.update_date >= date - timedelta(days=1),
                            history_table.c.update_date <= date,
                        )
                    )
                )
            )
        )

        data = list(map(dict, await self.pg.fetch(sql_request)))
        for record in data:
            record['date'] = datetime_to_str(record['date'])
        return Response(body=dumps(await edit_json_to_answer(data)))

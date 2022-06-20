from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse, unquote

from aiohttp.web_response import Response
from aiohttp_apispec import docs, response_schema
from sqlalchemy import and_, select

from db.schema import history_table, shop_units_table
from market.api.schema import datetime_to_str, ShopUnitResponseSchema, str_to_datetime
from .base import BaseImportView


class SalesView(BaseImportView):
    URL_PATH = r'/sales'

    @docs(summary='Отобразить товары со скидкой')
    @response_schema(ShopUnitResponseSchema())
    async def get(self):
        date = str_to_datetime(parse_qs(urlparse(unquote(str(self.request.url))).query)['date'][0])
        sql_request = shop_units_table.select().where(
            and_(
                shop_units_table.c.type == 'offer',
                shop_units_table.c.shop_unit_id.in_(
                    select(history_table.c.shop_unit_id).where(
                        history_table.c.update_date >= date - timedelta(days=1)
                    )
                )
            )
        )

        data = list(map(dict, await self.pg.fetch(sql_request)))
        for record in data:
            record['date'] = datetime_to_str(record['date'])

        return Response(body={'sales': data})

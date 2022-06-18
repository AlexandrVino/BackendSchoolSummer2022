from http import HTTPStatus
from itertools import groupby

from aiohttp.web_response import Response
from aiohttp_apispec import docs, response_schema
from sqlalchemy import Integer, and_, cast, func, select

from market.api.schema import ShopUnitStatisticResponseSchema
from market.db.schema import (
    shop_units_table as shop_units_t, relations_table as relations_t,
)

from .base import BaseImportView


class ShopUnitDateView(BaseImportView):
    URL_PATH = r'/imports/{import_id:\d+}/shop_unit/statistic'

    @docs(summary='Статистика по месяцам')
    @response_schema(ShopUnitStatisticResponseSchema(), code=HTTPStatus.OK.value)
    async def get(self):
        await self.check_import_exists()

        # В задании требуется, чтобы ключами были номера месяцев
        # (без ведущих нулей, "01" -> 1).
        month = func.date_part('month', shop_units_t.c.birth_date)
        month = cast(month, Integer).label('month')

        query = select([
            month,
            relations_t.c.citizen_id,
            func.count(relations_t.c.relative_id).label('presents')
        ]).select_from(
            relations_t.join(
                shop_units_t, and_(
                    shop_units_t.c.import_id == relations_t.c.import_id,
                    shop_units_t.c.citizen_id == relations_t.c.relative_id
                )
            )
        ).group_by(
            month,
            relations_t.c.import_id,
            relations_t.c.citizen_id
        ).where(
            relations_t.c.import_id == self.import_id
        )
        rows = await self.pg.fetch(query)

        result = {i: [] for i in range(1, 13)}
        for month, rows in groupby(rows, key=lambda row: row['month']):
            for row in rows:
                result[month].append({'citizen_id': row['citizen_id'],
                                      'presents': row['presents']})
        return Response(body={'data': result})

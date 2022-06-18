import logging
from http import HTTPStatus
from typing import Generator

from aiohttp.web_response import Response
from aiohttp_apispec import docs, request_schema, response_schema
from aiomisc import chunk_list
from asyncpg import UniqueViolationError
from sqlalchemy import select, Table, update
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Query

from market.api.schema import ImportResponseSchema, ImportSchema, update_parent_branch_date
from market.db.schema import shop_units_table, relations_table
from market.utils.pg import MAX_QUERY_ARGS

from .base import BaseView
from ..validators import validate_all_items

log = logging.getLogger(__name__)


class ImportsView(BaseView):
    URL_PATH = '/imports'
    # Так как данных может быть много, а postgres поддерживает только
    # MAX_QUERY_ARGS аргументов в одном запросе, писать в БД необходимо
    # частями.
    # Максимальное кол-во строк для вставки можно рассчитать как отношение
    # MAX_QUERY_ARGS к кол-ву вставляемых в таблицу столбцов.

    MAX_CITIZENS_PER_INSERT = MAX_QUERY_ARGS // len(shop_units_table.columns)
    MAX_RELATIONS_PER_INSERT = MAX_QUERY_ARGS // len(relations_table.columns)
    all_insert_data = None
    need_to_update_date = None

    @classmethod
    def make_shop_units_table_rows(cls, shop_units, date) -> Generator:
        """
        Генерирует данные готовые для вставки в таблицу citizens (с ключом
        import_id и без ключа relatives).
        """
        for shop_unit in shop_units:
            yield {
                'shop_unit_id': shop_unit['id'],
                'name': shop_unit['name'],
                'date': date,
                'type': shop_unit['type'].lower(),
                'parent_id': shop_unit.get('parentId'),
                'price': shop_unit.get('price'),
            }

    @classmethod
    def make_relations_table_rows(cls, shop_units) -> Generator:
        """
        Генерирует данные готовые для вставки в таблицу relations.
        """
        for shop_unit in shop_units:
            if not shop_unit.get('parentId'):
                continue
            yield {
                'children_id': shop_unit['id'],
                'relation_id': shop_unit['parentId'],
            }

    async def add_relatives(self, conn, chunk):
        try:
            query = relations_table.insert()
            query.parameters = chunk[0].values()
            await conn.execute(query.values(list(chunk)))
        except UniqueViolationError:
            pass

    async def update_or_create(self, conn, query: Query, chunk: list[dict], table: Table):
        """
        Метод, который добавляет/измеяет объект в бд
        """

        data = chunk[0]

        if self.all_insert_data.get(data['shop_unit_id']) is None:
            self.all_insert_data[data['shop_unit_id']] = data.copy()

        select_query = select(table)
        obj = await self.get_obj(select_query.where(table.c.shop_unit_id == data['shop_unit_id']))

        if obj is None:
            await conn.execute(query.values(list(chunk)))

        else:
            assert obj.get('type') == data[
                'type'].lower(), f"Incorrect obj with id {data['shop_unit_id']} type in request"

            query = table.update().values(**data).where(table.c.shop_unit_id == data['shop_unit_id'])
            query.parameters = data.values()
            await conn.execute(query)

        parent = self.all_insert_data.get(data.get('parent_id'))

        if not parent and data.get('parent_id'):
            parent = await self.get_obj(select_query.where(table.c.shop_unit_id == data.get('parent_id')))
            if parent:
                self.all_insert_data[data.get('parent_id')] = parent

        assert not parent and not data.get('parent_id') or parent.get('type') == 'category', 'Validation failed'

        if parent:
            self.need_to_update_date.append((data['shop_unit_id'], data['date']))

    @docs(summary='Добавить выгрузку с информацией о товарах/категориях')
    @request_schema(ImportSchema())
    @response_schema(ImportResponseSchema(), code=HTTPStatus.CREATED.value)
    async def post(self):

        self.all_insert_data = {}
        self.need_to_update_date = []

        async with self.pg.transaction() as conn:

            data = await self.request.json()
            shop_units = data['items']

            chunked_shop_unit_rows = list(
                chunk_list(
                    self.make_shop_units_table_rows(shop_units, data['updateDate']), self.MAX_CITIZENS_PER_INSERT
                )
            )
            relations_rows = list(
                chunk_list(
                    self.make_relations_table_rows(shop_units), self.MAX_CITIZENS_PER_INSERT
                )
            )

            await validate_all_items(chunked_shop_unit_rows)

            shop_units_query = shop_units_table.insert()

            for chunk in chunked_shop_unit_rows:
                shop_units_query.parameters = chunk[0].values()
                await self.update_or_create(conn, shop_units_query, chunk, shop_units_table)

            for chunk in relations_rows:
                await self.add_relatives(conn, chunk)

        for children_id, date in self.need_to_update_date:
            await update_parent_branch_date(children_id, self.pg, date)

        return Response(status=HTTPStatus.OK)

import logging
from http import HTTPStatus
from typing import Generator

from aiohttp.web_response import Response
from aiohttp_apispec import docs, request_schema
from aiomisc import chunk_list
from asyncpg import Connection
from sqlalchemy.dialects.postgresql import insert

from market.api.handlers.base import BaseView
from market.api.schema import ImportSchema
from market.api.utils import add_history, SQL_REQUESTS, str_to_datetime, update_parent_branch_date
from market.api.validators import validate_all_items
from market.db.schema import relations_table, shop_units_table
from market.utils.pg import MAX_QUERY_ARGS

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
    need_to_add_history = None

    @classmethod
    def make_shop_units_table_rows(cls, shop_units: list[dict], date: str) -> Generator:
        """
        :param shop_units: список элементов для вставки
        :param date: дата обновления
        :return: Generator

        Метод, который генерирует данные готовые для вставки в таблицу shop_units
        """

        for shop_unit in shop_units:
            yield {
                'shop_unit_id': shop_unit['id'],
                'name': shop_unit['name'],
                'date': str_to_datetime(date),
                'type': shop_unit['type'].lower(),
                'parent_id': shop_unit.get('parentId'),
                'price': shop_unit.get('price'),
            }

    @classmethod
    def make_relations_table_rows(cls, relations: list[dict]) -> Generator:
        """
        :param relations: список словарей для вставки
        :return: Generator

        Метод, который генерирует данные готовые для вставки в таблицу relations
        """

        for shop_unit in relations:
            if not shop_unit.get('parentId'):
                continue
            yield {
                'children_id': shop_unit['id'],
                'relation_id': shop_unit['parentId'],
            }

    @staticmethod
    async def add_relatives(conn: Connection, chunk: list[dict]) -> None:
        """
        :param conn: объект коннекта к бд
        :param chunk список элементов для вставки
        :return: None

        Метод, который вставляет данные в таблицу relations
        """

        query = insert(relations_table).on_conflict_do_nothing(index_elements=['relation_id', 'children_id'])
        query.parameters = []

        await conn.execute(query.values(list(chunk)))

    async def update_or_create(self, conn: Connection, chunk: list[dict]):
        """
        :param conn: объект коннекта к бд
        :param chunk список элементов для вставки
        :return: None

        Метод, который вставляет данные в таблицу shop_units
        """

        parents = set()
        all_objects = dict()

        for data in chunk:
            if data.get('parent_id'):
                parents.add(data.get('parent_id'))
            all_objects[data.get('shop_unit_id')] = data.copy()

        # проверяем, что родитель есть в бд и что его тип == 'category'
        if parents:
            for parent in await self.pg.fetch(SQL_REQUESTS['get_by_ides'].format(tuple(parents)).replace(',)', ')')):
                assert parent is not None and parent.get('type').lower() == 'category', \
                    f'Incorrect parent with id {parent.get("shop_unit_id")} (Not found in db or type is OFFER)'

        for data in chunk:

            # т.к. при изменении/добавлении товара необходимо менять всю родительскую ветку (дату и цену)
            # добавляю в 2 списка:
            # первый для установления даты на дату последнего измененного объекта
            # второй для добавления записи в таблицу истории изменений (статистики)

            if data.get('parent_id'):
                self.need_to_update_date.append((data['shop_unit_id'], data['date']))
            if data.get('type').lower() == 'offer':
                self.need_to_add_history.append((data['shop_unit_id'], data['date']))

        # добавляем объекты, которых еще нет в бд
        insert_query = insert(shop_units_table).values(list(all_objects.values())).on_conflict_do_update(
            index_elements=['shop_unit_id'],
            set_=shop_units_table.columns
        )
        insert_query.parameters = []

        await conn.execute(insert_query)

    @docs(summary='Добавить выгрузку с информацией о товарах/категориях')
    @request_schema(ImportSchema())
    async def post(self) -> Response:
        """
        :return: Response
        Метод добавления/изменения элемента (ов)
        """
        try:
            self.all_insert_data = {}
            self.need_to_update_date = []
            self.need_to_add_history = []

            async with self.pg.transaction() as conn:

                data = await self.request.json()
                assert data.get('items') and data.get('updateDate')
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

                validate_all_items(chunked_shop_unit_rows)

                for chunk in chunked_shop_unit_rows:
                    await self.update_or_create(conn, chunk)
                for chunk in relations_rows:
                    await self.add_relatives(conn, chunk)

            for children_id, date in self.need_to_update_date:
                await update_parent_branch_date(children_id, self.pg, date)
            for children_id, date in self.need_to_add_history:
                await add_history(children_id, self.pg, date, {})

            return Response(status=HTTPStatus.OK)
        except (AssertionError, ValueError) as err:
            return Response(body=str(err), status=HTTPStatus.BAD_REQUEST)

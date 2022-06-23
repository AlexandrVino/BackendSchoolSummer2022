import json
from datetime import datetime

from aiohttp.web_exceptions import HTTPNotFound
from aiomisc import chunk_list
from asyncpg import Record
from asyncpgsa import PG
from sqlalchemy.dialects.postgresql import insert

from market.db.schema import history_table
from market.utils.pg import MAX_QUERY_ARGS

''' 
Пишу ручками некоторые запросы т.к. 
1) либо sqlalchemy генерит что-то а потом на это и ругается, (при использовании Функции "_in" )
2) либо это обновление множества записей сразу (не нагуглил нормальное решение :/ )
3) либо это рекурсивный запрос, который легче написать ручками
'''

SQL_REQUESTS = {
    'get_by_ides': '''
        SELECT shop_units.shop_unit_id, shop_units.name, shop_units.date, shop_units.parent_id, shop_units.type, shop_units.price 
        FROM shop_units 
        WHERE shop_units.shop_unit_id IN {}''',
    'delete_by_ides': '''
    DELETE FROM public.history WHERE shop_unit_id IN {};
    DELETE FROM public.shop_units WHERE shop_unit_id IN {};
    DELETE FROM public.relations WHERE children_id IN {} OR relation_id IN {};''',
    'get_item_tree': '''
    WITH RECURSIVE search_tree(relation_id, children_id) AS (
        SELECT t.relation_id, t.children_id
        FROM relations t WHERE relation_id = '{}'
      UNION ALL
        SELECT t.relation_id, t.children_id
        FROM relations t, search_tree st
        WHERE t.relation_id = st.children_id
    )   
    SELECT * FROM search_tree;''',
    'get_parent_brunch': '''
    WITH RECURSIVE search_tree(relation_id, children_id) AS (
        SELECT t.relation_id, t.children_id
        FROM relations t WHERE children_id = '{}'
      UNION ALL
        SELECT t.relation_id, t.children_id
        FROM relations t, search_tree st
        WHERE t.children_id = st.relation_id
    )
    SELECT * FROM search_tree;''',
    'update_date': '''
    UPDATE shop_units 
    SET date = '{}'
    WHERE shop_units.shop_unit_id IN {}''',
}


async def get_item_tree(root_id, pg: PG) -> tuple[set[str], list[Record]] | tuple[None, None]:
    """
    :param root_id: id корневого элемента дерева
    :param pg: PG объект коннекта к базе данных
    :return:
    """

    sql_request = SQL_REQUESTS['get_item_tree'].format(root_id)

    ides = await pg.fetch(sql_request)

    if not ides:
        return None, None

    ides_to_req = {root_id}

    for record in ides:
        ides_to_req.update(record.values())

    return ides_to_req, ides


async def build_tree_json(ans: dict, data: list[Record], records: dict[str: dict]) -> None:
    """
    :param ans: Словарь-ответ
    :param data: список рекордов для построения дерева (таблица связей)
    :param records: словарь id: record для удобства
    :return: None

    Функция построения базового дерева ответа
    """

    while data:

        # костыль во избежание зацикливания
        # если среди дочерних элементов текущего нет не одного из data
        # мы прерываем цикл
        any_children_in_data = True

        for index, record in enumerate(data):
            parent_id, children_id = record.get('relation_id'), record.get('children_id')
            if parent_id != ans['shop_unit_id']:
                continue

            any_children_in_data = False
            data.pop(index)

            if ans.get('children') is None:
                ans['children'] = []
            ans['children'].append(records[children_id])

            await build_tree_json(ans['children'][-1], data, records)

        if any_children_in_data:
            break


async def get_total_price(tree: dict) -> tuple[int, int] | None:
    """
    :param tree: дерево элементов
    :return: либо цену и кол-во дочерних элементов (для рекурсии), либо None

    Функция, считающая и добавляющая цены категории
    """

    price = tree.get('price') or 0
    count = 0

    if tree.get('children'):
        for item in tree['children']:
            if item.get('children'):

                local_price, local_count = await get_total_price(item)
                item['price'] = local_price // local_count
                price += local_price
                count += local_count
            else:
                item['children'] = None
                price += 0 if item.get('price') is None else item.get('price')
                count += 1
            item['date'] = datetime_to_str(item['date'])
        tree['price'] = price // count
    else:
        tree['date'] = datetime_to_str(tree['date'])
        tree['children'] = None
    return price, count or 1


async def edit_json_to_answer(data: dict | list) -> dict:
    """
    :param data: данные для запросов
    :return: данные, подготовленные для отправки

    Функция изменения данных для ответа
    """
    return json.loads(
        json.dumps(data, ensure_ascii=False).replace('category', 'CATEGORY').replace('offer', 'OFFER')
        .replace('shop_unit_id', 'id').replace('parent_id', 'parentId')
    )


async def get_obj_tree_by_id(shop_unit_id: str, pg: PG) -> dict:
    """
    :param shop_unit_id: id элемента, для которого надо создать дерево
    :param pg: PG объект коннекта к базе данных
    :return: json, готовый к отправке на клиент

    Функция, возвращающая json, готовый к отправке на клиент
    """

    ides_to_req, ides = await get_item_tree(shop_unit_id, pg)
    if not ides_to_req:
        raise HTTPNotFound()

    sql_request = SQL_REQUESTS['get_by_ides'].format(tuple(ides_to_req)).replace(',)', ')')
    records = await pg.fetch(sql_request)
    records = {record.get('shop_unit_id'): dict(record) for record in records}
    ans = records.get(shop_unit_id)
    ans['date'] = datetime_to_str(ans['date'])

    await build_tree_json(ans, ides, records)
    await get_total_price(ans)

    return await edit_json_to_answer(ans)


async def get_history(obj_tree: dict, update_date: datetime, ides: list, data: dict):
    """
    :param obj_tree: дерево элементов, для которых необходимо получить историю
    :param update_date: время обновления
    :param ides: список айдишников для запроса
    :param data: словарь историй
    :return: None

    Функция, которая рекурсивно собирает историю
    """

    record = ides.pop(0)
    parent_id, children_id = record.get('relation_id'), record.get('children_id')
    data[parent_id] = {
        'price': obj_tree['price'],
        'date': update_date
    }

    for children in obj_tree['children']:
        if children.get('id') != children_id:
            continue
        if not ides:
            data[children_id] = {
                'price': children['price'],
                'date': update_date
            }
        else:
            await get_history(children, update_date, ides, data)


async def get_parent_brunch_ides(children_id: str, pg: PG) -> list[Record]:
    """
    :param children_id: id дочернего элемента в ветке
    :param pg: PG объект коннекта к базе данных
    :return: список рекордов

    Функция, возвращающая объекты из всей родительской ветки
    """

    return await pg.fetch(SQL_REQUESTS['get_parent_brunch'].format(children_id))


def get_history_table_chunk(prices: dict) -> None:
    """
    :param prices: Словарь со входными данными
    :return: None

    Функция генерации данных для записи в таблицу историй
    """

    for obj_id, obj_data in prices.items():
        yield {
            'shop_unit_id': obj_id,
            'update_date': obj_data.get('date'),
            'price': obj_data.get('price'),
        }


async def add_history(children_id: str, pg: PG, update_date: datetime, main_parents_trees: dict) -> None:
    """
    :param children_id: id дочернего элемента в ветке
    :param pg: PG объект коннекта к базе данных
    :param update_date: время обновления
    :param main_parents_trees: словарь, чтобы не запрашивать историю и дерево объекта несколько раз
    :return: None

    Функция вычисления и добавления истории объекту
    """

    ides = await get_parent_brunch_ides(children_id, pg)
    main_parent_id = ides and ides[-1].get('relation_id')

    if main_parents_trees.get(main_parent_id) is None:
        prices = {}
        main_parent_tree = await get_obj_tree_by_id(main_parent_id or children_id, pg)
        await get_history(main_parent_tree, update_date, ides[::-1], prices)

        main_parents_trees[main_parent_id] = [main_parent_tree, prices]

        sql_request = insert(history_table).on_conflict_do_nothing(
            index_elements=['shop_unit_id', 'update_date']
        )
        sql_request.parameters = []

        history_rows = list(chunk_list(get_history_table_chunk(prices), MAX_QUERY_ARGS // 3))
        for chunk in history_rows:
            await pg.execute(sql_request.values(chunk))


async def update_parent_branch_date(children_id: str, pg: PG, update_date: datetime) -> None:
    """
    :param children_id: id дочернего элемента в ветке
    :param pg: PG объект коннекта к базе данных
    :param update_date: время обновления
    :return: None

    Функция обновляет дату во всей родительской ветке
    """

    ides = await get_parent_brunch_ides(children_id, pg)
    ides_to_req = set()

    for record in ides:
        ides_to_req.update((record.get('relation_id'), record.get('children_id')))

    sql_request = SQL_REQUESTS['update_date'].format(update_date, tuple(ides_to_req))
    await pg.execute(sql_request)


def datetime_to_str(date: datetime) -> str:
    """
    :param date: datetime объект
    :return: дата и время строкой

    Функция перевода datetime объект в строку
    """

    return date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + 'Z'


def str_to_datetime(date: str) -> datetime:
    """
    :param date: дата и время строкой
    :return: datetime объект

    Функция перевода строки в datetime объект
    """

    return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")

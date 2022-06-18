'''
Модуль содержит схемы для валидации данных в запросах и ответах.

Схемы валидации запросов используются в бою для валидации данных отправленных
клиентами.

Схемы валидации ответов *ResponseSchema используются только при тестировании,
чтобы убедиться что обработчики возвращают данные в корректном формате.
'''
import json

from asyncpg import Record
from asyncpgsa import PG
from marshmallow import Schema, validates_schema, ValidationError
from marshmallow.fields import Dict, Int, Nested, Str
from marshmallow.validate import Length, Range

BIRTH_DATE_FORMAT = '%d.%m.%Y'

''' 
Пишу ручками некоторые запросы т.к. 
1) либо sqlalchemy генерит что-то а потом на это и ругается, 
2) либо это рекурсивный запрос, который легче написать ручками
'''

SQL_REQUESTS = {
    'get_by_ides': '''
        SELECT shop_units.shop_unit_id, shop_units.name, shop_units.date, shop_units.parent_id, shop_units.type, shop_units.price 
        FROM shop_units 
        WHERE shop_units.shop_unit_id IN {}''',
    'delete_by_ides': '''
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
    'add_history': ''''''
}


class GetShopUnitSchema(Schema):
    shop_unit_id = Str(validate=Length(min=1, max=256))


class ShopUnitSchema(GetShopUnitSchema):
    shop_unit_id = Str(validate=Length(min=1, max=256))


class ImportSchema(Schema):
    shop_units = Nested(
        ShopUnitSchema(), many=True, required=True, validate=Length(max=10000)
    )

    @validates_schema
    def validate_unique_shop_unit_id(self, data, **_):
        shop_unit_ids = set()
        for shop_unit in data['shop_units']:
            if shop_unit['shop_unit_id'] in shop_unit_ids:
                raise ValidationError(
                    'shop_unit_id %r is not unique' % shop_unit['shop_unit_id']
                )
            shop_unit_ids.add(shop_unit['shop_unit_id'])

    @validates_schema
    def validate_relatives(self, data, **_):
        children = {
            shop_unit['shop_unit_id']: set(shop_unit['shop_unit_id'])
            for shop_unit in data['shop_units']
        }

        for shop_unit_id, children_ids in children.items():
            for children_id in children_ids:
                if shop_unit_id not in children.get(children_id, set()):
                    raise ValidationError(
                        f'shop_unit {children_id} does not have children with {shop_unit_id}'
                    )


class ImportIdSchema(Schema):
    import_id = Int(strict=True, required=True)


class ImportResponseSchema(Schema):
    data = Nested(ImportIdSchema(), required=True)


class ShopUnitResponseSchema(Schema):
    data = Nested(ShopUnitSchema(many=True), required=True)


class GetShopUnitResponseSchema(Schema):
    data = Nested(ShopUnitSchema(), required=True)


class StatisticSchema(Schema):
    citizen_id = Int(validate=Range(min=0), strict=True, required=True)
    presents = Int(validate=Range(min=0), strict=True, required=True)


# Схема, содержащая кол-во подарков, которое купят жители по месяцам.
# Чтобы не указывать вручную 12 полей класс можно сгенерировать.
ShopUnitStatisticByMonthSchema = type(
    'ShopUnitStatisticByMonthSchema', (Schema,),
    {
        str(i): Nested(StatisticSchema(many=True), required=True)
        for i in range(1, 13)
    }
)


class ShopUnitStatisticResponseSchema(Schema):
    data = Nested(ShopUnitStatisticByMonthSchema(), required=True)


class ErrorSchema(Schema):
    code = Str(required=True)
    message = Str(required=True)
    fields = Dict()


class ErrorResponseSchema(Schema):
    error = Nested(ErrorSchema(), required=True)


async def get_item_tree(root_id, pg: PG):
    sql_request = SQL_REQUESTS['get_item_tree'].format(root_id)

    ides = await pg.fetch(sql_request)
    if not ides:
        return None, None
    ides_to_req = {root_id}

    for record in ides:
        parent_id, children_id = record.get('relation_id'), record.get('children_id')
        ides_to_req.update((parent_id, children_id))

    return ides_to_req, ides


async def build_tree_json(ans, data: list[Record], tree: dict[str: Record]):
    while data:

        flag = True

        for index, record in enumerate(data):
            parent_id, children_id = record.get('relation_id'), record.get('children_id')
            if parent_id != ans['shop_unit_id']:
                continue

            flag = False
            data.pop(index)
            if ans.get('children') is None:
                ans['children'] = []
            ans['children'].append(tree[children_id])
            await build_tree_json(ans['children'][-1], data, tree)

        if flag:
            break


async def get_total_price(tree):
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
        tree['price'] = price // count
    if not tree.get('children'):
        tree['children'] = None
    return price, count or 1


async def edit_json_to_answer(data: dict) -> dict:
    return json.loads(
        json.dumps(data, ensure_ascii=False).replace('category', 'CATEGORY').replace('offer', 'OFFER')
        .replace('shop_unit_id', 'id').replace('parent_id', 'parentId')
    )


async def update_parent_branch_date(children_id, pg: PG, update_date):
    sql_request = SQL_REQUESTS['get_parent_brunch'].format(children_id)

    ides = await pg.fetch(sql_request)
    ides_to_req = set()

    for record in ides:
        ides_to_req.update((record.get('relation_id'), record.get('children_id')))

    sql_request = SQL_REQUESTS['update_date'].format(update_date, tuple(ides_to_req))

    await pg.execute(sql_request)

from sqlalchemy import and_, func, select

from market.db.schema import shop_units_table, relations_table


CITIZENS_QUERY = select([
    shop_units_table.c.shop_unit_id,
    shop_units_table.c.name,
    shop_units_table.c.date,
    shop_units_table.c.type,
    shop_units_table.c.price,
    # В результате LEFT JOIN у жителей не имеющих родственников список
    # relatives будет иметь значение [None]. Чтобы удалить это значение
    # из списка используется функция array_remove.
    func.array_remove(
        func.array_agg(relations_table.c.children_id),
        None
    ).label('relatives')
]).group_by(
    shop_units_table.c.shop_unit_id
)

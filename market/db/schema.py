from enum import Enum, unique

from sqlalchemy import (
    Column, Date, Enum as PgEnum, ForeignKey, ForeignKeyConstraint, Integer,
    MetaData, String, Table, UniqueConstraint, DateTime
)

convention = {
    'all_column_names': lambda constraint, table: '_'.join([
        column.name for column in constraint.columns.values()
    ]),
    'ix': 'ix__%(table_name)s__%(all_column_names)s',  # Именование индексов
    'uq': 'uq__%(table_name)s__%(all_column_names)s',  # Именование уникальных индексов
    'ck': 'ck__%(table_name)s__%(constraint_name)s',  # Именование CHECK-constraint-ов
    'fk': 'fk__%(table_name)s__%(all_column_names)s__%(referred_table_name)s',  # Именование внешних ключей
    'pk': 'pk__%(table_name)s'  # Именование первичных ключей
}
metadata = MetaData(naming_convention=convention)


@unique
class ShopUnitType(Enum):
    category = 'CATEGORY'
    offer = 'OFFER'


shop_units_table = Table(
    'shop_units',
    metadata,
    Column('shop_unit_id', String, primary_key=True),
    Column('name', String, nullable=False, index=True),
    Column('date', DateTime, nullable=False),
    Column('parent_id', String, nullable=True),
    Column('type', PgEnum(ShopUnitType, name='type'), nullable=False),
    Column('price', Integer, nullable=True),
)

relations_table = Table(
    'relations',
    metadata,
    Column('relation_id', String, primary_key=True),
    Column('children_id', String, primary_key=True),

    UniqueConstraint('relation_id', 'children_id', name='uix_1')
)

history_table = Table(
    'history',
    metadata,

    Column('shop_unit_id', String, nullable=False),
    Column('update_date', DateTime, nullable=False),
    Column('price', Integer, nullable=False),

    UniqueConstraint('shop_unit_id', 'update_date', 'price', name='uix_2')
)

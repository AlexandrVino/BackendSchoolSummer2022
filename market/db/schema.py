from enum import Enum, unique

from sqlalchemy import (
    Column, Date, Enum as PgEnum, ForeignKey, ForeignKeyConstraint, Integer,
    MetaData, String, Table,
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


imports_table = Table(
    'imports',
    metadata,
    Column('import_id', Integer, primary_key=True)
)

citizens_table = Table(
    'shop_units',
    metadata,
    Column('import_id', Integer, ForeignKey('imports.import_id'), primary_key=True),
    Column('shop_unit_id', Integer, primary_key=True),
    Column('name', String, nullable=False, index=True),
    Column('date', String, nullable=False),
    Column('type', PgEnum(ShopUnitType, name='gender'), nullable=False),
    Column('price', Integer, nullable=True),
)

relations_table = Table(
    'relations',
    metadata,
    Column('import_id', Integer, primary_key=True),
    Column('shop_unit_id', Integer, primary_key=True),
    Column('parent_id', Integer, primary_key=True),
    Column('children_id', Integer, primary_key=True),
    ForeignKeyConstraint(
        ('import_id', 'shop_unit_id'),
        ('shop_units.import_id', 'shop_units.citizen_id')
    ),
    ForeignKeyConstraint(
        ('import_id', 'shop_unit_id'),
        ('shop_units.import_id', 'shop_units.citizen_id')
    ),
)

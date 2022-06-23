from marshmallow import Schema, validates_schema, ValidationError
from marshmallow.fields import Dict, Int, Nested, Str
from marshmallow.validate import Length

from market.api.validators import validate_all_items


class ShopUnitSchema(Schema):
    id = Str(validate=Length(min=1, max=256))
    name = Str(validate=Length(min=1, max=256))
    date = Str(validate=Length(min=1, max=256))
    parentId = Str(validate=Length(min=1, max=256))
    type = Str(validate=Length(min=1, max=256))
    price = Int()


class ImportSchema(Schema):
    shop_units = Nested(
        ShopUnitSchema(), many=True, required=True, validate=Length(max=10000)
    )

    @validates_schema
    def validate_unique_shop_unit_id(self, data, **_):
        shop_unit_ids = set()
        validate_all_items(data['items'])

        for shop_unit in data['items']:
            if shop_unit['shop_unit_id'] in shop_unit_ids:
                raise ValidationError(
                    'shop_unit_id %r is not unique' % shop_unit['shop_unit_id']
                )
            shop_unit_ids.add(shop_unit['shop_unit_id'])


class ErrorSchema(Schema):
    code = Str(required=True)
    message = Str(required=True)
    fields = Dict()

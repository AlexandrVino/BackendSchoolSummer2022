from typing import Generator


def validate_category(kwargs):
    keys = [
        ('shop_unit_id', (1,)), ('name', (1,)), ('date', (1,)), ('parentId', (0, 1)), ('type', (1,)), ('price', (0,))
    ]
    for key, value in keys:
        assert bool(kwargs.get(key)) in value, 'Validation failed'


def validate_product(kwargs):
    keys = [
        ('shop_unit_id', (1,)), ('name', (1,)), ('date', (1,)), ('parentId', (0, 1)), ('type', (1,)), ('price', (1,))
    ]
    for key, value in keys:
        assert bool(kwargs.get(key)) in value, 'Validation failed'


def validate_all_items(items: iter):
    funcs = {
        'category': validate_category,
        'offer': validate_product,
    }

    for item in items:
        funcs[item[0]['type']](item[0])

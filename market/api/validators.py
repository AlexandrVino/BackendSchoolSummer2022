def validate_category(kwargs):
    """
    :param kwargs: словарь
    :return: None

    Функция проверки категорий на валидность
    """

    keys = [
        ('shop_unit_id', (1,)), ('name', (1,)), ('date', (1,)), ('parentId', (0, 1)), ('type', (1,)), ('price', (0,))
    ]
    for key, value in keys:
        assert bool(kwargs.get(key)) in value, 'Validation failed'


def validate_product(kwargs):
    """
    :param kwargs: словарь
    :return: None

    Функция проверки товаров на валидность
    """

    keys = [
        ('shop_unit_id', (1,)), ('name', (1,)), ('date', (1,)), ('parentId', (0, 1)), ('type', (1,)), ('price', (1,))
    ]
    for key, value in keys:
        if key == 'price':
            assert kwargs.get(key) >= 0, 'Validation failed'
        assert bool(kwargs.get(key)) in value, 'Validation failed'


def validate_all_items(items: iter):
    """
    :param items: iter-объект
    :return: None

    Функция проверки категорий и товаров на валидность
    """

    funcs = {
        'category': validate_category,
        'offer': validate_product,
    }

    for item in items:
        funcs[item[0]['type']](item[0])

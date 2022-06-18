from .delete import DeleteView
from .nodes import NodeView
from .shop_unit_statistic import ShopUnitDateView
from .sales import SalesView
from .imports import ImportsView

HANDLERS = (
    ShopUnitDateView, SalesView, NodeView, ImportsView, DeleteView
)

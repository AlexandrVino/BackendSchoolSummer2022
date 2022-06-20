from .delete import DeleteView
from .nodes import NodeView
from .sales import SalesView
from .imports import ImportsView
from .stats import StatsView

HANDLERS = (
    StatsView, SalesView, NodeView, ImportsView, DeleteView
)

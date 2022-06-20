import logging
from functools import partial
from types import AsyncGeneratorType, MappingProxyType
from typing import AsyncIterable, Mapping

from aiohttp import PAYLOAD_REGISTRY
from aiohttp.web_app import Application
from configargparse import Namespace

from api.handlers import HANDLERS
from api.payloads import AsyncGenJSONListPayload, JsonPayload
from market.utils.pg import setup_pg

MEGABYTE = 1024 ** 2
MAX_REQUEST_SIZE = 70 * MEGABYTE

log = logging.getLogger(__name__)


def create_app(args: Namespace) -> Application:
    """
    Создает экземпляр приложения, готового к запуску.
    """
    app = Application(client_max_size=MAX_REQUEST_SIZE,
                      #middlewares=[error_middleware, validation_middleware]
                      )

    # Подключение на старте к postgres и отключение при остановке
    app.cleanup_ctx.append(partial(setup_pg, args=args))

    # Регистрация обработчиков
    for handler in HANDLERS:
        log.debug('Registering handler %r as %r', handler, handler.URL_PATH)
        app.router.add_route('*', handler.URL_PATH, handler)

    PAYLOAD_REGISTRY.register(AsyncGenJSONListPayload, (AsyncGeneratorType, AsyncIterable))
    PAYLOAD_REGISTRY.register(JsonPayload, (Mapping, MappingProxyType))

    return app

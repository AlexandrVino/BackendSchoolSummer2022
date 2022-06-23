import logging
import os
from dataclasses import dataclass
from pathlib import Path
from re import match

from aiohttp.web_app import Application
from asyncpgsa import PG
from configargparse import Namespace
from dotenv import load_dotenv

from sqlalchemy import create_engine

CENSORED = '***'

dotenv_path = os.path.join('.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

DEFAULT_PG_URL = create_engine(
    f'postgresql://{os.environ.get("POSTGRES_USER")}:{os.environ.get("POSTGRES_PASSWORD")}@'
    f'{os.environ.get("POSTGRES_HOST")}:{os.environ.get("POSTGRES_PORT")}/{os.environ.get("POSTGRES_DB")}'
)

MAX_QUERY_ARGS = 32767
MAX_INTEGER = 2147483647

PROJECT_PATH = Path(__file__).parent.parent.resolve()

log = logging.getLogger(__name__)


@dataclass
class DataBaseData:
    host: str
    port: int
    user: int
    password: int
    database: int

    @staticmethod
    async def get_from_url(url: str):
        regex = r'postgresql:\/\/\w{1,100}:\w{1,100}@\w{1,100}:\w{1,100}\/\w{1,100}$'
        if not match(regex, url):
            raise TypeError('Url should be: postgresql://user:password@host:port/database')

        url_split: list = url[url.find('://') + 3:].split('@')

        user, password = url_split[0].split(':')
        url_split = url_split[1].split('/')
        database = url_split[1]
        host, port = url_split[0].split(':')

        return DataBaseData(host=host, port=port, user=user, password=password, database=database)

    def __str__(self):
        return str(self.__dict__)


async def setup_pg(app: Application, args: Namespace) -> PG:
    db_info = args.pg_url.with_password(CENSORED)
    log.info('Connecting to database: %s', db_info)

    db_data = await DataBaseData.get_from_url(str(DEFAULT_PG_URL.url))

    app['pg'] = PG()
    await app['pg'].init(**db_data.__dict__)
    await app['pg'].fetchval('SELECT 1')

    log.info('Connected to database %s', db_info)

    try:
        yield
    finally:
        log.info('Disconnecting from database %s', db_info)
        await app['pg'].pool.close()
        log.info('Disconnected from database %s', db_info)

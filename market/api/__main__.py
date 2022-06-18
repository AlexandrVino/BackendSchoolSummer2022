import logging
import os
from sys import argv

from aiohttp import web
from aiohttp.web import run_app
from aiomisc import bind_socket
from configargparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from yarl import URL
from aiomisc.log import basic_config, LogFormat

from api.app import create_app, MAX_REQUEST_SIZE
from market.utils.argparse import clear_environ, positive_int
from utils.pg import DEFAULT_PG_URL
from setproctitle import setproctitle

ENV_VAR_PREFIX = 'MARKET_'

parser = ArgumentParser(
    auto_env_var_prefix=ENV_VAR_PREFIX,
    formatter_class=ArgumentDefaultsHelpFormatter
)

# api group
parser.add_argument(
    '--api-address', default='0.0.0.0',
    help='IPv4/IPv6 address API server would listen on'
)
parser.add_argument(
    '--api-port', type=positive_int, default=8081,
    help='TCP port API server would listen on'
)

# psql serv group
parser.add_argument(
    '--pg-url', type=URL, default=URL(str(DEFAULT_PG_URL.url)),
    help='URL to use to connect to the database'
)

parser.add_argument(
    '--pg-pass', type=str, default='postgres',
    help='Password to use to connect to the database'
)

parser.add_argument(
    '--pg-pool-min-size', type=int, default=0,
    help='Password to use to connect to the database'
)

parser.add_argument(
    '--pg-pool-max-size', type=int, default=MAX_REQUEST_SIZE * 10,
    help='Password to use to connect to the database'
)

# logging group
parser.add_argument(
    '--log-level', default='info',
    choices=('debug', 'info', 'warning', 'error', 'fatal')
)
parser.add_argument(
    '--log-format', default='color',
    choices=LogFormat.choices()
)


def main():
    args = parser.parse_args()

    clear_environ(lambda arg: arg.startswith(ENV_VAR_PREFIX))
    basic_config(args.log_level, args.log_format, buffered=True)

    sock = bind_socket(address=args.api_address, port=args.api_port, proto_name='http')

    # if args.pg_user is not None:
    #     logging.info('Changing user to %r', args.pg_user.pw_name)
    #     os.setgid(args.pg_user.pw_gid)
    #     os.setuid(args.pg_user.pw_uid)

        # В списке процессов намного удобнее видеть название текущего приложения
    setproctitle(os.path.basename(argv[0]))

    app = create_app(args)
    run_app(app, sock=sock)


if __name__ == '__main__':
    main()
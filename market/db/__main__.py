import os
from pathlib import Path

from alembic.config import CommandLine, Config

PROJECT_PATH = Path(__file__).parent.parent.resolve()


def main():
    alembic = CommandLine()
    options = alembic.parser.parse_args()

    # Если указан относительный путь (alembic.ini), добавляем в начало
    # абсолютный путь до приложения
    if not os.path.isabs(options.config):
        options.config = os.path.join(PROJECT_PATH, options.config)

    # Создаем объект конфигурации Alembic
    config = Config(file_=options.config, ini_section=options.name,
                    cmd_opts=options)

    # Подменяем путь до папки с alembic на абсолютный (требуется, чтобы alembic
    # мог найти env.py, шаблон для генерации миграций и сами миграции)
    alembic_location = config.get_main_option('script_location')
    if not os.path.isabs(alembic_location):
        config.set_main_option('script_location',
                               os.path.join(PROJECT_PATH, alembic_location))

    # Запускаем команду alembic
    exit(alembic.run_cmd(config, options))


if __name__ == '__main__':
    main()

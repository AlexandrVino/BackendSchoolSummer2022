FROM python:3.10

RUN mkdir /src
WORKDIR /src

COPY . /src
RUN mkdir /code
RUN pip install -r requirements.txt

# Устанавливаем ссылки, чтобы можно было воспользоваться командами
# приложения
RUN ln -snf /usr/share/python3/app/bin/market-* /usr/local/bin/

# Устанавливаем выполняемую при запуске контейнера команду по умолчанию
WORKDIR .
CMD ["python", "setup.py"]
CMD ["python", "market/api/__main__.py"]
FROM python:3.10

RUN mkdir /src
WORKDIR /src

COPY . /src
RUN mkdir /code
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Устанавливаем ссылки, чтобы можно было воспользоваться командами
# приложения
RUN ln -snf /usr/share/python3/app/bin/market-* /usr/local/bin/

# запускаем сервер
CMD ["python", "main.py"]
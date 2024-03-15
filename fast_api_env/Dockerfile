FROM python:3.9 as base_python

WORKDIR /app

COPY requirements.txt .
COPY /poetry.lock .
COPY /pyproject.toml .

RUN pip install -r requirements.txt
RUN pip install python-multipart


RUN poetry config virtualenvs.create false
RUN poetry install

COPY init.sql /docker-entrypoint-initdb.d/

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002", "--reload"]

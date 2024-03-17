FROM python:3.9 as base_python


COPY requirements.txt .
COPY poetry.lock .
COPY pyproject.toml .


RUN pip install -r requirements.txt
RUN pip install python-multipart
RUN poetry config virtualenvs.create false
RUN poetry install



COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002", "--reload"]

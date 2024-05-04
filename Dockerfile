FROM python:3.11

WORKDIR /app
COPY ./Pipfile /app/Pipfile
RUN ulimit -n 8192
RUN pip install --no-cache-dir --upgrade pipenv
RUN pipenv install --no-cache-dir --upgrade

COPY ./app/* /app
CMD ["uvicorn", "app.main:app", "--port", "8000"]

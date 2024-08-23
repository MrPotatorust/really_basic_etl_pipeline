FROM python:3.12.5-slim


COPY . /app
WORKDIR /app


RUN apt-get update && apt-get install -y postgresql-client
RUN pip install sqlalchemy psycopg2-binary

COPY el.py .
CMD ["python", "el.py"]
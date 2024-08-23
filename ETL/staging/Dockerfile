FROM python:3.12.5-slim


COPY . /app
WORKDIR /app


RUN apt-get update && apt-get install -y postgresql-client
RUN pip install pandas sqlalchemy psycopg2-binary

COPY main.py .
CMD ["python", "main.py"]
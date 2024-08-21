FROM python:3.12.5-slim


COPY . /app
WORKDIR /app


RUN apt-get update && apt-get install -y postgresql-client
RUN pip install -r requirements.txt

COPY el.py .
CMD ["python", "el.py"]
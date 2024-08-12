FROM python:3.11-slim

WORKDIR /app

COPY app.py .

RUN pip install Flask requests docker redis

CMD ["python", "app.py"]

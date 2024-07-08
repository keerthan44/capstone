FROM python:3.9-slim

WORKDIR /app

COPY app.py .

RUN pip install Flask requests docker

CMD ["python", "app.py"]

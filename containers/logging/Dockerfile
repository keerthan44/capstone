FROM python:3.9-slim

WORKDIR /app

COPY app.py .

RUN pip install Flask requests redis docker

CMD ["python", "app.py"]

FROM python:3.9-slim

WORKDIR /app

COPY app.py .

RUN pip install Flask requests docker redis jaeger-client flask-opentracing

CMD ["python", "app.py"]

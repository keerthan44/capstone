FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY ./src/ /app/

ENV PYTHONUNBUFFERED=1

# Define the command to run the application
CMD ["python", "app.py"]

FROM python:3.11-slim

WORKDIR /app

COPY data_loader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data_loader/app.py .
COPY pipeline ./pipeline

CMD ["python", "app.py"]

FROM python:3.12-slim

WORKDIR /app

COPY requirements-api.txt .
RUN pip install --no-cache-dir -r requirements-api.txt

COPY src ./src

ENV PYTHONPATH=/app/src
ENV CIDM_DATA_DIR=/data

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "company_platform.api.app:app", "--host", "0.0.0.0", "--port", "8000"]

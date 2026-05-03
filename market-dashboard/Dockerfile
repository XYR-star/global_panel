FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "ai_app.app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]

FROM python:3.12-slim

WORKDIR /app

COPY requirements_api.txt .
RUN pip install --no-cache-dir -r requirements_api.txt

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY . .

RUN mkdir -p models && curl -L -o models/model_projection_carriere.pkl \
    https://github.com/Justhamid/football-analytics-platform/releases/tag/V1.0-model/model_projection_carriere.pkl

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ARG READER_VERSION=dev
ARG READER_COMMIT=
ARG READER_BUILD_TIME=development
ARG READER_DEPLOY_URL=
ENV READER_VERSION=$READER_VERSION
ENV READER_COMMIT=$READER_COMMIT
ENV READER_BUILD_TIME=$READER_BUILD_TIME
ENV READER_DEPLOY_URL=$READER_DEPLOY_URL

COPY reader_api ./reader_api

EXPOSE 8000
CMD ["uvicorn", "reader_api.main:app", "--host", "0.0.0.0", "--port", "8000"]

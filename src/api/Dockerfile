FROM python:3.13-slim-trixie

ARG UID=1000
ARG GID=1000

# Las mismas dos dependencias del sistema que el contenedor de Spark: cargar un
# modelo de MLlib levanta una JVM, aunque aca sea para predecir una fila.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    CBP_OUTPUT_DIR=/app/outputs \
    CBP_MODELS_DIR=/app/models \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python \
    SPARK_LOCAL_DIRS=/tmp/spark-local

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.12.16 /uv /usr/local/bin/uv

# Dependencias primero: esta capa se cachea y no se reinstala al tocar el codigo
COPY src/api/requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt

COPY pyproject.toml README.md ./
COPY src/clinical_bigdata_pipeline/ ./src/clinical_bigdata_pipeline/
RUN uv pip install --system --no-cache --no-deps .

RUN groupadd --gid "${GID}" app 2>/dev/null || true \
 && useradd --uid "${UID}" --gid "${GID}" --create-home app 2>/dev/null || true \
 && mkdir -p /app/outputs /app/models /tmp/spark-local \
 && chown -R "${UID}:${GID}" /app /tmp/spark-local

USER ${UID}:${GID}

EXPOSE 8000

CMD ["uvicorn", "clinical_bigdata_pipeline.api:app", "--host", "0.0.0.0", "--port", "8000"]

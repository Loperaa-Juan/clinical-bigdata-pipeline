# ---------------------------------------------------------------------------
# Contenedor de PYSPARK — etapas 3 a 6 del pipeline.
#
# Lee el Parquet que dejo el contenedor de Dask, corre las agregaciones
# distribuidas, el analisis sobre RDDs y el modelo de MLlib.
#
# Parte de una imagen de Python (requisito 3.5 de la guia) y le agrega el JRE:
# el wheel de pyspark ya trae la distribucion completa de Spark en deps/jars,
# asi que no hace falta una imagen de Apache Spark. Ademas:
#   - apache/spark-py esta muerta (ultimo tag de abril de 2023)
#   - bitnami/spark dejo de publicar tags publicos en agosto de 2025
#   - apache/spark:*-python3 trae Python 3.10, no 3.13
# ---------------------------------------------------------------------------
FROM python:3.13-slim-trixie

ARG UID=1000
ARG GID=1000

# Spark 4 exige Java 17 o superior. procps aporta `ps`, que usan los scripts
# bin/spark-class internos.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    CBP_DATA_DIR=/app/data \
    CBP_OUTPUT_DIR=/app/outputs \
    # Mismo interprete en driver y ejecutores: PySpark aborta si las versiones
    # minor de Python no coinciden.
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python \
    # Los defaults apuntan a $SPARK_HOME/{work,logs}, que queda dentro de
    # site-packages y es de root: el usuario app no podria escribir ahi.
    SPARK_LOCAL_DIRS=/tmp/spark-local

WORKDIR /app

# uv en vez de pip: con los ~650 MB de pyspark la diferencia se nota
COPY --from=ghcr.io/astral-sh/uv:0.12.16 /uv /usr/local/bin/uv

# pyspark va en su propia capa para que no se reinstale al tocar el codigo
COPY src/pyspark/requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt

COPY pyproject.toml README.md ./
COPY src/clinical_bigdata_pipeline/ ./src/clinical_bigdata_pipeline/
RUN uv pip install --system --no-cache --no-deps .

RUN groupadd --gid "${GID}" app 2>/dev/null || true \
 && useradd --uid "${UID}" --gid "${GID}" --create-home app 2>/dev/null || true \
 && mkdir -p /app/data /app/outputs /app/models /tmp/spark-local \
 && chown -R "${UID}:${GID}" /app /tmp/spark-local

USER ${UID}:${GID}

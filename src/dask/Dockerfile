# ---------------------------------------------------------------------------
# Contenedor de DASK — etapas 1 y 2 del pipeline.
#
# Ingesta el CSV, lo particiona, lo limpia, crea las variables derivadas, corre
# el EDA y escribe el Parquet que despues consume el contenedor de PySpark.
#
# Es la imagen liviana de las dos: NO instala Java, porque Dask es Python puro.
# El contexto de construccion es la raiz del repo (ver docker-compose.yml), no
# esta carpeta, para poder copiar src/clinical_bigdata_pipeline/.
# trixie porque ya tiene precompilado numpy, pandas y pyarrow
# ---------------------------------------------------------------------------
FROM python:3.13-slim-trixie 

ARG UID=1000
ARG GID=1000

ENV PYTHONUNBUFFERED=1 \
    CBP_DATA_DIR=/app/data \
    CBP_OUTPUT_DIR=/app/outputs

WORKDIR /app

# uv en vez de pip: resuelve e instala bastante mas rapido. Version fijada
# para que el build sea reproducible.
COPY --from=ghcr.io/astral-sh/uv:0.12.16 /uv /usr/local/bin/uv

# Dependencias primero: esta capa se cachea y no se reinstala al tocar el codigo
COPY src/dask/requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt

# El paquete despues. --no-deps: ya estan instaladas y el pyproject declara
# ademas cosas que este contenedor no necesita.
COPY pyproject.toml README.md ./
COPY src/clinical_bigdata_pipeline/ ./src/clinical_bigdata_pipeline/
RUN uv pip install --system --no-cache --no-deps .

# Mismo UID/GID que el usuario del host, para que lo que el contenedor escriba
# en el bind mount ./data pertenezca al usuario y no a root.
RUN groupadd --gid "${GID}" app 2>/dev/null || true \
 && useradd --uid "${UID}" --gid "${GID}" --create-home app 2>/dev/null || true \
 && mkdir -p /app/data /app/outputs \
 && chown -R "${UID}:${GID}" /app

USER ${UID}:${GID}

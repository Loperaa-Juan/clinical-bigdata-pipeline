# Clinical Big Data Pipeline

Proyecto de la asignatura **Big Data** de la Facultad de Ingenierías de la Institución Universitaria de Envigado.

El repositorio contiene **dos trabajos independientes**:

| Trabajo | Dataset | Herramientas | Dónde está |
|---|---|---|---|
| **1. Algoritmo genético y procesamiento paralelo** | CDC Diabetes Health Indicators | Dask (`LocalCluster`) | `notebooks/01..03` |
| **2. Pipeline integrador Dask + Spark + Docker** | Heart Disease Health Indicators (BRFSS 2015) | Dask, Apache Spark, Docker | `src/`, `notebooks/04` |

---

## Trabajo 1 — Algoritmo genético y procesamiento paralelo

Implementación de un algoritmo genético desde cero para **selección de variables**, comparando el tiempo de ejecución entre una versión secuencial y una paralelizada con Dask. El detalle de cada etapa está documentado en los notebooks.

```bash
uv sync
uv run jupyter lab
```

Ejecutar los notebooks en orden. El notebook `03_genetic_algorithm_dask.ipynb` lee el tiempo que guarda `02_genetic_algorithm.ipynb` en `outputs/tiempo_secuencial.csv`, así que el 02 debe correr antes.

> [!NOTE]
> Durante la actividad ejecutamos los notebooks tanto en hardware local como en la nube, usando Google Colab a través de su extensión para VS Code. Por eso, algunas salidas guardadas en los notebooks muestran rutas y tiempos de ejecución de uno u otro entorno.

---

## Trabajo 2 — Pipeline integrador Dask + Spark

### El flujo

```
  CSV de Kaggle (host)
        │
        ▼
 ┌──────────────────────┐   Parquet particionado   ┌────────────────────────┐
 │  contenedor DASK     │ ───────────────────────► │  contenedor PYSPARK    │
 │  · ingesta           │    volumen ./data        │  · agregación          │
 │  · limpieza          │                          │  · análisis sobre RDDs │
 │  · variables derivad.│                          │    (flatMap/reduceByKey)│
 │  · EDA + selección   │ ◄─────────────────────── │  · modelo MLlib        │
 └──────────────────────┘    ./data/resultados     └────────────────────────┘
```

Dask y Spark **no son dos ejercicios separados**: se pasan los datos por un volumen compartido en formato Parquet, y `docker compose` declara la dependencia con `service_completed_successfully`, de modo que Spark no arranca hasta que Dask terminó de escribir.

### Qué hace cada motor y por qué

| Etapa | Motor | Justificación |
|---|---|---|
| Ingesta y particionamiento | **Dask** | Semántica pandas, todo en proceso Python, sin frontera de serialización con la JVM |
| Limpieza y variables derivadas | **Dask** | Es `map` sobre particiones sin shuffle: donde Dask es más eficiente y Spark más caro |
| Selección de variables | **Dask** | Es una matriz de correlación sobre todo el dataset: una agregación, sin entrenar modelos |
| Agregaciones distribuidas | **Spark** | Catalyst optimiza el plan lógico y aprovecha el particionado del Parquet |
| Conteos sobre RDDs | **Spark** | La API de RDD hace explícita la separación entre transformaciones y acciones |
| Modelo | **Spark MLlib** | Biblioteca de ML realmente distribuida |

### Requisitos

- **Docker** y **Docker Compose** (no hace falta instalar Java ni Spark en el host)
- El CSV del dataset, que se descarga una sola vez desde el host

### EJEMPLO DE .ENV
UID=1000
GID=1000

CBP_DASK_MEM=2g
CBP_SPARK_MEM=3g

### Ejecución

El paso 0 descarga el dataset **en el host**, una sola vez. El contenedor de Dask también sabría descargarlo, pero conviene hacerlo antes: así la corrida del pipeline no depende de la red ni de Kaggle, y se puede repetir sin conexión.

```bash
# 0. Descargar el dataset (una sola vez)
uv sync
uv run python -c "from clinical_bigdata_pipeline import dask_stage; \
                  print(dask_stage.ensure_dataset(dask_stage.RUTA_CSV))"

# 1. Construir las dos imágenes
docker compose build

# 2. Ejecutar el pipeline completo: primero Dask, después Spark
docker compose up
```

Mientras corre:

- **Dashboard de Dask** — http://localhost:8787
- **Spark UI** — http://localhost:4040 (visible mientras Spark trabaja)

### Parámetros

Los parámetros de cada etapa son constantes al principio de su módulo: [dask_stage.py](src/clinical_bigdata_pipeline/dask_stage.py) (particiones, workers, cuántas variables conserva la selección) y [spark_stage.py](src/clinical_bigdata_pipeline/spark_stage.py) (master de Spark, repeticiones del benchmark). La función `main()` de cada uno es el mapa: llama las etapas en orden.

Los límites de memoria de cada contenedor y el UID/GID con el que corren se ajustan en `.env`, que no se versiona porque el UID/GID es propio de cada máquina; la plantilla es [.env.example](.env.example). El UID/GID hace que lo que el contenedor escriba en `./data` y `./outputs` pertenezca al usuario del host y no a root: como se pasan al Dockerfile en tiempo de construcción, cambiarlos exige volver a correr `docker compose build`.

### Salidas

Todo cae en `outputs/heart_pipeline/`, en su propia subcarpeta para no mezclarse con los artefactos del trabajo 1:

| Archivo | Contenido |
|---|---|
| `pipeline_gradiente_riesgo.png` | Prevalencia según el puntaje de salud cardiovascular |
| `pipeline_variables.png` | Selección de variables en Dask y prevalencia por factor en Spark |
| `pipeline_comparacion_motores.png` | Dask vs. Spark: tiempo y memoria |
| `tabla_comparativa.csv` | La misma operación en los dos motores, con tiempos y memoria |
| `metricas_modelos.csv` | Accuracy, Precision, Recall, F1 y AUC-PR de los cinco modelos |
| `seleccion_variables.csv` | Correlación de cada variable y cuáles quedaron seleccionadas |

---

## Estructura del repositorio

```
clinical-bigdata-pipeline/
├── notebooks/
│   ├── 01_exploratory_data_analysis.ipynb      # Trabajo 1: EDA del dataset de diabetes
│   ├── 02_genetic_algorithm.ipynb              # Trabajo 1: GA secuencial
│   ├── 03_genetic_algorithm_dask.ipynb         # Trabajo 1: GA paralelizado con Dask
│   └── 04_dask_spark_pipeline.ipynb            # Trabajo 2: recorrido narrado del pipeline
├── src/
│   ├── dask/
│   │   ├── Dockerfile                          # Imagen del contenedor de Dask (sin Java)
│   │   └── requirements.txt
│   ├── pyspark/
│   │   ├── Dockerfile                          # Imagen del contenedor de Spark (con JRE 21)
│   │   └── requirements.txt
│   └── clinical_bigdata_pipeline/              # Paquete compartido por ambos contenedores
│       ├── dask_stage.py                       # Etapas 1-2 y mitad Dask de la 5 · comando cbp-dask
│       ├── spark_stage.py                      # Etapas 3-4 y mitad Spark de la 5 · comando cbp-spark
│       └── visualize.py                        # Etapa 6: las tres figuras
├── outputs/
│   ├── ga_secuencial_vs_dask.png               # Trabajo 1
│   ├── tiempo_secuencial.csv                   # Trabajo 1
│   └── heart_pipeline/                         # Trabajo 2
├── data/                                       # No versionado: dataset y Parquet generados
├── docker-compose.yml
├── .dockerignore
├── .env                                        # Parámetros de ejecución
├── pyproject.toml
├── uv.lock
└── README.md
```

## Dataset del trabajo 2

[Heart Disease Health Indicators](https://www.kaggle.com/datasets/alexteboul/heart-disease-health-indicators-dataset), derivado de la encuesta **BRFSS 2015** del CDC de Estados Unidos.

- 253.680 registros × 22 columnas, todas numéricas, sin valores faltantes
- Objetivo: `HeartDiseaseorAttack`, con **9,42 % de positivos** (desbalance ≈ 1:9,6)
- 21 variables predictoras, de las que el pipeline selecciona las 10 más correlacionadas con el objetivo


## Integrantes

- **Juan José Lopera Londoño** — [@Loperaa-Juan](https://github.com/Loperaa-Juan)
- **Jairo Alberto Mejía Ramírez** — [@Jairo-commit](https://github.com/Jairo-commit)

# Clinical Big Data Pipeline

Proyecto de la asignatura **Big Data** de la Facultad de Ingenierías de la Institución Universitaria de Envigado.

## Contexto

Este repositorio aborda la actividad de **algoritmos genéticos y procesamiento paralelo**: la implementación de un algoritmo genético desde cero en Python y la comparación del tiempo de ejecución entre una versión secuencial y una versión paralelizada con Dask.

El detalle de cada etapa se encuentra documentado en los notebooks.

## Estructura del repositorio

```
clinical-bigdata-pipeline/
├── notebooks/
│   ├── 01_exploratory_data_analysis.ipynb            # Análisis exploratorio del dataset
│   ├── 02_genetic_algorithm.ipynb                    # Algoritmo genético secuencial
│   └── 03_genetic_algorithm_dask.ipynb               # Algoritmo genético paralelizado con Dask
├── outputs/
│   ├── ga_secuencial_vs_dask.png                     # Comparación de tiempos entre ambas versiones
│   └── tiempo_secuencial.csv                         # Tiempo de ejecución de la versión secuencial
├── .gitignore
├── .python-version                                   # Versión de Python del proyecto (3.13)
├── pyproject.toml                                    # Dependencias del proyecto
├── uv.lock                                           # Versiones exactas de las dependencias
└── README.md
```

## Requisitos y ejecución

El proyecto usa **Python 3.13** y [uv](https://docs.astral.sh/uv/) para gestionar el entorno y las dependencias.

1. Instalar las dependencias:

   ```bash
   uv sync
   ```

2. Abrir Jupyter Lab:

   ```bash
   uv run jupyter lab
   ```

3. Ejecutar los notebooks en orden. El notebook `03_genetic_algorithm_dask.ipynb` lee el tiempo que guarda `02_genetic_algorithm.ipynb` en `outputs/tiempo_secuencial.csv` para comparar ambas versiones, así que el notebook 02 debe ejecutarse antes.

> [!NOTE]
> Durante la actividad ejecutamos los notebooks tanto en hardware local como en la nube, usando Google Colab a través de su extensión para VS Code. Por eso, algunas salidas guardadas en los notebooks muestran rutas y tiempos de ejecución de uno u otro entorno.

## Integrantes

- **Juan José Lopera Londoño** — [@Loperaa-Juan](https://github.com/Loperaa-Juan)
- **Jairo Alberto Mejía Ramírez** — [@Jairo-commit](https://github.com/Jairo-commit)

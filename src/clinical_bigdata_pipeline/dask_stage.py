"""Etapas 1, 2 y mitad Dask de la 5.

Ingesta, limpieza, variables derivadas, EDA y seleccion de variables. Corre en
el contenedor `dask` con el comando `cbp-dask` y deja un Parquet particionado
que despues lee el contenedor `pyspark`.
"""

import os
import shutil
import time
from pathlib import Path

import dask.dataframe as dd
import kagglehub
import pandas as pd
import psutil
from dask.distributed import Client, LocalCluster

# --- rutas -----------------------------------------------------------------
# Contra este archivo y no contra el cwd: asi valen igual desde la terminal,
# desde Jupyter y desde el contenedor.
RAIZ = Path(__file__).resolve().parents[2]
DIR_DATOS = Path(os.environ.get("CBP_DATA_DIR", RAIZ / "data"))
DIR_SALIDAS = Path(os.environ.get("CBP_OUTPUT_DIR", RAIZ / "outputs")) / "heart_pipeline"

RUTA_CSV = DIR_DATOS / "raw" / "heart_disease_health_indicators_BRFSS2015.csv"
DIR_PARQUET = DIR_DATOS / "curated"
RUTA_FILAS = DIR_DATOS / "filas_dask.txt"  # Spark lo lee para verificar el traspaso

# --- parametros ------------------------------------------------------------
N_WORKERS = 4  # explicito: os.cpu_count() ve los 16 del host, no el limite del contenedor
BLOCKSIZE = "2MB"  # el default son 64 MiB y el CSV pesa 21,7: seria 1 sola particion
TOP_VARIABLES = 10
REPETICIONES_BENCHMARK = 5

OBJETIVO = "HeartDiseaseorAttack"  # es la PRIMERA columna del CSV, no la ultima

DATASET_KAGGLE = "alexteboul/heart-disease-health-indicators-dataset"
NOMBRE_CSV = "heart_disease_health_indicators_BRFSS2015.csv"

# Contrato con el dataset: si Kaggle publica otra revision, falla en voz alta
COLUMNAS = [
    "HeartDiseaseorAttack", "HighBP", "HighChol", "CholCheck", "BMI", "Smoker",
    "Stroke", "Diabetes", "PhysActivity", "Fruits", "Veggies",
    "HvyAlcoholConsump", "AnyHealthcare", "NoDocbcCost", "GenHlth", "MentHlth",
    "PhysHlth", "DiffWalk", "Sex", "Age", "Education", "Income",
]
PREDICTORAS = [c for c in COLUMNAS if c != OBJETIVO]  # las 21 candidatas

# Bandas de Age (1 = 18-24 ... 13 = 80+). El prefijo numerico importa: Spark lee
# la clave de particion como string y sin el ordenaria mal.
BANDAS_EDAD = [0, 4, 7, 10, 13]
ETIQUETAS_EDAD = ["1_18a39", "2_40a54", "3_55a69", "4_70ymas"]


def ensure_dataset(ruta_csv: Path) -> Path:
    if ruta_csv.exists():
        return ruta_csv

    ruta_csv.parent.mkdir(parents=True, exist_ok=True)
    try:
        descarga = Path(kagglehub.dataset_download(DATASET_KAGGLE))
        shutil.copy2(next(descarga.rglob(NOMBRE_CSV)), ruta_csv)
        return ruta_csv
    except Exception as error:
        raise FileNotFoundError(
            f"No se encontro {ruta_csv} y la descarga automatica fallo ({error}).\n"
            f"Descargalo de https://www.kaggle.com/datasets/{DATASET_KAGGLE} "
            f"y dejalo en {ruta_csv}"
        ) from error


def load_raw(ruta_csv: Path) -> dd.DataFrame:
    ddf = dd.read_csv(str(ruta_csv), blocksize=BLOCKSIZE)

    # Validar columnas es gratis: ddf.columns no dispara computo
    if list(ddf.columns) != COLUMNAS:
        raise ValueError(f"El esquema del dataset cambio: {list(ddf.columns)}")

    return ddf


def clean_and_derive(ddf: dd.DataFrame) -> dd.DataFrame:
    """Limpieza y las dos variables derivadas que pide la guia."""
    ddf = ddf.dropna()
    ddf = ddf[(ddf["BMI"] >= 12) & (ddf["BMI"] <= 60)]  # el archivo llega a 98

    # PuntajeSaludCV (0-7): adaptacion de Life's Simple 7 de la AHA, un punto
    # por componente saludable. Ojo: Diabetes tiene tres niveles, no es binaria.
    ddf["PuntajeSaludCV"] = (
        (ddf["Smoker"] == 0).astype("int8")
        + (ddf["PhysActivity"] == 1).astype("int8")
        + ((ddf["Fruits"] == 1) & (ddf["Veggies"] == 1)).astype("int8")
        + (ddf["BMI"] < 25).astype("int8")
        + (ddf["HighBP"] == 0).astype("int8")
        + (ddf["HighChol"] == 0).astype("int8")
        + (ddf["Diabetes"] == 0).astype("int8")
    )

    # GrupoEtario: agrupa la edad y es la clave de particion del Parquet
    ddf["GrupoEtario"] = ddf["Age"].map_partitions(
        lambda s: pd.cut(
            s, bins=BANDAS_EDAD, labels=ETIQUETAS_EDAD, right=True
        ).astype(str),
        meta=("GrupoEtario", "object"),
    )

    return ddf


def run_eda(ddf: dd.DataFrame) -> tuple[dict, pd.DataFrame]:
    # Un solo compute() para los tres: Dask fusiona el grafo y recorre los
    # datos una vez en lugar de tres.
    n_filas, n_nulos, n_positivos = dd.compute(
        ddf.shape[0], ddf.isna().sum().sum(), ddf[OBJETIVO].sum()
    )
    perfil = {
        "filas": int(n_filas),
        "nulos": int(n_nulos),
        "positivos": int(n_positivos),
        "tasa_positivos_pct": round(100 * float(n_positivos) / int(n_filas), 2),
    }

    tasas = (
        ddf.groupby("PuntajeSaludCV")[OBJETIVO].agg(["count", "mean"]).compute()
    ).sort_index()
    tasas.columns = ["n", "tasa"]
    tasas["TasaPct"] = (tasas["tasa"] * 100).round(2)

    return perfil, tasas.drop(columns="tasa").reset_index()


def select_features(ddf: dd.DataFrame, top_n: int) -> pd.DataFrame:
    """Seleccion tipo *filter*: correlacion con el objetivo, sin entrenar nada."""
    numericas = ddf[[OBJETIVO] + PREDICTORAS]

    # Materializar ANTES de elegir la columna: si no, el optimizador de Dask
    # empuja la proyeccion dentro de corr() y falla.
    matriz = numericas.corr().compute()

    correlaciones = matriz[OBJETIVO].drop(OBJETIVO)
    tabla = pd.DataFrame({
        "variable": correlaciones.index,
        "correlacion": correlaciones.values.round(4),
    })
    tabla["abs"] = tabla["correlacion"].abs()
    tabla = tabla.sort_values("abs", ascending=False).reset_index(drop=True)
    tabla["seleccionada"] = tabla.index < top_n
    return tabla.drop(columns="abs")


def write_parquet(ddf: dd.DataFrame, destino: Path) -> int:
    # int16 reduce el Parquet ~4x y Spark lee los tipos sin inferirlos
    for columna in ddf.columns:
        if columna != "GrupoEtario":
            ddf[columna] = ddf[columna].astype("int16")

    n_filas = int(ddf.shape[0].compute())

    destino.parent.mkdir(parents=True, exist_ok=True)
    # Una sola clave de particion, cardinalidad 4 y balanceada. Con Age (13) o
    # Age+Sex (26) saldrian decenas de archivos de pocos KB.
    ddf.to_parquet(
        destino,
        engine="pyarrow",
        compression="snappy",
        partition_on=["GrupoEtario"],
        write_index=False,
        overwrite=True,
    )
    return n_filas


def benchmark_dask(dir_parquet: Path, repeticiones: int) -> pd.DataFrame:
    """Mitad Dask de la etapa 5: la misma operacion que corre Spark despues."""
    tiempos = []
    proceso = psutil.Process()
    rss_pico = 0.0

    for i in range(repeticiones):
        inicio = time.perf_counter()
        ddf = dd.read_parquet(str(dir_parquet))
        # observed=True: GrupoEtario vuelve del Parquet como categoria
        agregado = ddf.groupby(["GrupoEtario", "Sex"], observed=True)[OBJETIVO].agg(
            ["count", "mean"]
        )
        agregado.compute()  # dentro del reloj: si no, se mide armar el grafo
        transcurrido = time.perf_counter() - inicio

        # RSS del arbol de procesos: sobreestima, sirve para comparar motores
        rss = proceso.memory_info().rss
        for hijo in proceso.children(recursive=True):
            try:
                rss += hijo.memory_info().rss
            except psutil.Error:
                continue
        rss_pico = max(rss_pico, rss / 1024**2)

        if i > 0:  # se descarta la primera: cache del SO en frio
            tiempos.append(transcurrido)

    return pd.DataFrame([{
        "motor": "Dask",
        "operacion": "groupBy GrupoEtario+Sex",
        "tiempo_mediano_s": round(pd.Series(tiempos).median(), 4),
        "rss_pico_mb": round(rss_pico, 1),
    }])


def _titulo(texto: str) -> None:
    print("\n" + "=" * 70)
    print(texto)
    print("=" * 70)


def main() -> None:
    """Punto de entrada del contenedor `dask`. Llama las etapas en orden."""
    DIR_SALIDAS.mkdir(parents=True, exist_ok=True)

    cluster = LocalCluster(
        n_workers=N_WORKERS, threads_per_worker=1, processes=True
    )
    cliente = Client(cluster)
    print(f"Dashboard de Dask: {cliente.dashboard_link}")

    try:
        _titulo("ETAPA 1: INGESTA Y PARTICIONAMIENTO CON DASK")
        ensure_dataset(RUTA_CSV)
        ddf = load_raw(RUTA_CSV)
        print(f"Particiones: {ddf.npartitions}")

        _titulo("ETAPA 2: LIMPIEZA, VARIABLES DERIVADAS Y EDA")
        ddf = clean_and_derive(ddf)
        perfil, tasas = run_eda(ddf)
        print(perfil)
        print(tasas.to_string(index=False))
        tasas.to_csv(DIR_SALIDAS / "tasas_puntaje.csv", index=False)

        _titulo("ETAPA 2b: SELECCION DE VARIABLES CON DASK")
        seleccion = select_features(ddf, TOP_VARIABLES)
        print(seleccion.to_string(index=False))
        seleccion.to_csv(DIR_SALIDAS / "seleccion_variables.csv", index=False)

        n_filas = write_parquet(ddf, DIR_PARQUET)
        print(f"\nParquet escrito en {DIR_PARQUET} ({n_filas} filas)")
        RUTA_FILAS.write_text(str(n_filas))

        _titulo("ETAPA 5 (mitad Dask)")
        tabla = benchmark_dask(DIR_PARQUET, REPETICIONES_BENCHMARK)
        print(tabla.to_string(index=False))
        tabla.to_csv(DIR_SALIDAS / "benchmark_dask.csv", index=False)
    finally:
        cliente.close()
        cluster.close()

    print("\nListo.")

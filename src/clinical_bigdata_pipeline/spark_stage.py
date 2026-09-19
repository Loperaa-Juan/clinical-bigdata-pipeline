"""Etapas 3, 4, mitad Spark de la 5, y 6.

Procesamiento distribuido, analisis sobre RDDs, modelos MLlib, comparacion
entre motores y visualizaciones. Corre en el contenedor `pyspark` con el
comando `cbp-spark`, sobre el Parquet que escribio Dask.
"""

import os
import time
from pathlib import Path

import pandas as pd
import psutil
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    DecisionTreeClassifier,
    LinearSVC,
    LogisticRegression,
    NaiveBayes,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import visualize

# --- rutas -----------------------------------------------------------------
# Repetidas aca a proposito: este contenedor no tiene Dask instalado y no puede
# importar nada de dask_stage.
RAIZ = Path(__file__).resolve().parents[2]
DIR_DATOS = Path(os.environ.get("CBP_DATA_DIR", RAIZ / "data"))
DIR_SALIDAS = Path(os.environ.get("CBP_OUTPUT_DIR", RAIZ / "outputs")) / "heart_pipeline"
DIR_MODELOS = Path(os.environ.get("CBP_MODELS_DIR", RAIZ / "models"))

DIR_PARQUET = DIR_DATOS / "curated"
DIR_RESULTADOS = DIR_DATOS / "resultados"
RUTA_FILAS = DIR_DATOS / "filas_dask.txt"

# --- parametros ------------------------------------------------------------
SPARK_MASTER = "local[*]"
REPETICIONES_BENCHMARK = 5

OBJETIVO = "HeartDiseaseorAttack"
SEMILLA = 42


def build_spark_session(master: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("clinical-bigdata-pipeline")
        .master(master)
        # El default es 200: sobre 253k filas serian ~1270 filas por tarea
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def verify_exchange(filas_dask: int, sdf: DataFrame, dir_parquet: Path) -> None:
    """Evidencia del traspaso Dask -> Spark: es condicion de aprobacion."""
    print("=" * 70)
    print("PUNTO DE INTERCAMBIO DASK -> SPARK")
    print("=" * 70)
    print(f"Parquet escrito por Dask: {dir_parquet}")
    for archivo in sorted(dir_parquet.rglob("*.parquet"))[:8]:
        relativo = archivo.relative_to(dir_parquet)
        print(f"  {relativo}  ({archivo.stat().st_size / 1024:.1f} KB)")

    sdf.printSchema()

    filas_spark = sdf.count()
    print(f"Filas escritas por Dask : {filas_dask}")
    print(f"Filas leidas por Spark  : {filas_spark}")
    assert filas_dask == filas_spark, (
        f"El traspaso perdio filas: {filas_dask} != {filas_spark}"
    )

    # PartitionFilters en el plan = partition pruning funcionando
    print("\nPlan fisico de un filtro sobre la clave de particion:")
    sdf.filter(F.col("GrupoEtario") == "4_70ymas").explain()


def aggregate(sdf: DataFrame) -> pd.DataFrame:
    """Etapa 3: agregacion distribuida y filtro."""
    return (
        sdf.groupBy("PuntajeSaludCV")
        .agg(
            F.count("*").alias("n"),
            F.round(100 * F.avg(OBJETIVO), 2).alias("TasaPct"),
        )
        # Con tasa base 9.4%, un grupo de 100 tiene IC95% de +-6 puntos: ruido
        .filter(F.col("n") >= 100)
        .orderBy("PuntajeSaludCV")
        .toPandas()
    )


def prevalencia_por_variable(sdf: DataFrame, variables: list[str]) -> pd.DataFrame:
    """Etapa 3 sobre RDDs: prevalencia por cada valor de cada variable binaria.

    Sobre la API de RDD y no sobre DataFrames a proposito: ahi la separacion
    entre planeacion y ejecucion se ve explicita.
    """
    binarias = [v for v in variables if v in {
        "HighBP", "HighChol", "CholCheck", "Smoker", "Stroke", "PhysActivity",
        "Fruits", "Veggies", "HvyAlcoholConsump", "AnyHealthcare",
        "NoDocbcCost", "DiffWalk", "Sex",
    }]
    columnas = binarias + [OBJETIVO]

    # ---------- PLANEACION: transformaciones, no se ejecuta nada ----------
    rdd = (
        sdf.select(*columnas).rdd
        .flatMap(lambda fila: [
            ((v, int(fila[v])), (int(fila[OBJETIVO]), 1)) for v in binarias
        ])
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    )

    # ---------- EJECUCION: la accion dispara el trabajo ----------
    conteos = rdd.collect()

    filas = [
        {"variable": v, "valor": valor, "n": total,
         "TasaPct": round(100 * casos / total, 2)}
        for (v, valor), (casos, total) in conteos
    ]
    return pd.DataFrame(filas).sort_values(["variable", "valor"]).reset_index(drop=True)


def _preparar(sdf: DataFrame, variables: list[str]) -> tuple[DataFrame, DataFrame, float]:
    """Features, pesos de clase y particion train/test. Se hace una sola vez."""
    categoricas = [v for v in variables if v == "GrupoEtario"]
    numericas = [v for v in variables if v not in categoricas]

    etapas = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in categoricas
    ]
    etapas.append(
        VectorAssembler(
            inputCols=numericas + [f"{c}_idx" for c in categoricas],
            outputCol="features",
        )
    )
    datos = Pipeline(stages=etapas).fit(sdf).transform(sdf)

    # Pesos de clase en lugar de SMOTE: MLlib no lo trae, y reponderar no
    # inventa pacientes sinteticos a partir de registros reales.
    prevalencia = datos.select(F.avg(OBJETIVO)).first()[0]
    datos = datos.withColumn(
        "pesoClase",
        F.when(F.col(OBJETIVO) == 1, (1 - prevalencia) / prevalencia).otherwise(1.0),
    )

    # left_anti y no subtract(): subtract hace diferencia de CONJUNTOS y
    # deduplicaria las ~23.900 filas repetidas, que son encuestados distintos.
    datos = datos.withColumn("_id", F.monotonically_increasing_id()).cache()
    train = datos.sampleBy(OBJETIVO, {0: 0.8, 1: 0.8}, seed=SEMILLA).cache()
    test = datos.join(train.select("_id"), on="_id", how="left_anti").cache()

    return train, test, prevalencia


def train_models(sdf: DataFrame, variables: list[str],
                 dir_modelos: Path) -> tuple[pd.DataFrame, str]:
    """Etapa 4: los cinco modelos del paper que tienen equivalente en MLlib.

    Quedan fuera KNN (no existe en MLlib), Linear Regression (es un regresor,
    el paper lo usa mal para clasificacion binaria) y el Ensemble por votacion.
    """
    train, test, prevalencia = _preparar(sdf, variables)

    comunes = dict(featuresCol="features", labelCol=OBJETIVO, weightCol="pesoClase")
    modelos = {
        "DecisionTree": DecisionTreeClassifier(**comunes, maxDepth=8, seed=SEMILLA),
        "RandomForest": RandomForestClassifier(**comunes, numTrees=50, maxDepth=8,
                                               seed=SEMILLA),
        "LinearSVC": LinearSVC(**comunes, maxIter=50),
        "LogisticRegression": LogisticRegression(**comunes, maxIter=50),
        # gaussian y no el multinomial por defecto: ese asume features de
        # conteo y deja el modelo anticorrelado
        "NaiveBayes": NaiveBayes(**comunes, modelType="gaussian"),
    }

    # Las cuatro metricas del paper, para poder poner la tabla al lado de la suya
    evaluadores = {
        "Accuracy": MulticlassClassificationEvaluator(
            labelCol=OBJETIVO, metricName="accuracy"),
        "Precision": MulticlassClassificationEvaluator(
            labelCol=OBJETIVO, metricName="weightedPrecision"),
        "Recall": MulticlassClassificationEvaluator(
            labelCol=OBJETIVO, metricName="weightedRecall"),
        "F1": MulticlassClassificationEvaluator(labelCol=OBJETIVO, metricName="f1"),
    }
    # AUC-PR es la nuestra: su linea base es la prevalencia, no 0.5
    eval_pr = BinaryClassificationEvaluator(labelCol=OBJETIVO, metricName="areaUnderPR")

    filas, ajustados = [], {}
    for nombre, estimador in modelos.items():
        ajustado = estimador.fit(train)
        predicciones = ajustado.transform(test)
        ajustados[nombre] = ajustado

        fila = {"modelo": nombre}
        fila.update({k: round(e.evaluate(predicciones), 4)
                     for k, e in evaluadores.items()})
        fila["AUC_PR"] = round(eval_pr.evaluate(predicciones), 4)
        filas.append(fila)
        print(f"  {nombre:20} accuracy={fila['Accuracy']:.4f}  AUC-PR={fila['AUC_PR']:.4f}")

    tabla = pd.DataFrame(filas).sort_values("Accuracy", ascending=False)
    tabla["linea_base_AUC_PR"] = round(prevalencia, 4)

    # El mejor por accuracy, como en el paper
    mejor = tabla.iloc[0]["modelo"]
    dir_modelos.mkdir(parents=True, exist_ok=True)
    ajustados[mejor].write().overwrite().save(str(dir_modelos / mejor))

    train.unpersist()
    test.unpersist()
    return tabla.reset_index(drop=True), mejor


def benchmark_spark(spark: SparkSession, dir_parquet: Path,
                    repeticiones: int) -> pd.DataFrame:
    """Mitad Spark de la etapa 5: la misma operacion que corrio Dask."""
    tiempos = []
    proceso = psutil.Process()
    rss_pico = 0.0

    for i in range(repeticiones):
        inicio = time.perf_counter()
        sdf = spark.read.parquet(str(dir_parquet))
        agregado = sdf.groupBy("GrupoEtario", "Sex").agg(
            F.count("*").alias("n"), F.avg(OBJETIVO).alias("tasa")
        )
        agregado.collect()  # collect y no show: show inserta un limit
        transcurrido = time.perf_counter() - inicio

        # Mismo criterio que la mitad de Dask: RSS del arbol de procesos
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
        "motor": "Spark",
        "operacion": "groupBy GrupoEtario+Sex",
        "tiempo_mediano_s": round(pd.Series(tiempos).median(), 4),
        "rss_pico_mb": round(rss_pico, 1),
    }])


def _titulo(texto: str) -> None:
    print("\n" + "=" * 70)
    print(texto)
    print("=" * 70)


def main() -> None:
    """Punto de entrada del contenedor `pyspark`. Llama las etapas en orden."""
    DIR_SALIDAS.mkdir(parents=True, exist_ok=True)
    DIR_RESULTADOS.mkdir(parents=True, exist_ok=True)

    # El arranque de la JVM se mide aparte: comparado con la agregacion
    # completa es el argumento central de la conclusion del trabajo.
    inicio = time.perf_counter()
    spark = build_spark_session(SPARK_MASTER)
    spark.range(1).count()
    arranque = time.perf_counter() - inicio
    spark.sparkContext.setLogLevel("WARN")
    print(f"Arranque de SparkSession (JVM + catalogo): {arranque:.2f} s")

    try:
        sdf = spark.read.parquet(str(DIR_PARQUET))

        if RUTA_FILAS.exists():
            verify_exchange(int(RUTA_FILAS.read_text()), sdf, DIR_PARQUET)

        _titulo("ETAPA 3: AGREGACION DISTRIBUIDA")
        tasas = aggregate(sdf)
        print(tasas.to_string(index=False))

        # Segundo traspaso Dask -> Spark, esta vez por CSV
        seleccion = pd.read_csv(DIR_SALIDAS / "seleccion_variables.csv")
        elegidas = seleccion.loc[seleccion["seleccionada"], "variable"].tolist()
        print(f"\nVariables que selecciono Dask: {elegidas}")

        _titulo("ETAPA 3b: PREVALENCIA POR FACTOR, SOBRE RDDs")
        prevalencias = prevalencia_por_variable(sdf, elegidas)
        print(prevalencias.to_string(index=False))

        _titulo("ETAPA 4: MODELOS MLlib (los del paper de Ilyas et al.)")
        metricas, mejor = train_models(sdf, elegidas, DIR_MODELOS)
        print()
        print(metricas.to_string(index=False))
        print(f"\nMejor por accuracy: {mejor}  ->  guardado en {DIR_MODELOS / mejor}")

        _titulo("ETAPA 5: COMPARACION DASK vs. SPARK")
        tabla_spark = benchmark_spark(spark, DIR_PARQUET, REPETICIONES_BENCHMARK)
        ruta_dask = DIR_SALIDAS / "benchmark_dask.csv"
        partes = [pd.read_csv(ruta_dask)] if ruta_dask.exists() else []
        tabla = pd.concat(partes + [tabla_spark], ignore_index=True)
        print(tabla.to_string(index=False))
        print(f"\nCosto fijo no incluido arriba: arranque de la JVM "
              f"{arranque:.2f} s.")

        _titulo("ETAPA 6: VISUALIZACIONES")
        figuras = [
            visualize.figura_gradiente(tasas, DIR_SALIDAS),
            visualize.figura_variables(seleccion, prevalencias, DIR_SALIDAS),
            visualize.figura_motores(tabla, DIR_SALIDAS),
        ]
        for figura in figuras:
            print(f"  {figura}")

        # Vuelta Spark -> Dask: el intercambio queda en los dos sentidos
        prevalencias.to_parquet(DIR_RESULTADOS / "prevalencias.parquet")

        tabla.to_csv(DIR_SALIDAS / "tabla_comparativa.csv", index=False)
        metricas.to_csv(DIR_SALIDAS / "metricas_modelos.csv", index=False)
    finally:
        spark.stop()

    print("\nListo.")

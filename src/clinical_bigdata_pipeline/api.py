import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pyspark.ml.classification import (
    DecisionTreeClassificationModel,
    LinearSVCModel,
    LogisticRegressionModel,
    NaiveBayesModel,
    RandomForestClassificationModel,
)
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

# Rutas
RAIZ = Path(__file__).resolve().parents[2]
DIR_SALIDAS = Path(os.environ.get("CBP_OUTPUT_DIR", RAIZ / "outputs")) / "heart_pipeline"
DIR_MODELOS = Path(os.environ.get("CBP_MODELS_DIR", RAIZ / "models"))
RUTA_SELECCION = DIR_SALIDAS / "seleccion_variables.csv"

# MLlib guarda el nombre de la clase Java en la metadata. Cual de los cinco
# modelos gano cambia entre corridas, asi que la clase se resuelve leyendo ese
# campo en vez de fijarla aca.
CLASES = {
    "org.apache.spark.ml.classification.DecisionTreeClassificationModel":
        DecisionTreeClassificationModel,
    "org.apache.spark.ml.classification.RandomForestClassificationModel":
        RandomForestClassificationModel,
    "org.apache.spark.ml.classification.LinearSVCModel": LinearSVCModel,
    "org.apache.spark.ml.classification.LogisticRegressionModel":
        LogisticRegressionModel,
    "org.apache.spark.ml.classification.NaiveBayesModel": NaiveBayesModel,
}

# Un encuestado cualquiera del BRFSS, solo para que /docs traiga el cuerpo ya
# lleno y el endpoint se pueda probar desde el navegador.
EJEMPLO = {
    "GenHlth": 3, "Age": 9, "DiffWalk": 0, "HighBP": 1, "Stroke": 0,
    "PhysHlth": 5, "HighChol": 1, "Diabetes": 0, "Income": 6, "Smoker": 1,
}

estado: dict = {}


def cargar_variables(ruta: Path) -> list[str]:
    seleccion = pd.read_csv(ruta)
    return seleccion.loc[seleccion["seleccionada"], "variable"].tolist()


def cargar_modelo(dir_modelos: Path):
    """Busca el modelo guardado mas reciente y lo carga con la clase que toca.

    Por fecha y no alfabeticamente: la etapa 4 guarda solo al ganador, pero si
    en otra corrida gano otro modelo la carpeta vieja sigue ahi.
    """
    candidatos = sorted(
        (d for d in dir_modelos.iterdir()
         if d.is_dir() and (d / "metadata").is_dir()),
        key=lambda d: d.stat().st_mtime,
        reverse=True,
    )
    if not candidatos:
        raise RuntimeError(
            f"No hay ningun modelo en {dir_modelos}. Corre antes la etapa de "
            f"entrenamiento: `docker compose up pyspark`."
        )

    dir_modelo = candidatos[0]
    archivo = next((dir_modelo / "metadata").glob("part-*"))
    clase_java = json.loads(archivo.read_text().splitlines()[0])["class"]

    clase = CLASES.get(clase_java)
    if clase is None:
        raise RuntimeError(f"Modelo no soportado por la API: {clase_java}")

    return dir_modelo.name, clase.load(str(dir_modelo))


@asynccontextmanager
async def lifespan(app: FastAPI):
    spark = (
        SparkSession.builder
        .appName("clinical-bigdata-api")
        
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    estado["spark"] = spark
    estado["variables"] = cargar_variables(RUTA_SELECCION)
    estado["nombre"], estado["modelo"] = cargar_modelo(DIR_MODELOS)
    print(f"Modelo cargado: {estado['nombre']}  |  variables: {estado['variables']}")

    yield

    spark.stop()


app = FastAPI(
    title="Clinical Big Data Pipeline - API",
    description="Expone el modelo MLlib que entreno la etapa 4 del pipeline.",
    version="0.1.0",
    lifespan=lifespan,
)


class Paciente(BaseModel):
    """Un encuestado: el valor de cada variable seleccionada."""

    features: dict[str, float] = Field(..., examples=[EJEMPLO])


@app.get("/")
def info() -> dict:
    """Que modelo quedo cargado y que espera recibir."""
    return {
        "modelo": estado["nombre"],
        "variables": estado["variables"],
        "objetivo": "HeartDiseaseorAttack",
        "endpoint": "POST /predict",
        "documentacion": "/docs",
    }


@app.post("/predict")
def predict(paciente: Paciente) -> dict:
    """Clasifica un encuestado: 1 = infarto o enfermedad coronaria."""
    variables = estado["variables"]

    faltantes = [v for v in variables if v not in paciente.features]
    if faltantes:
        raise HTTPException(422, f"Faltan variables: {faltantes}")

    # El orden lo manda `variables`, no el del JSON que llego
    fila = [float(paciente.features[v]) for v in variables]
    sdf = estado["spark"].createDataFrame([(Vectors.dense(fila),)], ["features"])
    prediccion = estado["modelo"].transform(sdf).first().asDict()

    respuesta = {
        "modelo": estado["nombre"],
        "prediccion": int(prediccion["prediction"]),
        "riesgo": bool(prediccion["prediction"] == 1),
    }
    # LinearSVC no estima probabilidades: solo tiene margen (rawPrediction)
    if "probability" in prediccion:
        respuesta["probabilidad"] = round(float(prediccion["probability"][1]), 4)
    return respuesta

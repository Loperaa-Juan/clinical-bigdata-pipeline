"""Etapa 6: las tres visualizaciones que pide la guia.

Todas salen de DataFrames agregados de pocas filas. El assert es la evidencia
literal de que no se convierten los datos masivos completos a pandas.
"""

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

# Backend sin ventanas: dentro del contenedor no hay display
matplotlib.use("Agg")


def _verificar_agregado(df: pd.DataFrame) -> None:
    assert len(df) < 500, (
        f"{len(df)} filas: solo se grafica desde resultados agregados"
    )


def figura_gradiente(tasas: pd.DataFrame, destino: Path) -> Path:
    _verificar_agregado(tasas)

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(tasas["PuntajeSaludCV"], tasas["TasaPct"], color="#c0392b", alpha=0.85)
    ax.set_xlabel("PuntajeSaludCV (0 = ningun factor saludable, 7 = todos)")
    ax.set_ylabel("Prevalencia de enfermedad cardiaca (%)")
    ax.set_title(
        f"Gradiente de riesgo segun Life's Simple 7 (AHA): "
        f"{tasas['TasaPct'].iloc[0]:.2f}% -> {tasas['TasaPct'].iloc[-1]:.2f}%"
    )
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    ruta = destino / "pipeline_gradiente_riesgo.png"
    fig.savefig(ruta, dpi=130)
    plt.close(fig)
    return ruta


def figura_variables(seleccion: pd.DataFrame, prevalencias: pd.DataFrame,
                     destino: Path) -> Path:
    _verificar_agregado(seleccion)
    _verificar_agregado(prevalencias)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Izquierda: la seleccion que hizo Dask por correlacion
    orden = seleccion.iloc[::-1]
    colores = ["#2980b9" if s else "#bdc3c7" for s in orden["seleccionada"]]
    ax1.barh(orden["variable"], orden["correlacion"], color=colores)
    ax1.axvline(0, color="black", linewidth=0.8)
    ax1.set_xlabel("Correlacion con el objetivo")
    ax1.set_title(
        f"Seleccion de variables en Dask\n"
        f"(en azul las {int(seleccion['seleccionada'].sum())} elegidas)"
    )
    ax1.grid(axis="x", alpha=0.3)

    # Derecha: el contraste entre tener y no tener cada factor, calculado en
    # Spark sobre RDDs con flatMap + reduceByKey
    tabla = prevalencias.pivot(index="variable", columns="valor",
                               values="TasaPct").dropna()
    tabla["brecha"] = (tabla[1] - tabla[0]).abs()
    tabla = tabla.sort_values("brecha").tail(12)

    posiciones = range(len(tabla))
    ax2.barh([p - 0.2 for p in posiciones], tabla[0], height=0.4,
             label="Valor 0 (no)", color="#95a5a6")
    ax2.barh([p + 0.2 for p in posiciones], tabla[1], height=0.4,
             label="Valor 1 (si)", color="#c0392b")
    ax2.set_yticks(list(posiciones))
    ax2.set_yticklabels(tabla.index)
    ax2.set_xlabel("Prevalencia de enfermedad cardiaca (%)")
    ax2.set_title("Prevalencia por factor, en Spark\n(flatMap + reduceByKey)")
    ax2.legend()
    ax2.grid(axis="x", alpha=0.3)

    fig.tight_layout()
    ruta = destino / "pipeline_variables.png"
    fig.savefig(ruta, dpi=130)
    plt.close(fig)
    return ruta


def figura_motores(tabla: pd.DataFrame, destino: Path) -> Path:
    _verificar_agregado(tabla)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))
    colores = ["#f39c12", "#16a085"]

    ax1.bar(tabla["motor"], tabla["tiempo_mediano_s"], color=colores)
    for i, valor in enumerate(tabla["tiempo_mediano_s"]):
        ax1.text(i, valor, f"{valor:.3f}s", ha="center", va="bottom")
    ax1.set_ylabel("Tiempo mediano (s)")
    ax1.set_title("Misma agregacion en los dos motores")
    ax1.grid(axis="y", alpha=0.3)

    ax2.bar(tabla["motor"], tabla["rss_pico_mb"], color=colores)
    for i, valor in enumerate(tabla["rss_pico_mb"]):
        ax2.text(i, valor, f"{valor:.0f} MB", ha="center", va="bottom")
    ax2.set_ylabel("RSS pico del arbol de procesos (MB)")
    ax2.set_title("Memoria")
    ax2.grid(axis="y", alpha=0.3)

    fig.tight_layout()
    ruta = destino / "pipeline_comparacion_motores.png"
    fig.savefig(ruta, dpi=130)
    plt.close(fig)
    return ruta

__version__ = "0.1.0"

# A proposito no se importa nada pesado aca: `import clinical_bigdata_pipeline`
# no debe arrastrar pyspark (que tarda segundos y exige una JVM) ni dask. Cada
# modulo se importa donde se usa.

__all__ = ["__version__"]

# movies-recommender

This project contains examples of applications in Spark + illustrate a way of doing movie recommendations based on collaborative user feedback and using only Spark.

## Estructura de proyecto

```bash
examples/                        # folder with python scripts used as local/aws-docker entrypoints
recommender/                    # contains `local_env.json` file used to simulate required environment variables while running the pipeline locally
application.yaml            # Pipeline configuration file. See `Configuration file` below more details

src/cpa_sqc_app/
├── input/                  # folder with context normalization logic
│   ├── querybuilder/       # folder different implementations of a query builder - a query builder normalizes the context input into a SQC Query
├── kpi/                    # folder with key-phrases identification logic
├── search/                 # folder with search logic
├── reranking/              # folder with reranking logic - see reranking/README.md for more info.
│   ├── features/           # folder with features generation logic
│   ├── train/              # folder with XGBoost model related tasks
├── eval/                   # folder with performance statitistics calculation logic
├── util/                   # folder with misc scripts for various general tasks
├── api.py                  # SearchQueryConstructor API
├── pipeline.py             # Runtime pipeline modes orchestration
├── main.py                 # Runtime pipeline starter script
```

## Prerequisitos

- Java 8
- Python 3.8
- Spark 3

### Instrucciones para Mac OS:

- https://medium.com/luckspark/installing-spark-2-3-0-on-macos-high-sierra-276a127b8b85

    **Nota**: Aunque las versiones en el post son diferentes, los pasos para la instalación debe ser los mismos.

### Instrucciones para Windows:

TBD

### Troubleshooting

1. Conflictos al instalar XCode Command Line Tools en Mac:

    Ejecuta desde la consola - este comando instalará el XCode IDE + Command Line Tools (lo cual funciona si tienes espacio suficiente en tu Mac):
    ```
    $ xcode-select –install
    ```
   
    Otra alternativa es simplemente bajar e instalar la version que necesitemos de `Command Line Tools`, esto lo puedes hacer de manera manual desde el [Developer Porta / More Downloads](https://developer.apple.com/download/all/?q=xcode).





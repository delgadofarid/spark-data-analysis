# Spark Data Analysis

This project contains examples of applications in Spark, plus it includes a script showcasing a way of doing movie recommendations using item-based collaborative filtering with Spark.

## Project structure

```bash
examples/                       # folder with python scripts used as examples
├── generate-data.py            # script to generate sample data required by all the examples scripts under this folder
├── order-analysis-rdd.py       # script to generate insights from sample data using RDDs
├── order-analysis-sql.py       # script to generate insights from sample data using SQL (DataFrames)

recommender/                    # contains python scripts for doing movie recommendation
├── movies-recommendation.py    # script containing logic to recommend movies using item-based collaborative filtering with Spark
```

## Requirements

- Java 8
- Python 3.8
- Spark 3

### Instructions for Mac OS:

- https://medium.com/luckspark/installing-spark-2-3-0-on-macos-high-sierra-276a127b8b85

    **Note**: Although the versions in the post are different, the installation steps should be the same.

### Instructions for Windows:

- https://kontext.tech/column/spark/450/install-spark-300-on-windows-10

### Troubleshooting

1. Conflicts when installing `XCode Command Line Tools` on Mac:

    Run from the console - this command will install the XCode IDE + Command Line Tools (which works if you have enough space on your Mac):
    ```
    $ xcode-select –install
    ```
   
    Another alternative is to simply download and install the version of `Command Line Tools` that we need: this can be done manually from the [Apple Developer Portal / More Downloads] (https://developer.apple.com/download/all/? q = xcode).





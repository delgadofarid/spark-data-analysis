# -------------------------- EJERCICIO --------------------------
# Crear motor de recomendaciones de películas usando un conjunto 
# de datos de calificaciones (ratings) realizadas por usuarios a 
# películas disponible en https://grouplens.org/datasets/movielens/
#
# Nombre del conjunto de datos: MovieLens Latest Datasets - Small
#
# Dada una película en el conjunto de datos, el motor debe 
# recomendar las 10 películas más parecidas
#
# Pasos:
# 1. Descargar y descomprimir el conjunto de datos
# 2. Entender el formato de los datos en `ratings.csv` y 
#    `movies.csv`
# 3. Leer archivos (inferir schema) en dos dataframes, uno para 
#    las calificaciones (`ratings.csv`) y otro para las 
#    películas (`movies.csv`)
# 4. Construye un nuevo dataframe con todos los pares 
#    de película vistas por usuario: para esto realizar 
#    self-join del dataframe de calificaciones (ratings) 
#    por el campo `userId` y asegurandose que no existan 
#    duplicados del mismo par de películas. 
#    
#    El nuevo dataset debe contener movieId y rating de 
#    ambas películas:
#
#    eg. [
#       (userId1, movieId1, movieId2, rating1=5.0, rating2=5.0),
#       (userId2, movieId1, movieId2, rating1=1.0, rating2=5.0),
#       (userId3, movieId1, movieId2, rating1=3.0, rating2=5.0),
#       ...
#    ]
# 5. Agrupar dataframe por pares de IDs de películas,
#    y calcular cosine similarity con los valores de 
#    calificaciones (ratings) agrupados. Al mismo tiempo, 
#    mantener el conteo del número de filas agrupadas 
#    por grupo
#   
#    eg. Agrupando las filas con movieId1 y movieId2:
#    // ignoramos el userId
#    [
#       (movieId1=100, movieId2=200, rating1=5.0, rating2=5.0),
#       (movieId1=100, movieId2=200, rating1=1.0, rating2=5.0),
#       (movieId1=100, movieId2=200, rating1=3.0, rating2=5.0),
#       ...
#    ]
#   
#   A = [5.0, 1.0, 3.0]
#   B = [5.0, 5.0, 5.0]
#
#   numeroPares = 3
#   simcosNumerador = (5.0 * 5.0) + (1.0 * 5.0) + (3.0 * 5.0)
#   simcosDenominador = sqrt((5.0 * 5.0) + (1.0 * 1.0) + (3.0 * 3.0)) * sqrt((5.0 * 5.0) + (5.0 * 5.0) + (5.0 * 5.0))
#   simcos = simcosNumerador / simcosDenominador
#   
#   nuevo dataframe: [
#       (movieId1=100, movieId2=200, simcos = 0.87, numeroPares = 3),
#       ...
#   ]
# 6. Crear programa que reciba como input el ID de la pelicula
#    y devuelva al usuario el top 10 usando el dataframe creado
#    en el paso anterior
#    
#    Antes de mostrar los resultados al usuario, debemos 
#    asegurarnos de definir thresholds para el valor de la
#    similitud coseno y el número de ocurrencias con el
#    fin de mostrar resultados en los que se tenga un
#    grado "decente" de confianza
# ---------------------------------------------------------------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("MoviesRecommender").master("local[*]").getOrCreate()

# TODO: Lectura de conjutos de datos

movies = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/u6104617/Desktop/misc/MinTic/bigdata_with_spark/lab/ml-latest-small/movies.csv")

movieNames = {row.movieId: row.title for row in movies.collect()}

ratings = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/u6104617/Desktop/misc/MinTic/bigdata_with_spark/lab/ml-latest-small/ratings.csv")

# TODO: Crear base de datos de películas similares

# Despues del Self-Join:
# user1, movie1, movie1
# user1, movie2, movie2
# user1, movie2, movie1
# user1, movie1, movie2  # solo nos quedamos con uno

print(f"Antes del self-join: {ratings.count()}")

moviePairs = ratings.alias("r1")\
    .join(ratings.alias("r2"), 
          (F.col("r1.userId") == F.col("r2.userId")) & 
          (F.col("r1.movieId") < F.col("r2.movieId")))

print(f"Despues del self-join: {moviePairs.count()}")

moviePairs = moviePairs.select(
    F.col("r1.movieId").alias("movieId1"),
    F.col("r2.movieId").alias("movieId2"),
    F.col("r1.rating").alias("rating1"),
    F.col("r2.rating").alias("rating2")
)

moviePairsWithMetrics = moviePairs\
    .withColumn("A^2", F.pow("rating1", F.lit(2)))\
    .withColumn("B^2", F.pow("rating2", F.lit(2)))\
    .withColumn("A*B", F.col("rating1") * F.col("rating2"))

similarityDatabase = moviePairsWithMetrics\
    .groupBy("movieId1", "movieId2")\
    .agg(
        F.sum("A*B").alias("numerator"),
        (F.sqrt(F.sum("A^2")) * F.sqrt(F.sum("B^2"))).alias("denominator"),
        F.count("A*B").alias("userCount")
    )

similarityDatabase = similarityDatabase\
    .withColumn("cosSim", 
                F.when(F.col("denominator") != 0, F.col("numerator") / F.col("denominator"))\
                 .otherwise(F.lit(0)))\
    .select("movieId1", "movieId2", "cosSim", "userCount")\
    .cache()

import sys
if len(sys.argv) > 1:

    movieId = int(sys.argv[1])
    MIN_USER_NUM = 50

    # TODO:
    # 1. Buscar en base de datos de películas similares a `movieId`

    foundMovies = similarityDatabase\
        .filter(((F.col("movieId1") == movieId) | (F.col("movieId2") == movieId)) &
                (F.col("userCount") >= MIN_USER_NUM))\
        .sort(F.desc("cosSim"))\
        .take(10)
        
    # 2. Mostrar el top 10 de peliculas más parecidas ordenadas por relevancia

    for pos, row in enumerate(foundMovies, 1):

        similarMovieId = row.movieId1 if row.movieId1 != movieId \
                                      else row.movieId2

        print(f"{pos}. {movieNames[similarMovieId]}\t\tSimilitud: {row.cosSim} - NumUsuarios: {row.userCount}")

spark.stop()
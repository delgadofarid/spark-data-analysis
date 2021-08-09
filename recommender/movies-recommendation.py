# -------------------------- EJERCICIO --------------------------
# Crear motor de recomendaciones de películas similares a partir 
# de un conjunto de datos de calificaciones de películas 
# (ratings) en https://grouplens.org/datasets/movielens/
#
# Nombre del conjunto de datos: MovieLens Latest Datasets - Small
#
# Dada una película en el conjunto de datos, el motor debe 
# recomendar las 10 películas más parecidas
#
# Pasos:
# 1. Descargar y descomprimir el conjunto de datos
# 2. Entender el formato de los datos
# 3. Leer archivos (inferir schema) en dos dataframes, uno para 
#    ratings y otro para movies
# 4. Construye un nuevo dataframe con todos los pares 
#    de película vistas por usuario: para esto realizar 
#    self-join del dataframe de ratings, eliminando 
#    duplicados, y manteniendo ambos, id y rating de ambas
#    películas
# 5. Agrupar dataframe con los pares de peliculas, por
#    las columnas de IDs, calcular cosine similarity con
#    los valores de ratings agrupados y también contar
#    el número de pares del mismo en cada grupo
#    
#    eg. [
#       (movieId1, movieId2, rating1=5.0, rating2=5.0),
#       (movieId1, movieId2, rating1=1.0, rating2=5.0),
#       (movieId1, movieId2, rating1=3.0, rating2=5.0),
#       ...
#    ]
#   
#   numeroPares = 3
#   simcosNumerador = (5.0 * 5.0) + (1.0 * 5.0) + (3.0 * 5.0)
#   simcosDenominador = sqrt((5.0 * 5.0) + (1.0 * 1.0) + (3.0 * 3.0)) * sqrt((5.0 * 5.0) + (5.0 * 5.0) + (5.0 * 5.0))
#   simcos = simcosNumerador / simcosDenominador
#   
#   nuevo dataframe: [
#       (movieId1, movieId2, simcos = 0.87, numeroPares = 3),
#       ...
#   ]
#   
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
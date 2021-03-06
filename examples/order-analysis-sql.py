# --------------------------- EJEMPLO ---------------------------
# Dado los datasets `customer-orders.csv` y `customer-names.csv`
# calcular:
#
# - Total número de ordenes por cliente e identificar 
#   top 10 clientes con más ordenes
#
# - Valor total de ordenes por cliente e identificar 
#   top 10 clientes con mayor valor invertido
# ---------------------------------------------------------------

# importamos paquetes requeridos
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import pyspark.sql.functions as F

# creamos nuestra session de Spark
spark = SparkSession.builder.master("local").appName("SQLDataframeExample").getOrCreate()

# definimos la estructura de mi conjunto de datos de ordenes (similar a un schema de base de datos)
orderSchema = StructType([
    StructField("customerId", IntegerType(), True),
    StructField("itemId", IntegerType(), True),
    StructField("value", FloatType(), True)
])

# definimos la estructura del conjunto de datos de clientes con sus nombres
customerSchema = StructType([
    StructField("customerId", IntegerType(), True),
    StructField("customerName", StringType(), True)
])


# lectura de conjunto de datos a un Dataframe
ordersDF = spark.read.schema(orderSchema).csv("file:///Users/u6104617/Desktop/misc/MinTic/bigdata_with_spark/lab/spark-data-analysis/examples/customer-orders.csv")
customersDF = spark.read.schema(customerSchema).csv("file:///Users/u6104617/Desktop/misc/MinTic/bigdata_with_spark/lab/spark-data-analysis/examples/customer-names.csv")

# agrupamos por la columna `customerId`
# usamos dos funciones de aggregacion sobre la misma 
# columna `value` para crear dos nuevas columnas con:
# - orderCount: COUNT("value") cuenta de todos los elementos agrupados
# - totalValue: SUM("value") suma de todos los valores agrupados en la columna `value` 

# con SQL:
# ordersDF.createOrReplaceTempView("orders")
# totalByCustomer = spark.sql("SELECT customerId, COUNT(value) AS orderCount, SUM(value) AS totalValue "
#                             "FROM orders GROUP BY customerId")

# con funciones (functional programming):
# - orderCount: F.count("value") cuenta de todos los elementos agrupados
# - totalValue: F.sum("value") suma de todos los valores agrupados en la columna `value` 
totalByCustomer = ordersDF.groupBy("customerId")\
                          .agg(F.count("value").alias("orderCount"), 
                               F.sum("value").alias("totalValue"))


# agregamos columna con el nombre del cliente
totalByCustomerWithNames = totalByCustomer.join(customersDF, "customerId")

# ordenamos por numero total de ordenes `orderCount` de manera ascendente
# escogemos los primeros 10 resultados
topCustomersByNumOrders = totalByCustomerWithNames.sort(F.desc("orderCount")).take(10)

# realizamos la misma operación pero con `totalValue`
topCustomersByTotalValue = totalByCustomerWithNames.sort(F.desc("totalValue")).head(10)

print("\ntop 10 clientes con más ordenes - SIN FORMATO:")
print(topCustomersByNumOrders)

print("\ntop 10 clientes con más valor invertido - CON FORMATO:")
for pos, row in enumerate(topCustomersByNumOrders, start=1):
    print(f"{pos}. {row.customerName}: ${row.totalValue:.2f}")

# eliminamos session
spark.stop()

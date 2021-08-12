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
from pyspark import SparkConf, SparkContext

# creamos nuestro contexto de Spark (local = 1 cpu, 1 thread)
conf = SparkConf().setMaster("local").setAppName("RDDExample")
sc = SparkContext(conf = conf)

# función para formatear cada línea de texto en nuestro conjunto de datos
def parseLine(line):
    """Convierte cada línea de texto a una Tupla
    conteniendo solo el ID del cliente, Tupla 
    compuesta de una unidad (útil para conteo de 
    número de ordenes) y el valor de la orden
    
    ejemplo:
        76,2487,25.79  -> (76, (1, 25.79))
        87,4821,83.47  -> (87, (1, 83.47))
        2,1520,91.26   -> ( 2, (1, 91.26))
        98,2732,39.99  -> (98, (1, 39.99))
    """
    fields = line.split(',')
    return (int(fields[0]), (1, float(fields[2])))

# lectura de conjunto de datos a un RDD
ordersInput = sc.textFile("file:///path/to/customer-orders.csv")
namesInput = sc.textFile("ile:///path/to//customer-names.csv")

# mapeo de líneas en nuestro RDD de ordenes a una nueva estructura -> (id_cliente, (1, valor_orden))
mappedOrdersInput = ordersInput.map(parseLine)

# agrupamos por llave, que en nuestro caso sería el primer elemento 
# de cada tupla en `mappedOrdersInput` (ID del cliente)
#
# Ejemplo:
# Tupla_1: (10, (1, 20.50))
# Tupla_2: (10, (1, 65.20))
# ---> x: (1, 20.50)
# ---> y: (1, 65.20)
# Tupla_final: Result: (2, 85.70)
# reduceByKey assigna tupla de vuelta al correspondiente ID del cliente: 
# eg. (10, (2, 85.70))
totalByCustomer = mappedOrdersInput.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# mapeo de líneas en nuestro RDD de nombres a una nueva estructura: (id_cliente, nombre_cliente)
# eg. (10, "Kris Connie")
customerNames = namesInput.map(lambda x: (int(x.split(",")[0]), x.split(",")[1]))

# unimos (join) totalByCustomer con customerNames:
# eg. (10, ((2, 85.70), "Kris Connie"))
totalByCustomerNamesJoined = totalByCustomer.join(customerNames)

# mantenemos solo el nombre del cliente y la tupla de valores de interés
# eg. (10, ((2, 85.70), "Kris Connie")) -> ((2, 85.70), "Kris Connie")
totalByCustomerWithNames = totalByCustomerNamesJoined.map(lambda x: x[1])

# reemplazamos llave por numero total de ordenes y valor por ID del cliente
# y ordenamos por numero total de ordenes asc
# eg. ((2, 85.70), "Kris Connie") -> (2, "Kris Connie")
numOrdersByCustomer = totalByCustomerWithNames.map(lambda x: (x[0][0], x[1]))
numOrdersByCustomerSorted = numOrdersByCustomer.sortByKey(ascending=False)

# realizamos la misma operación pero esta vez reemplazamos la llave por
# el valor total de todas las ordenes del cliente
# eg. ((2, 85.70), "Kris Connie") -> (85.70, "Kris Connie")
totalValueByCustomerSorted = totalByCustomerWithNames.map(lambda x: (x[0][1], x[1]))\
                                                     .sortByKey(ascending=False)

# tomamos los primeros 10 resultados de cada RDD
topCustomersByNumOrders = numOrdersByCustomerSorted.take(10)
topCustomersByTotalValue = totalValueByCustomerSorted.top(10)

print("\ntop 10 clientes con más ordenes - SIN FORMATO:")
print(topCustomersByNumOrders)

print("\ntop 10 clientes con más valor invertido - CON FORMATO:")
for pos, result in enumerate(topCustomersByTotalValue, start=1):
    print(f"{pos}. {result[1]}: ${result[0]:.2f}")

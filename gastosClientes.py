# Importing libs to use with python
from pyspark import SparkContext, SparkConf

# Creating sparkContext.
conf = SparkConf().setAppName("TotalSpend").set("master","local")
sc = SparkContext(conf = conf)

# Loading data from HDFS:
#inputdataset = sc.textFile("hdfs://user/dados/gastosCliente/gastos-cliente.csv")

# Loading data from Local Directory:
inputdataset = sc.textFile("/home/hadoop/miniprojeto3/gastos-cliente.csv")


# Map Function splitting data by common
# Position [0]: ID Client
# Position [2]: Value Spent
def mapClient(line):
        line_split = line.split(',')
        return (int(line_split[0]), float(line_split[2]))

mappedResult = inputdataset.map(mapClient)

# Reduce Function: Sum values by Key (ID Client)
totalByClient = mappedResult.reduceByKey(lambda x, y: x + y)

# Save As File Local Directory
totalByClient.saveAsTextFile("/home/hadoop/miniprojeto3/saida")

# Save as File HDFS
totalByClient.saveAsTextFile("hdfs://user/dados/gastosCliente/saida")

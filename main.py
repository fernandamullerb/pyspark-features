import os
import pandas
import findspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# CONFIGURANDO VARIÁVEIS DE AMBIENTE
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-17.0.1"
os.environ["SPARK_HOME"] = "C:/Program Files/Spark/spark-3.5.0-bin-hadoop3"
os.environ["HADOOP_HOME"] = "C:/Program Files/Hadoop"
findspark.init()

# INICIANDO SESSÃO DO SPARK
spark = (
    SparkSession.builder
    .master('local')
    .appName('PySpark')
    .getOrCreate()
)

# LENDO ARQUIVO
df = spark.read.csv('arquivos/wc2018-players.csv', header=True, inferSchema=True)

# VISUALIZANDO ARQUIVO
df.show(5)
df.printSchema()

# RENOMEANDO COLUNAS
df = df.withColumnRenamed('Pos.', 'Posicao')

# VERIFICANDO QUANTOS DADOS NULOS HÁ EM CADA COLUNA
for coluna in df.columns:
    print(coluna, df.filter(df[coluna].isNull()).count())

# SELECIONANDO COLUNAS
df.select('FIFA Popular Name', 'Posicao', 'Team').show(5)
df.select(col('Team').alias('Time')).show(5) # com alias

# FILTRANDO COLUNAS COM CONDIÇÃO "E"
df.filter(col('Team') == 'Brazil').show(10)
df.filter((col('Team') == 'Argentina') & (col('Height') > 180) & (col('Weight') >= 85)).show()
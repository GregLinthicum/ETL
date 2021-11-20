...
Predecesor in the same Notebook
import findspark
findspark.init('C:\SparkForWindows\spark-3.2.0-bin-hadoop3.2')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.context import SparkContext
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql.types import DecimalType, StructField, StructType,FloatType
...


import pyspark
sc = SparkContext.getOrCreate()
from pyspark import SparkContext
from pyspark.sql import SQLContext
sql = SQLContext(sc)

from pyspark.sql import SparkSession

df = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").option("delimiter", ",") \
          .load("C:\SparkForWindows\GregsData\SampleCSVFile_556kb.csv")

df.show(truncate=False)


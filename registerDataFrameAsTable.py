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
from pyspark import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf
from pyspark.sql import Row


conf = pyspark.SparkConf() 

 
sc = pyspark.SparkContext.getOrCreate(conf=conf)
spark = SQLContext(sc)

schema = StructType([
    StructField("sales", FloatType(),True),    
    StructField("employee", StringType(),True),
    StructField("ID", IntegerType(),True)
])

data = [[ 10.2, "Fred",123],[ 20.2, "Lary",223]]

df = spark.createDataFrame(data,schema=schema)
df.show()

colsInt = udf(lambda z: toInt(z), IntegerType())
spark.udf.register("colsInt", colsInt)

def toInt(s):
    if isinstance(s, str) == True:
        st = [str(ord(i)) for i in s]
        return(int(''.join(st)))
    else:
         return Null


df2 = df.withColumn( 'semployee',colsInt('employee')).withColumn( 'ID2', F.col('ID')).withColumn( 'ID3', F.col('ID2')+5 )
df2.show()

df2 = df.withColumn( 'semployee',  concat(col('employee'), lit('-AA')) ).withColumn( 'ID2', F.col('ID')).withColumn( 'ID3', F.col('ID2')+5 )
df2.show()

spark.registerDataFrameAsTable(df2, "dftab")
df3 = spark.sql("select sales, employee, ID, colsInt(employee) as iemployee from dftab")
df3.show()

df4 = spark.sql("select employee, sum(sales)as sumSal from dftab group by employee")
df4.show()

print (" **** CORRELATED SUBQUERY :  SELECT employee, COALESCE ((SELECT Max(sales) FROM dftab2 WHERE dftab.employee = dftab2.employee), 0) AS maxSales from dftab  **** ")
spark.registerDataFrameAsTable(df2, "dftab2")
df7 = spark.sql("SELECT employee, COALESCE ((SELECT Max(sales) FROM dftab2 WHERE dftab.ID = dftab2.ID), 0) AS maxSales from dftab")
df7.show()


================

+-----+--------+---+
|sales|employee| ID|
+-----+--------+---+
| 10.2|    Fred|123|
| 20.2|    Lary|223|
| 30.2|    Fred|523|
+-----+--------+---+

+-----+--------+---+----------+---+---+
|sales|employee| ID| semployee|ID2|ID3|
+-----+--------+---+----------+---+---+
| 10.2|    Fred|123|1394624364|123|128|
| 20.2|    Lary|223|-892820471|223|228|
| 30.2|    Fred|523|1394624364|523|528|
+-----+--------+---+----------+---+---+

+-----+--------+---+---------+---+---+
|sales|employee| ID|semployee|ID2|ID3|
+-----+--------+---+---------+---+---+
| 10.2|    Fred|123|  Fred-AA|123|128|
| 20.2|    Lary|223|  Lary-AA|223|228|
| 30.2|    Fred|523|  Fred-AA|523|528|
+-----+--------+---+---------+---+---+

+-----+--------+---+----------+
|sales|employee| ID| iemployee|
+-----+--------+---+----------+
| 10.2|    Fred|123|1394624364|
| 20.2|    Lary|223|-892820471|
| 30.2|    Fred|523|1394624364|
+-----+--------+---+----------+

+--------+------------------+
|employee|            sumSal|
+--------+------------------+
|    Fred| 40.40000057220459|
|    Lary|20.200000762939453|
+--------+------------------+

 **** CORRELATED SUBQUERY :  SELECT employee, COALESCE ((SELECT Max(sales) FROM dftab2 WHERE dftab.employee = dftab2.employee), 0) AS maxSales from dftab  **** 
+--------+--------+
|employee|maxSales|
+--------+--------+
|    Lary|    20.2|
|    Fred|    30.2|
|    Fred|    10.2|
+--------+--------+

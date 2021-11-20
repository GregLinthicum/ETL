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
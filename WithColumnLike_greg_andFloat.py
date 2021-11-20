import findspark
findspark.init('C:\SparkForWindows\spark-3.2.0-bin-hadoop3.2')
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.context import SparkContext
## from pyspark.sql.session import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
from pyspark.sql.types import DecimalType, StructField, StructType,FloatType

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","36636","M",3000, "3321 Coolbrook"),
    ("Michael","Rose","","40288","M",4000,"632 Crispy spring-field road"),
    ("Robert","","Williams","42114","M",4000,"6632 Long Sault precopice"),
    ("Maria","Anne","Jones","39192","F",4000,"Steep Lake"),
    ("Jen","Mary","Brown","","F",-1, "2167 Three rodent triumvirate")
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True), \
    StructField("address", StringType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)

df = df.withColumn(
    'address',
    F.when(
        F.col('address').like('%spring-field_%'),
        F.lit('spring-field')
    ).otherwise(F.col('address'))
)
df.show()


df = df.withColumn("salary", df["salary"].cast( FloatType() ))
df.show()


==================
root
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)
 |-- address: string (nullable = true)

+---------+----------+--------+-----+------+------+-----------------------------+
|firstname|middlename|lastname|id   |gender|salary|address                      |
+---------+----------+--------+-----+------+------+-----------------------------+
|James    |          |Smith   |36636|M     |3000  |3321 Coolbrook               |
|Michael  |Rose      |        |40288|M     |4000  |632 Crispy spring-field road |
|Robert   |          |Williams|42114|M     |4000  |6632 Long Sault precopice    |
|Maria    |Anne      |Jones   |39192|F     |4000  |Steep Lake                   |
|Jen      |Mary      |Brown   |     |F     |-1    |2167 Three rodent triumvirate|
+---------+----------+--------+-----+------+------+-----------------------------+

+---------+----------+--------+-----+------+------+--------------------+
|firstname|middlename|lastname|   id|gender|salary|             address|
+---------+----------+--------+-----+------+------+--------------------+
|    James|          |   Smith|36636|     M|  3000|      3321 Coolbrook|
|  Michael|      Rose|        |40288|     M|  4000|        spring-field|
|   Robert|          |Williams|42114|     M|  4000|6632 Long Sault p...|
|    Maria|      Anne|   Jones|39192|     F|  4000|          Steep Lake|
|      Jen|      Mary|   Brown|     |     F|    -1|2167 Three rodent...|
+---------+----------+--------+-----+------+------+--------------------+

+---------+----------+--------+-----+------+------+--------------------+
|firstname|middlename|lastname|   id|gender|salary|             address|
+---------+----------+--------+-----+------+------+--------------------+
|    James|          |   Smith|36636|     M|3000.0|      3321 Coolbrook|
|  Michael|      Rose|        |40288|     M|4000.0|        spring-field|
|   Robert|          |Williams|42114|     M|4000.0|6632 Long Sault p...|
|    Maria|      Anne|   Jones|39192|     F|4000.0|          Steep Lake|
|      Jen|      Mary|   Brown|     |     F|  -1.0|2167 Three rodent...|
+---------+----------+--------+-----+------+------+--------------------+
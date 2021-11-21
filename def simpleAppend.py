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

from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import col

def struct_from_string(attribute_string):
    attribute_map ={}
    if attribute_string != '':
        attribute_string = attribute_string.split("__") # This will be a list 
        for substring in attribute_string:
            k,v = substring.split("=")
            attribute_map[str(k)] = str(v)
    return attribute_map

df=spark.createDataFrame([('id_1',"department=Sales__title=Sales_executive__level=junior"), \
                          ('id_2' ,"department=Engineering__title=Software Engineer__level=entry-level")], \
                         ['person_id','person_attributes'])

df.show(truncate=False)

my_parse_string_udf = spark.udf.register("my_parse_string", struct_from_string, 
     MapType(StringType(), StringType()))

df2 = df.select(col("person_id"), my_parse_string_udf(col("person_attributes")))

df2.show(truncate=False)


def simpleAppend(attribute_str):
    return attribute_str + "blah blah blah"

my_simpleAppend_udf = spark.udf.register("my_simpleAppend", simpleAppend, StringType())

df3 = df.select( col("person_id"), my_simpleAppend_udf( col("person_id") ) )
df3.show()






+---------+------------------------------------------------------------------+
|person_id|person_attributes                                                 |
+---------+------------------------------------------------------------------+
|id_1     |department=Sales__title=Sales_executive__level=junior             |
|id_2     |department=Engineering__title=Software Engineer__level=entry-level|
+---------+------------------------------------------------------------------+

+---------+-----------------------------------------------------------------------------+
|person_id|my_parse_string(person_attributes)                                           |
+---------+-----------------------------------------------------------------------------+
|id_1     |{title -> Sales_executive, department -> Sales, level -> junior}             |
|id_2     |{title -> Software Engineer, department -> Engineering, level -> entry-level}|
+---------+-----------------------------------------------------------------------------+

+---------+--------------------------+
|person_id|my_simpleAppend(person_id)|
+---------+--------------------------+
|     id_1|        id_1blah blah blah|
|     id_2|        id_2blah blah blah|
+---------+--------------------------+

+---------+------------------------------------------------------------------------------+
|person_id|person_attributes                                                             |
+---------+------------------------------------------------------------------------------+
|id_1     |{{department=Sales }, { title=Sales_executive }, { level=junior}}             |
|id_2     |{{department=Engineering }, { title=Software Engineer }, { level=entry-level}}|
+---------+------------------------------------------------------------------------------+
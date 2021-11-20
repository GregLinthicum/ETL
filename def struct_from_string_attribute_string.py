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

...
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
...
# ETL

def ConvertDateStringToDate (glueContext, dfc) -> DynamicFrameCollection:
    sparkDF = dfc.select(list(dfc.keys())[0]).toDF()
    sparkDF.createOrReplaceTempView("inputTable")

    df = spark.sql("select TO_DATE(CAST(UNIX_TIMESTAMP(date, 'yyyyMMdd') AS TIMESTAMP)) as date, \
                           state , \
                           positiveIncrease ,  \
                           totalTestResultsIncrease \
                    from   inputTable")

    dyf = DynamicFrame.fromDF(df, glueContext, "results")
    return DynamicFrameCollection({"CustomTransform0": dyf}, glueContext)

Transform = ConvertDateStringToDate(glueContext, DynamicFrameCollection({"DataSource0": DataSource0}, glueContext))
resultDF = Transform.select(list(Transform.keys())[0]).toDF()
resultDF.printSchema()
resultDF.show(10)

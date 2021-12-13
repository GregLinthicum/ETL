# Packaging

[Wheel ( .whl)  in newer than setuptools ](https://packaging.python.org/en/latest/glossary/#term-Distribution-Package)

[PDM project Development Manager](https://pypi.org/project/pdm/)


# ETL

=========================================================

When testing Notebooks run the last cel in #11, [JupyterNotebook11](https://github.com/GregLinthicum/ETL/blob/main/CustomTransformFunction2)  that is:
%run ./JupyterNotebook12.ipynb
printTest()
## https://coderedirect.com/questions/501971/running-a-jupyter-notebook-from-another-notebook
## show_notebook('JupyterNotebook12')

==========================================================

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

=====================================================================


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

[Workshop](https://catalog.us-east-1.prod.workshops.aws/v2/workshops/aaaabcab-5e1e-4bff-b604-781a804763e1/en-US/lab6/custom-transformation)

[pipenv  (maven for Python)](https://medium.com/nerd-for-tech/what-is-pipenv-5b552184852)

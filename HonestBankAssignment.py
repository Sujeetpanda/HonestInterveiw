import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col
from pyspark.sql.types import DoubleType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HonestBankAssignment") \
        .getOrCreate()

    logger = Log4j(spark)

    config = configparser.ConfigParser()
    config.read(r'projectconfig/config.ini')
    value = config.get('avg','range')
    print(value)

    config = configparser.ConfigParser()
    config.read(r'projectconfig/config.ini')
    url = config.get('connection','url')
    driver = config.get('connection', 'driver')
    dbtable = config.get('connection', 'dbtable')
    user = config.get('connection', 'user')
    pwd = config.get('connection', 'password')

    surveyDF = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/rejected_2007_to_2018Q4.csv")
    surveyDF.printSchema()
    newDf = surveyDF.withColumnRenamed("Amount Requested","Amount_Requested")\
        .withColumnRenamed("Application Date","Application_Date")\
        .withColumnRenamed("Loan Title","Loan_Title")\
        .withColumnRenamed("Zip Code","Zip_Code")\
        .withColumnRenamed("Debt-To-Income Ratio","Debt-To-Income_Ratio")\
        .withColumnRenamed("Employment Length","Employment_Length")\
        .withColumnRenamed("Policy Code","Policy_Code")
    newDf.printSchema()
    df2007 = newDf.filter(newDf.Application_Date.contains("2007"))
    df2007.createOrReplaceTempView("tab2007")
    df=spark.sql("select *,avg(Risk_Score) OVER(partition by Application_Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW ) as RiskScoreMA50 from tab2007")
    df.show(10, False)
    # Requirement 1
    df.coalesce(1).write.option("header", "true").mode('overwrite').csv("output")

    # df.write.format('jdbc').options(
    #     url=url,
    #     driver=driver,
    #     dbtable='riskscorema50',
    #     user=user,
    #     password=pwd).mode('append').save()
    # print("Completed successfully")
    #insert to mysql
    ###############################################################################

    #Requirement 3 and 4
    newDf=newDf.withColumn('Risk_Score',col('Risk_Score').cast(DoubleType()))
    df0708=newDf.filter(col("Application_Date").contains("2007") | col("Application_Date").contains("2008"))
    df0708_ma50=df0708
    if(value == 'MA100'):
        pandasdf = df0708_ma50.toPandas()
        pandasdf['MA50'] = pandasdf['Risk_Score'].ewm(span=50, adjust=False).mean()
        df0708_ma50 = spark.createDataFrame(pandasdf)
    else:
        df0708_ma50.createOrReplaceTempView("tab2007_08")
        df0708_ma50=spark.sql("select *,avg(Risk_Score) OVER(partition by Application_Date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW ) as MA50 from tab2007_08")
    df009=newDf.exceptAll(df0708)
    df0708.createOrReplaceTempView("tab09")
    dfMA100_09 = spark.sql("select *,avg(Risk_Score) OVER(partition by Application_Date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW ) as MA100 from tab09")

    dfMA50_07_08=df0708_ma50.withColumn("MA100",lit("NA"))
    dfMA100_09=dfMA100_09.withColumn("MA50",lit("NA"))
    finalDf = dfMA50_07_08.unionByName(dfMA100_09)

    finalDf.coalesce(1).write.option("header", "true").mode('overwrite').csv("ma50")

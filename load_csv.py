from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

#session builder is used to start the session and getorcreate is used to find if exists or create a new if it does not exists.
spark = SparkSession.builder \
        .appName("load-csv") \
        .master("local[*]")\
        .getOrCreate()

# reading the csv , header is used to determine the 1 st row is column name & inferschema is used to find the datatypes of each columns.
df=spark.read\
    .option("header",True)\
    .option("inferSchema",True)\
    .csv("data/AAPL.csv")

#returning the schema of data
df.printSchema()

#methods of referencing columns
column1 = df.Close
column2 = df['Close']
column3 = col('Close')

#select works similar to sql select statement.
df.select(column1,column2,column3).show()

#retrieval of columns by select with column names .
df.select("Date","Close","Open").show()

#cast function is used to convert (or cast) the data type of the specified column & alias is used to rename the column name
date_as_string= df['Date'].cast(StringType()).alias("date")   
df.select("Date",date_as_string).show()


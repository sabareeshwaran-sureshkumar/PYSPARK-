from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
        .appName("load-csv") \
        .master("local[*]")\
        .getOrCreate()

def load_data(symbol):

    df=spark.read\
    .option("header",True)\
    .option("inferSchema",True)\
    .csv("data/"+symbol+".csv")
    return df.select(
        df['Date'],
        df['Open'].cast(DoubleType()).alias("open"),
        df['Close'].cast(DoubleType()).alias("close"),
        df['High'].cast(DoubleType()).alias("high"),
        df['Low'].cast(DoubleType()).alias("low")
    )
df=load_data("AAPL")
# df.show()

concat_column=concat(df['Date'],lit(" Monday"))

df.select(df['Date'],concat_column.alias("ChangedDate")).show()
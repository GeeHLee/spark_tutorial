from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg

sc = SparkContext()
sqlContext = SQLContext(sc)
file_path = "data/1987.csv"
# there two way to reading a csv file into spark Data Frame.
# we will use 100% spark cause its more faster:
df = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true').load(file_path)
# see the first 5 rows:
df.take(5)
# see the type of variables:
df.printSchema()
# converter year to string:
df["Year"].cast("string")
# converter year to int:
df["Year"].cast("int")
# create new variable and drop the older one:
df_1 = df.withColumnRenamed("Year", "oldYear")
df_2 = df_1.withColumn("Year", df_1["oldYear"].cast("int")).drop("oldYear")


# if we need to converter multiple variables, we need create a function instead by doing one by one:

def convert_column(df, name, newtype):
    df_tmp = df.withColumnRenamed(name, "swap")
    return df_tmp.withColumn(name, df_tmp["swap"].cast(newtype)).drop("swap")


df_3 = convert_column(df_2, "ArrDelay", "int")
df_4 = convert_column(df_2, "DepDelay", "int")

# Now calculation the average delay time for each plan:
averageDelays = df_4.groupBy(df_4["FlightNum"]).agg(avg(df_4["ArrDelay"]), avg(df_4["DepDelay"]))
averageDelays.cache()
# sort averageDelays:
averageDelays.orderBy("avg(ArrDelay)").show()
averageDelays.orderBy("avg(ArrDelay)", ascending=0).show()
averageDelays.sort("avg(ArrDelay)", "avg(DepDelay)", ascending=[0, 0]).show()
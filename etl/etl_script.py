import os.path
import pyspark
from pyspark.sql import SparkSession
file_path = os.path.abspath("Behavioral_Risk_Factor_Surveillance_System_data.csv")

spark = SparkSession.builder \
    .appName("SQLite Example") \
    .master("local[*]") \
    .getOrCreate()

# // extract this away //
db_path = os.path.abspath("../healtdata")
db_url = f"jdbc:sqlite:{db_path}"

df = spark.read.csv(file_path, header=True, inferSchema=True)
filtered_data = df.filter(df["Year"] > 2014)
filtered_data.show(3)
grouped_data = filtered_data.groupby("Locationdesc", "Class") \
    .agg({"Sample_Size": "sum"}) \
    .withColumnRenamed("sum(Sample_Size)", "Total_Sample_Size")

grouped_data.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("driver", "org.sqlite.JDBC") \
    .option("dbtable", "healthyheart") \
    .mode("overwrite") \

spark.stop()

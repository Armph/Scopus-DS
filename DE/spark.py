from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, concat_ws

spark = SparkSession.builder.appName("Data Processing").getOrCreate()


# Load data
df = spark.read.json("DE/data/*.json")

# Filter data
df = df.dropna()

# Explode subject_areas
df = df.withColumn("subject_areas", explode(col("subject_areas")))

# Flatten author_keywords
df = df.withColumn("author_keywords", concat_ws(",", col("author_keywords")))

# df.printSchema()
# df.show()


df2 = spark.read.csv("DE/source_data/*.csv", header=True)

df2 = df2.dropna()

df2 = df2.withColumn("subject_areas", explode(split(col("subject_areas"), ",")))

# df2.printSchema()
# df2.show()

# Union data
prep_data = df.union(df2)

prep_data.printSchema()
prep_data.show()
print(prep_data.count())

# Save data

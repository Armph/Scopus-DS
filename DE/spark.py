import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, concat_ws


def merge_data():
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
    # ------------------------------------------------------------------------------ #

    df2 = spark.read.csv("DE/source_data/*.csv", header=True)

    df2 = df2.dropna()

    df2 = df2.withColumn("subject_areas", explode(split(col("subject_areas"), ",")))

    df2 = df2.select("author_keywords", "citation_count", "eid", "length_of_abstract", "publication_year", "refcount", "sub_type", "subject_areas")

    # df2.printSchema()
    # df2.show()
    # ------------------------------------------------------------------------------ #

    # Union data
    prep = df.union(df2)

    # prep.printSchema()
    # prep.show()
    # print(prep.count())

    prep.coalesce(1) \
        .write.format("com.databricks.spark.csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("DE/processed_data")

    spark.stop()
    return

def group_data():
    spark = SparkSession.builder.appName("Data Processing").getOrCreate()

    df = spark.read.csv("DE/processed_data/*.csv", header=True)

    df = df.groupBy("publication_year", "subject_areas") \
        .count() \
        .orderBy("publication_year")

    df.coalesce(1) \
        .write.format("com.databricks.spark.csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("DE/grouped_data")

    spark.stop()
    return

if __name__ == "__main__":
    merge_data()
    group_data()

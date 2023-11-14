from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession


def load(dataset="dbfs:/FileStore/extract_fs"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema
    songs_normalize_df = spark.read.csv(dataset, header=True, inferSchema=True)

    # add unique IDs to the DataFrames
    songs_normalize_df = songs_normalize_df.withColumn(
        "id", monotonically_increasing_id()
    )

    # drop table if exists
    spark.sql("DROP TABLE IF EXISTS songs_normalize_df")

    # transform into a delta lakes table and store it
    songs_normalize_df.write.format("delta").mode("overwrite").saveAsTable(
        "songs_normalize_df"
    )
    num_rows = songs_normalize_df.count()
    print("Finished transfrom and load. Number of rows: ", num_rows)

    return songs_normalize_df


if __name__ == "__main__":
    load()

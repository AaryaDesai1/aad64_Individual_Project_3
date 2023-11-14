from pyspark.sql import SparkSession


def execute_queries():
    spark = SparkSession.builder.appName("Query").getOrCreate()

    # Query 1
    query_1 = """
    SELECT *
    FROM songs_normalize_df
    WHERE year = 2000
    """
    result_1 = spark.sql(query_1).show()

    # Query 2
    query_2 = """
    SELECT AVG(danceability) AS avg_danceability, AVG(energy) AS avg_energy
    FROM songs_normalize_df
    WHERE explicit = True
    """
    result_2 = spark.sql(query_2).show()

    # Query 3
    query_3 = """
    SELECT *
    FROM songs_normalize_df
    WHERE popularity > 75
    ORDER BY popularity DESC
    """
    result_3 = spark.sql(query_3).show()

    # Query 4
    query_4 = """
    SELECT genre, COUNT(*) AS song_count
    FROM songs_normalize_df
    GROUP BY genre
    """
    result_4 = spark.sql(query_4).show()

    # Query 5
    query_5 = """
    SELECT genre, AVG(popularity) AS avg_popularity
    FROM songs_normalize_df
    GROUP BY genre
    """
    result_5 = spark.sql(query_5).show()

    return {
        "Query 1": result_1,
        "Query 2": result_2,
        "Query 3": result_3,
        "Query 4": result_4,
        "Query 5": result_5,
    }


if __name__ == "__main__":
    execute_queries()

# Databricks notebook source
!pip install -r ../requirements.txt

# COMMAND ----------

import requests
from dotenv import load_dotenv
import os
import json
import base64

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/'))

# COMMAND ----------

load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("PERSONAL_ACCESS_TOKEN")

# COMMAND ----------

headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://"+server_h+"/api/2.0"

# COMMAND ----------

#Defining functions necessary to add dataset to dbfs:

def perform_query(path, headers, data={}):
  session = requests.Session()
  resp = session.request('POST', url + path, data=json.dumps(data), verify=True, headers=headers)
  return resp.json()

def mkdirs(path, headers):
  _data = {}
  _data['path'] = path
  return perform_query('/dbfs/mkdirs', headers=headers, data=_data)

def create(path, overwrite, headers):
  _data = {}
  _data['path'] = path
  _data['overwrite'] = overwrite
  return perform_query('/dbfs/create', headers=headers, data=_data)

def add_block(handle, data, headers):
  _data = {}
  _data['handle'] = handle
  _data['data'] = data
  return perform_query('/dbfs/add-block', headers=headers, data=_data)

def close(handle, headers):
  _data = {}
  _data['handle'] = handle
  return perform_query('/dbfs/close', headers=headers, data=_data)

def put_file(src_path, dbfs_path, overwrite, headers):
  handle = create(dbfs_path, overwrite, headers=headers)['handle']
  print("Putting file: " + dbfs_path)
  with open(src_path, 'rb') as local_file:
    while True:
      contents = local_file.read(2**20)
      if len(contents) == 0:
        break
      add_block(handle, base64.standard_b64decode(contents).decode(), headers=headers)
    close(handle, headers=headers)

def put_file_from_url(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(handle, base64.standard_b64encode(content[i:i+2**20]).decode(), headers=headers)
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")

# COMMAND ----------

mkdirs(path="dbfs:/FileStore/ind_proj_3", headers=headers)
create(path="dbfs:/FileStore/ind_proj_3/songs.csv", overwrite= True, headers=headers)

# COMMAND ----------

serve_times_url = "https://raw.githubusercontent.com/nogibjj/aad64_PySpark/main/songs_normalize.csv"
serve_times_dbfs_path = "dbfs:/FileStore/ind_proj_3/songs.csv"
overwrite = True  # Set to False if you don't want to overwrite existing file

put_file_from_url(serve_times_url, serve_times_dbfs_path, overwrite, headers=headers)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/ind_proj_3/'))


print(os.path.exists('dbfs:/FileStore/ind_proj_3'))

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read CSV").getOrCreate()

# Define the file paths
songs_path = "dbfs:/FileStore/ind_proj_3/songs.csv"

# Read the CSV files into DataFrames
songs_df = spark.read.csv(songs_path, header=True, inferSchema=True)

# Show the DataFrames
# songs_df.show()
num_rows = songs_df.count()
print(num_rows)

# COMMAND ----------

# save as delta
# transform_load
songs_df.write.format("delta").mode("overwrite").saveAsTable("songs_delta")
spotify_df = spark.read.table("songs_delta")
# spotify_df.show()
num_rows_spotify = spotify_df.count()
print(num_rows_spotify)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/ind_proj_3/'))

# COMMAND ----------

import requests
from pyspark.sql import SparkSession


FILESTORE_PATH = "dbfs:/FileStore/extract_fs"
headers = {'Authorization': 'Bearer %s' % access_token}


def extract(
    url="https://raw.githubusercontent.com/nogibjj/aad64_PySpark/main/songs_normalize.csv",
    file_path=FILESTORE_PATH+"/spotify.csv",
    directory=FILESTORE_PATH,
):
    """Extract a url to a file path"""
    # Making the directory
    mkdirs(path=directory, headers=headers)
    # Adding the csv file
    put_file_from_url(url, file_path, overwrite, headers=headers)

    return file_path

# COMMAND ----------

extract()

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/extract_fs'))

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/extract_fs"):
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    # load csv and transform it by inferring schema 
    songs_normalize_df = spark.read.csv(dataset, header=True, inferSchema=True)

    # add unique IDs to the DataFrames
    songs_normalize_df = songs_normalize_df.withColumn("id", monotonically_increasing_id())

    # drop table if exists
    spark.sql("DROP TABLE IF EXISTS songs_normalize_df")

    # transform into a delta lakes table and store it 
    songs_normalize_df.write.format("delta").mode("overwrite").saveAsTable("songs_normalize_df")
    num_rows = songs_normalize_df.count()
    print("Finished transfrom and load. Number of rows: ", num_rows)
    
    return songs_normalize_df

# COMMAND ----------

load()

# COMMAND ----------

#Independently testing queries before creating function:

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Query").getOrCreate()

# Query 1
query_result_1 = spark.sql("""
    SELECT *
    FROM songs_normalize_df
    WHERE year = 2000
""")
query_result_1.show()

# Query 2
query_result_2 = spark.sql("""
    SELECT AVG(danceability) AS avg_danceability, AVG(energy) AS avg_energy
    FROM songs_normalize_df
    WHERE explicit = True
""")
query_result_2.show()

# Query 3
query_result_3 = spark.sql("""
    SELECT *
    FROM songs_normalize_df
    WHERE popularity > 75
    ORDER BY popularity DESC
""")
query_result_3.show()

# Query 4
query_result_4 = spark.sql("""
    SELECT genre, COUNT(*) AS song_count
    FROM songs_normalize_df
    GROUP BY genre
""")
query_result_4.show()

# Query 5
query_result_5 = spark.sql("""
    SELECT genre, AVG(popularity) AS avg_popularity
    FROM songs_normalize_df
    GROUP BY genre
""")
query_result_5.show()



# COMMAND ----------

from pyspark.sql.functions import when, col
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

def transform_data(dataframe):
    # Handle missing values
    dataframe = dataframe.na.fill(0)  # You may want to use a more sophisticated method based on your data
    
    # Feature Engineering
    dataframe = dataframe.withColumn("duration_minutes", col("duration_ms") / 60000.0)
    
    # Encoding categorical variables
    categorical_cols = ["genre"]
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep") for col in categorical_cols]
    encoder = OneHotEncoder(inputCols=[f"{col}_index" for col in categorical_cols], outputCols=[f"{col}_encoded" for col in categorical_cols])
    
    pipeline = Pipeline(stages=indexers + [encoder])
    dataframe = pipeline.fit(dataframe).transform(dataframe)

    # Drop unnecessary columns
    columns_to_drop = ["duration_ms", "genre"]  # Add other columns you want to drop
    dataframe = dataframe.drop(*columns_to_drop)

    return dataframe

# Example usage
df = load()
transformed_df = transform_data(df)
transformed_df.show()


# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def visualize(dataframe):
    # Convert PySpark DataFrame to Pandas DataFrame
    pandas_df = dataframe.toPandas()

    # Visualization examples using Matplotlib
    plt.figure(figsize=(10, 6))

    # Example 1: Popularity Distribution
    plt.subplot(2, 2, 1)
    pandas_df['popularity'].plot(kind='hist', bins=20, color='blue', edgecolor='black')
    plt.title('Popularity Distribution')
    plt.xlabel('Popularity')
    plt.ylabel('Frequency')

    # Example 2: Danceability vs. Energy Scatter Plot
    plt.subplot(2, 2, 2)
    plt.scatter(x=pandas_df['danceability'], y=pandas_df['energy'], c=pandas_df['explicit'], cmap='viridis')
    plt.title('Danceability vs. Energy')
    plt.xlabel('Danceability')
    plt.ylabel('Energy')

    plt.tight_layout()
    plt.show()

    return "finished visualization"

# Example usage
df = load()
result = visualize(df)
print(result)
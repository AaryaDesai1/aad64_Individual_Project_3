from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from load import load


def transform_data(dataframe):
    # Handle missing values
    dataframe = dataframe.na.fill(
        0
    )  # You may want to use a more sophisticated method based on your data

    # Feature Engineering
    dataframe = dataframe.withColumn("duration_minutes", col("duration_ms") / 60000.0)

    # Encoding categorical variables
    categorical_cols = ["genre"]
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
        for col in categorical_cols
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{col}_index" for col in categorical_cols],
        outputCols=[f"{col}_encoded" for col in categorical_cols],
    )

    pipeline = Pipeline(stages=indexers + [encoder])
    dataframe = pipeline.fit(dataframe).transform(dataframe)

    # Drop unnecessary columns
    columns_to_drop = ["duration_ms", "genre"]  # Add other columns you want to drop
    dataframe = dataframe.drop(*columns_to_drop)

    return dataframe


if __name__ == "__main__":
    df = load()
    transformed_df = transform_data(df)
    transformed_df.show()

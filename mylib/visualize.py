import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from load import load


def visualize(dataframe):
    # Convert PySpark DataFrame to Pandas DataFrame
    pandas_df = dataframe.toPandas()

    # Visualization examples using Matplotlib
    plt.figure(figsize=(10, 6))

    # Example 1: Popularity Distribution
    plt.subplot(2, 2, 1)
    pandas_df["popularity"].plot(kind="hist", bins=20, color="blue", edgecolor="black")
    plt.title("Popularity Distribution")
    plt.xlabel("Popularity")
    plt.ylabel("Frequency")

    # Example 2: Danceability vs. Energy Scatter Plot
    plt.subplot(2, 2, 2)
    plt.scatter(
        x=pandas_df["danceability"],
        y=pandas_df["energy"],
        c=pandas_df["explicit"],
        cmap="viridis",
    )
    plt.title("Danceability vs. Energy")
    plt.xlabel("Danceability")
    plt.ylabel("Energy")

    plt.tight_layout()
    plt.show()

    return "finished visualization"


if __name__ == "__main__":
    df = load()
    result = visualize(df)
    print(result)

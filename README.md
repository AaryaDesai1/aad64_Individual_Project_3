# aad64_Individual_Project_3
Databricks ETL (Extract Transform Load) Pipeline
[![CI](https://github.com/AaryaDesai1/aad64_Individual_Project_3/actions/workflows/actions.yml/badge.svg)](https://github.com/AaryaDesai1/aad64_Individual_Project_3/actions/workflows/actions.yml)

Click here for [demo video](https://youtu.be/N9CFWcLH0Hk).

# Summary and Objectives:
This individual project was aimed at understanding how to use Databricks, create an ETL pipeline, and automating the trigger to initiate said pipeline. Furthermore, we needed to create a delta lake to store our data and then use PySpark to conduct data transformations. The following sections will elaborate on the components of this project. 

# Databricks Notebook:
This was made to test the entire ETL pipeline first and ensure that each step was working as need be. This included extraction, loading the data and ingestion, querying the delta lake made using PySpark, transforming said data, and then finally, visualizing the transformed data. 

# PyScripts:
After completing the entire ETL pipeline, querying, and visualization in the overarching Databricks notebook, I abstracted each function into their own Python scripts as follows. 
## 1. [extract.py](https://github.com/AaryaDesai1/aad64_Individual_Project_3/blob/main/mylib/extract.py)
This file was created to extract the data I was interested in using. This involved writing a lot of function to first create a dfbs for the project, and adding the data to the dbfs. The following screenshot shows the successful run of this Python file in Databricks. 
<p align = "center"><img width="1129" alt="image" src="https://github.com/AaryaDesai1/aad64_Individual_Project_3/assets/143753050/e58adc22-b012-482d-b35e-f0a840df9f42"></p>

## 2. [load.py](https://github.com/AaryaDesai1/aad64_Individual_Project_3/blob/main/mylib/load.py)
This Python script was then created to load the data and store it in a `Delta Lake`. The dataset used for this project was a Spotify dataset. 
<p align = "center"><img width="1141" alt="image" src="https://github.com/AaryaDesai1/aad64_Individual_Project_3/assets/143753050/18dbcbee-aade-4bd8-8148-4e9036559568"></p>

## 3. [query.py](https://github.com/AaryaDesai1/aad64_Individual_Project_3/blob/main/mylib/query.py)
This file was dedicated to conducted some simple queries on the now extracted, loaded, and stored delta lake table. Since this dataset was a Spotify dataset, the queries were in relation to the same, looking up rows based on genre, danceability, etc. There were a total of 5 PySpark SQL queries written in this file, and they all successfully ran as seen below:
<p align = "center"><img width="1168" alt="image" src="https://github.com/AaryaDesai1/aad64_Individual_Project_3/assets/143753050/4533d2b1-e4fc-45dc-9f75-48d0c90433e0"></p>

## 4. [transform.py](https://github.com/AaryaDesai1/aad64_Individual_Project_3/blob/main/mylib/transform.py)
The next step was to transform the table based on my needs. The following transformations were carried out in this script:
- [x] Handle Missing Values: It fills missing values in the DataFrame with zeros. This is done using the na.fill() method provided by PySpark's DataFrame API.
- [x] Feature Engineering: It creates a new column named duration_minutes by dividing the existing duration_ms column by 60000. This calculates the duration in minutes assuming the duration_ms column represents duration in milliseconds.
- [x] Encoding Categorical Variables: It encodes categorical variables in the DataFrame. Specifically, it takes the categorical column genre and encodes it using StringIndexer (to convert string categories to numerical indices) and OneHotEncoder (to convert these indices into binary vectors). This step is essential for machine learning algorithms that require numerical inputs.
- [x] Drop Unnecessary Columns: After feature engineering and encoding, it drops the columns duration_ms and genre as they might no longer be needed in the processed DataFrame.
<p align = "center"><img width="1508" alt="image" src="https://github.com/AaryaDesai1/aad64_Individual_Project_3/assets/143753050/09b439b3-620a-4bfa-a3ed-b8a5acd7a6ba"></p>

## 5. [visualize.py](https://github.com/AaryaDesai1/aad64_Individual_Project_3/blob/main/mylib/visualize.py)
Finally, the transformed dataframe was visualized with the help of `matplotlib`. 
<p align = "center"><img width="1083" alt="image" src="https://github.com/AaryaDesai1/aad64_Individual_Project_3/assets/143753050/2fa83bb1-b120-496a-8748-dda72a84778e"></p>

## Interpretation and Conclusion from ETL and Visualization
Though this was just a demo for ETL pipeline creation and visualization, we can infer certain results from the same. For example, the visualization shows that the frequency of song plays is higher when the song is more popular. Furthermore, the more energy a song has, the more "danceable" it seems to be. Both of these results seem to align with our inherent understanding of music! 

# GitHub Actions:
This project also has a GitHub actions workflow to ensure the CI/CD pipeline structure is maintained. However, this project does not have a test file. This is because all the testing was conducted directly in the Databricks notebook. Therefore, I have only included installation of dependencies, linting, and formatting in my GitHub actions. All of these are passing as shown below. 

## Linting
<p align = "center"><img width="507" alt="image" src="https://github.com/AaryaDesai1/aad64_Individual_Project_3/assets/143753050/52fa5663-9530-4032-89b0-d574a41bb08e"></p>

## Formatting
<p align = "center"><img width="531" alt="image" src="https://github.com/AaryaDesai1/aad64_Individual_Project_3/assets/143753050/ef3b89d4-49eb-4fea-b4ea-8ea7751d9530"></p>


# Pipeline and Automatic Trigger
Finally, this project also has a pipeline set up to ensure that the Python files run in the same order that is specfied above. This is done with the help of a Cluster and it has been scheduled to run everyday at 2:45 PM Eastern 🕝. 
<p align = "center"><img width="1502" alt="Screenshot 2023-11-15 at 12 02 29 PM" src="https://github.com/AaryaDesai1/aad64_Individual_Project_3/assets/143753050/56c8b7de-ba16-49eb-b3d0-8a11249bcd43"></p>

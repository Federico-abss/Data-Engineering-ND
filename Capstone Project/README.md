# Project: ETL transformations in Google Cloud

Flows Airline needs to analyze the historical data of their flights to identify possible causes of delays and improve the service offered to their costumers.  
As the new data Engineer of the company I was tasked with building an etl pipeline that extracts the historical data of the flights, transforms it in a way that allows business analysts to obtain the desired insights and load it in a data warehouse that makes querying for informations and constructing analytics dashboards easy. 

## Step 1: Scope the Project and Gather Data

The original data is taken from the [Bureau of Transportation Statistics](https://www.transtats.bts.gov/airports.asp), specifically the dataset I am using consists of two months of airports data, 
covering around one million flights that took place between Jan 1 2015 and Feb 28 2015.

![Flights dataset](/Capstone%20Project/images/flights_data.png)

The aim of the project is cleaning the data and transporting it to BigQuery to make it available for analytical purposes to the rest of  the organization.

## Step 2: Explore and Assess the Data

The original dataset is in csv format and presents this schema:

```
FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,  
DEST_AIRPORT_ID, DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME, DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,
WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,DISTANCE
```

To make it suitable for analysis it needs a specific time format that presents date and time in the same column and a consistent timezone, right now date and time are separated into the FL_DATE and DEP_TIME, and there is no time zone offset associated with the time, meaning a departure time of 1406 in different rows can be different times depending on the time zone of the origin airport.

The time zone offsets, one for the origin airport and another for the destination, are not present in the data, which means I need a different dataset that contains the timezone offset of each airport to clean my data.  
Not having any available I will use the airport informations, also provided by BTS, as well as the timezonefinder library, that uses longitude and latitude to determine the timezone, to create the necessary informations.

Another step necessary to allow more complex comparative queries is using the informations presents about departures, wheels off and arrival (DEP_TIME, WHEELS_OFF, ARR_TIME) to create three final rows for each original row except canceled flights.

## Step 3: Define the Data Model

The final schema is very similar to the original one, with exception of having time and date under the same column, and three extra 
columns representing event data like arrival or departure.

This is what the schema looks like from the BigQuery UI:

![BQ schema](/Capstone%20Project/images/BQ_schema_full.jpg)

The file `bqschema.txt` was used during the creation of the table to enforce this schema and can be reused for creating new ones.

* A short overview of the processes involved in creating the final dataset:
	- Add timezone offset to the schema of airport informations
	- Convert flights dataset to a common timezone using the airports data
	- Unite date and time informations under the same column
	- Create events data

## Step 4: Run ETL to Model the Data

Visualization of the overall process on a component level:

![ETL overview](/Capstone%20Project/images/csv_file_to_bigquery.png)

Here is the pipeline as shown in the Dataflow UI:

![dataflow pipeline](/Capstone%20Project/images/dataflow_pipeline.png)

The file `etl.py` contains every function used in the pipeline and is responsible for triggering its execution.

The data needs to respect the schema to be loaded in the final destination, one of the test checks for data consistencies controls if changing timezones affected dates and caused the arrival time to be lower than departure, in that case it add 24 hours to the time column to fix the issue, and another ensures the row is not empty before writing it in BigQuery.

## Step 5: Complete Project Write Up

Now that the data is in the warehouse the analytics team has easy access to it, you can query in place and leverage the automatic optimizations and computational power of BigQuery, as well as use integrated tools like DataStudio or Qlik to create dashboards and presentations.

An example of a basic query run directly inside BigQuery: 

```
SELECT EVENT, NOTIFY_TIME, EVENT_DATAFROM `flights.events`
WHERE NOTIFY_TIME >= TIMESTAMP('2015-05-01 00:00:00 UTC') AND NOTIFY_TIME < TIMESTAMP('2015-05-03 00:00:00 UTC')
ORDER BY NOTIFY_TIME ASC LIMIT 10;
```
![Query Output](/Capstone%20Project/images/query.png)

#### About the tools I used

This project could have been realized with S3 ingestion point, Airflow to build the pipeline and Redshift for the warehouse, but I wanted to leverage the knowledge I have about Google Cloud and try my hand at making this project completely serverless, which was allowed by the technologies I chose.  

The only things I needed to do to set up the infrastructure (outside of writing the python code) were:
1. Creating the storage bucket I used for ingestion.
2. Creating the BigQuery dataset and the destination table
3. Activating the Dataflow api and then running `etl.py` in the cloud shell terminal inside a python venv containing the necessary dependencies (Apache Beam and and timezonefinder).

Dataflow takes care of creating worker nodes and requesting more resources in case of bigger workloads, and automatically shuts down when the job is finished, similarly Storage and BigQuery can scale without any intervention of the user.

#### Possible Scenarios
***If the data was increased by 100x*** - The pipeline has autoscaling enabled, this makes so that a reasonably bigger amount of data can be processed without many concerns outside computation and storage costs.  
***If the pipelines were run on a daily basis by 7am*** - If for example you need to process data on a daily base, you would need to create a new destination table or modify the dataflow job to append instead of truncating.  
***If the database needed to be accessed by 100+ people*** - BigQuery allows any user with the right authorizations to query the dataset, you can also cache the results of previous queries or create custom views from the most frequent queries to reduce BQ costs. 

#### Sources

I used the book [Data Science On Google Cloud](http://shop.oreilly.com/product/0636920057628.do) and the Google Cloud official tutorials during the execution of this project.

# Project: ETL transformations in Google Cloud

Flows Airline needs to analyze the historical data of their flights to identify possible causes of delays and improve the service offered to their costumers. As the new data Engineer of the company I was tasked with building an etl pipeline that extracts the historical data of the flights, transforms it in a way that allows business analysts to obtain the desired insights and load it in a data warehouse that makes querying for informations and constructing analytics dashboards easy. 

### Step 1: Scope the Project and Gather Data

The original data is taken from the [Bureau of Transportation Statistics](https://www.transtats.bts.gov/airports.asp), specifically the dataset I am using consists of two months of airports data, 
covering around one million flights that took place between Jan 1 2015 and Feb 28 2015.

![Flights dataset](/Capstone%20Project/images/flights_data.png)

The aim of the project is cleaning the data and transporting it to BigQuery to make it available for analytical purposes to the rest of  the organization.

### Step 2: Explore and Assess the Data

The original dataset is in csv format and presents this schema:

```
FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,  
DEST_AIRPORT_ID, DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME, DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,
WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,DISTANCE
```

To make it suitable for analysis it needs a specific time format that presents date and time in the same column and a consistent timezone, right now date and time are separated into the FL_DATE and DEP_TIME, and there is no time zone offset associated with the departure time, meaning a departure time of 1406 in different rows can be different times depending on the time zone of the origin airport.

The time zone offsets, one for the origin airport and another for the destination, are not present in the data, this means I need a different dataset that contains the timezone offset of each airport and use it to clean my data.  
Not having any available I will use the airport informations, also provided by BTS, as well as the timezonefinder library, that uses longitude and latitude to determine the timezone.

Another step necessary to allow more complex comparative queries is using the informations presents about departures, wheels off and arrival to create three final rows for each original row except canceled flights.

### Step 3: Define the Data Model

Departed All fields available in scheduled message, plus:DEP_TIME,DEP_DELAYCANCELLED,CANCELLATION_CODEDEP_AIRPORT_LAT,DEP_AIRPORT_LON,DEP_AIRPORT_TZOFFSET

wheels off All fields available in departed message, plus:TAXI_OUT,WHEELS_OFF

Arrived All fields available in wheelson message, plus:ARR_TIME,ARR_DELAY

pipeline steps

![dataflow pipeline](/Capstone%20Project/images/dataflow_pipeline.png)

1. Data ingestion - Upload the historical data and the airport informations to Cloud Storage
2. Data transformation 
	- Convert dataset to a common timezone
	- Add timezone offset to the schema
	- Create flight events
3. Use BigQuery as a data warehouse to store the events in a database and perform query/create analytics on.
`bq mk --project_id $PROJECT_ID flights`

BigQuery schema 

![BQ schema](/Capstone%20Project/images/BQ_schema_full.jpg)

### Step 4: Run ETL to Model the Data

![ETL overview](/Capstone%20Project/images/csv_file_to_bigquery.png)

Unit test to check if data is consistent, changing timezones might affect dates and generate inconsistencies, in that case we are gonna add 24 hours The 24-hour hack is called just before the yield in
tz_correct
. Now that we have new data aboutthe airports, it is probably wise to add it to our dataset. Also, as remarked earlier, we want to keeptrack of the time zone offset from UTC because some types of analysis might require knowledge of thelocal time. Thus, the new
tz_correct
 code becomes the following
 
After we have our time-corrected data, we can move on to creating events. We’ll limit ourselves for now to just the
departed
 and
arrived
 messages—we can rerun the pipeline to create the additionalevents if and when our modeling efforts begin to use other events

### Step 5: Complete Project Write Up

```
SELECT EVENT, NOTIFY_TIME, EVENT_DATAFROM `flights.events`
WHERE NOTIFY_TIME >= TIMESTAMP('2015-05-01 00:00:00 UTC') AND NOTIFY_TIME < TIMESTAMP('2015-05-03 00:00:00 UTC')
ORDER BY NOTIFY_TIME ASC LIMIT 10;
```
![Query Output](/Capstone%20Project/images/query.png)

scenarios:
If the data was increased by 100x - the pipeline has autoscaling enabled, this makes so that any amount of data can be processed without many concerns outside computation and storage costs.
If the pipelines were run on a daily basis by 7am - It's as simple as writing a script or a cron job to trigger the pipeline daily, to ingest the info from the previous day.
If the database needed to be accessed by 100+ people - BigQuery allows any user with the right authorizations to query the dataset, you can also cache the results of previous queries or create view only tables to reduce the querying costs.

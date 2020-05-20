import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from sql_queries import songs_table_query, artists_table_query, log_filtered_query, users_query, time_query, songplays_query


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: This function loads song_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3
        
        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files with the songs metadata
            output_data : S3 bucket were dimensional tables in parquet format will be stored
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql(songs_table_query) 
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(path = output_data + "/songs/songs.parquet", mode = "overwrite")

    # extract columns to create artists table
    artists_table = spark.sql(artists_table_query)
    
    # write artists table to parquet files
    artists_table.write.parquet(path = output_data + "/artists/artists.parquet", mode = "overwrite")


def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3. Also output from previous function is used in by spark.read.json command
        
        Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files with the events data
            output_data : S3 bucket were dimensional tables in parquet format will be stored
            
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*"

    # read log data file
    df = spark.read.json(log_data)    
    df.createOrReplaceTempView("staging_events")
    
    # filter by actions for song plays
    df = spark.sql(log_filtered_query)
    df.createOrReplaceTempView("staging_events")

    # extract columns for users table    
    users_table = spark.sql(users_query).dropDuplicates(['userId', 'level'])
    
    # write users table to parquet files
    users_table.write.parquet(path = output_data + "/users/users.parquet", mode = "overwrite")
    
    # extract columns to create time table
    time_table = spark.sql(time_query)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(path = output_data + "/time/time.parquet", mode = "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs/songs.parquet")
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(songplays_query) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(path = output_data + "/songplays/songplays.parquet", mode = "overwrite")


def main():
    """
        Extract songs and events data from S3, Transform it into dimensional tables format, and Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

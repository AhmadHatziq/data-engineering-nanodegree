import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, TimestampType
from pyspark.sql.types import StringType as Str, IntegerType as Int, DateType as Date, LongType as Long

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"] = config.get('AWS','KEY')
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get('AWS','SECRET')

def get_log_schema():
    """ Creates and returns the log schema.
    
    Returns:
        Schema of the log files in StructType object. 

    """
    log_schema = R([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Long()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Long()),
        Fld("song", Str()),
        Fld("status", Long()),
        Fld("ts", Long()),
        Fld("userAgent", Str()),
        Fld("userId", Str())
    ])
    return log_schema
    

def get_song_schema():
    """ Creates and returns the song schema.
    
    Returns:
        Schema of the song files in StructType object. 

    """
    song_schema = R([
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_name", Str()),
        Fld("duration", Dbl()),
        Fld("num_songs", Long()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("year", Long())
    ])
    return song_schema
    

def create_spark_session():
    """ Creates a spark session.

    Returns:
        SparkSession object

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Processes the song json files. 

    Args:
        spark (SparkSession): The SparkContext object.
        input_data (str): The source S3 bucket.
        output_data (str): The output directory to save the parquet files.

    Returns:
        None

    """
    
    # get song_schema
    song_schema = get_song_schema()
    
    # get filepath to song data file
    song_data_folder = os.path.join(input_data, "song-data/")
    
    # Select a folder to import in
    # song_data = "{}*/*/*/*.json".format(song_data_folder) # Use the whole folder. Note that it takes a long time
    song_files = "{}A/A/A/*.json".format(song_data_folder) # Select a small subset of the data
    # song_files = "{}A/A/*/*.json".format(song_data_folder) # Only select files in the first A folder
    
    # read song data file
    df = spark.read.json(song_files, schema = song_schema).dropDuplicates().cache()

    # extract columns to create songs table
    songs_table = df.select(col("song_id"), col("title"), col("artist_id"), col("year"), col("duration")).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    output = os.path.join(output_data, 'song.parquet')
    songs_table.write.partitionBy("year", "artist_id").parquet(output, 'overwrite')
    
    print("SUCCESS: song.parquet written")

    # extract columns to create artists table
    artists_table = df.select(col("artist_id"), col("artist_name"), col("artist_location"), \
                               col("artist_longitude"), col("artist_latitude")).distinct()
    
    # write artists table to parquet files
    output = os.path.join(output_data, 'artist.parquet')
    artists_table.write.parquet(output, 'overwrite')
    
    print("SUCCESS: artists.parquet written")
    
    # Register sql table to be used in the songplays table


def process_log_data(spark, input_data, output_data):
    """ Processes the log json files. 

    Args:
        spark (SparkSession): The SparkContext object.
        input_data (str): The source S3 bucket.
        output_data (str): The output directory to save the parquet files.

    Returns:
        None
        
    """
    
    # get log schema
    log_schema = get_log_schema()

    # get filepath to log data file
    log_data_folder = os.path.join(input_data, "log-data/")
    log_files = "{}*/*/*events.json".format(log_data_folder)

    # read log data file
    df = spark.read.json(log_files, schema = log_schema).dropDuplicates().cache()
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong").cache()

    # extract columns for users table    
    users_table = df.select(col("firstName"), col("lastName"), col("gender"), col("level"), col("userId")).distinct()
    
    # write users table to parquet files
    output = os.path.join(output_data, 'users.parquet')
    users_table.write.parquet(output, 'overwrite')
    
    print("SUCCESS: users.parquet written")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # create other timestamp columns
    df = df.withColumn("hour", hour("start_time"))
    df = df.withColumn("day", dayofmonth("start_time"))
    df = df.withColumn("month", month("start_time"))
    df = df.withColumn("year", year("start_time"))
    df = df.withColumn("week", weekofyear("start_time"))
    df = df.withColumn("weekday", dayofweek("start_time"))

    # extract columns to create time table
    time_table = df.select(col("start_time"), col("hour"), col("day"), col("week"), \
                           col("month"), col("year"), col("weekday")).distinct()
    
    # write time table to parquet files partitioned by year and month
    output = os.path.join(output_data, 'time.parquet')
    time_table.write.partitionBy("year", "month").parquet(output, 'overwrite')
    
    print("SUCCESS: time.parquet written")

    # read in song and artist data to use for songplays table
    song_parquet_filepath = os.path.join(output_data, 'song.parquet')
    song_df = spark.read.parquet(song_parquet_filepath)
    song_df = song_df.drop("year")
    artist_parquet_filepath = os.path.join(output_data, 'artist.parquet')
    artist_df = spark.read.parquet(artist_parquet_filepath)
    
    # join the above tables together
    joined_table = song_df.join(artist_df, ['artist_id'], "inner").distinct()
    
    # join with table with time data
    temp_table = df.join(joined_table, joined_table.artist_name == df.artist, "inner").distinct()
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = temp_table.select(["start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent", "month", "year"])
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id()  )

    # write songplays table to parquet files partitioned by year and month
    output = os.path.join(output_data, 'songplays.parquet')
    songplays_table.write.partitionBy("year", "month").parquet(output, 'overwrite')
    
    print("SUCCESS: songplays.parquet written")



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "/home/workspace/parquet_output" # Saves parquet files to a local directory
    
    # Process song json files
    process_song_data(spark, input_data, output_data)    
    
    # Process log json files
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

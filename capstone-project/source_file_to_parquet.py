import configparser
import os
import pandas as pd
import psycopg2
import sql_statements 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat, col, lit, length
from pyspark.sql.types import IntegerType, DoubleType, LongType

def blank_as_null(x):
    """
        Converts '\\N' strings to None ie null. 
        Used for cleaning the IMDB .tsv datasets.
        
    Args:
        param1 (pyspark column name): The first parameter.
        
    """
    null_string = r"\N"
    return when(col(x) != null_string, col(x)).otherwise(None)

def clean_data(spark):
    """
        Loads the raw data from S3 bucket, cleans them and stores the .parquet file back into S3.
        
    Args:
        param1 (spark session object): The first parameter.
        
    """
    
    clean_imdb_data(spark)
    clean_kaggle_data(spark)
    return 

def clean_imdb_data(spark):
    """
        Processes the 7 IMDB .tsv files
        
    Args:
        param1 (spark session object): The first parameter.
        
    """
    
    # Process title-principals.tsv
    filepath = "s3a://udacity-dend-capstone-1995/datasets/imdb/title-principals.tsv"
    principal_df = spark.read.csv(filepath, sep=r'\t', header=True)
    principal_df = principal_df.withColumn("characters", blank_as_null("characters"))
    principal_df = principal_df.withColumn("job", blank_as_null("job"))
    principal_df = principal_df.withColumn("ordering", principal_df["ordering"].cast(IntegerType()))
    principal_df = principal_df.selectExpr("tconst as TITLE_ID", "nconst as NAME_ID", "ordering as ORDERING", "category as CATEGORY", 
                                          "job as JOB", "characters as CHARACTERS")
    output_path = "s3a://udacity-dend-capstone-1995/parquet/imdb-principal/imdb_principal.parquet"
    principal_df.write.parquet(output_path, 'overwrite')
    del principal_df
    print("title-principals.tsv SUCCESSFULLY processed to imdb_principal.parquet")
    
    # Process title-akas.tsv
    filepath = 's3a://udacity-dend-capstone-1995/datasets/imdb/title-akas.tsv'
    akas_df = spark.read.csv(filepath, sep=r'\t', header=True)
    akas_df = akas_df.withColumn("language", blank_as_null("language"))
    akas_df = akas_df.withColumn("types", blank_as_null("types"))
    akas_df = akas_df.withColumn("attributes", blank_as_null("attributes"))
    akas_df = akas_df.withColumn("title", blank_as_null("title"))
    akas_df = akas_df.withColumn("ordering", akas_df["ordering"].cast(IntegerType()))
    akas_df = akas_df.withColumn("isOriginalTitle", akas_df["isOriginalTitle"].cast(IntegerType()))
    akas_df = akas_df.selectExpr("titleId as TITLE_ID", "ordering as ORDERING", "title as TITLE", 
                             "region as REGION", "language as LANGUAGE", "types as TYPES", 
                             "attributes as ATTRIBUTES", "isOriginalTitle as IS_ORIGINAL_TITLE" )
    output_path = "s3a://udacity-dend-capstone-1995/parquet/imdb-akas/imdb_akas.parquet"
    akas_df.write.parquet(output_path, 'overwrite')
    del akas_df
    print("title-akas.tsv SUCCESSFULLY processed to imdb_akas.parquet")
    
    # Process title-basics.tsv
    filepath = 's3a://udacity-dend-capstone-1995/datasets/imdb/title-basics.tsv'
    basics_df = spark.read.csv(filepath, sep=r'\t', header=True)
    basics_df = basics_df.withColumn("titleType", blank_as_null("titleType"))
    basics_df = basics_df.withColumn("primaryTitle", blank_as_null("primaryTitle"))
    basics_df = basics_df.withColumn("originalTitle", blank_as_null("originalTitle"))
    basics_df = basics_df.withColumn("isAdult", blank_as_null("isAdult"))
    basics_df = basics_df.withColumn("startYear", blank_as_null("startYear"))
    basics_df = basics_df.withColumn("endYear", blank_as_null("endYear"))
    basics_df = basics_df.withColumn("runtimeMinutes", blank_as_null("runtimeMinutes"))
    basics_df = basics_df.withColumn("genres", blank_as_null("genres"))
    basics_df = basics_df.withColumn("isAdult", basics_df["isAdult"].cast(IntegerType()))
    basics_df = basics_df.withColumn("startYear", basics_df["startYear"].cast(IntegerType()))
    basics_df = basics_df.withColumn("endYear", basics_df["endYear"].cast(IntegerType()))
    basics_df = basics_df.withColumn("runtimeMinutes", basics_df["runtimeMinutes"].cast(IntegerType()))
    basics_df = basics_df.selectExpr("tconst as TITLE_ID", "titleType as TITLE_TYPE", "primaryTitle as PRIMARY_TITLE", 
                                 "originalTitle as ORIGINAL_TITLE", "isAdult as IS_ADULT", "startYear as START_YEAR", 
                                 "endYear as END_YEAR", "runtimeMinutes as RUNTIME_MINUTES", "genres as GENRES" )
    output_path = "s3a://udacity-dend-capstone-1995/parquet/imdb-basics/imdb_basics.parquet"
    basics_df.write.parquet(output_path, 'overwrite')
    del basics_df
    print("title-basics.tsv SUCCESSFULLY processed to imdb_basics.parquet")
    
    # Process title-crew.tsv
    filepath = 's3a://udacity-dend-capstone-1995/datasets/imdb/title-crew.tsv'
    crew_df = spark.read.csv(filepath, sep=r'\t', header=True)
    crew_df = crew_df.withColumn("writers", blank_as_null("writers"))
    crew_df = crew_df.withColumn("directors", blank_as_null("directors"))
    crew_df = crew_df.selectExpr("tconst as TITLE_ID", "directors as DIRECTORS", "writers as WRITERS")
    output_path = "s3a://udacity-dend-capstone-1995/parquet/imdb-crew/imdb_crew.parquet"
    crew_df.write.parquet(output_path, 'overwrite')
    del crew_df
    print("title-crew.tsv SUCCESSFULLY processed to imdb_crew.parquet")
    
    # Process title-episode.tsv
    filepath = 's3a://udacity-dend-capstone-1995/datasets/imdb/title-episode.tsv'
    episode_df = spark.read.csv(filepath, sep=r'\t', header=True)
    episode_df = episode_df.withColumn("parentTconst", blank_as_null("parentTconst"))
    episode_df = episode_df.withColumn("seasonNumber", blank_as_null("seasonNumber"))
    episode_df = episode_df.withColumn("episodeNumber", blank_as_null("episodeNumber"))
    episode_df = episode_df.withColumn("seasonNumber", episode_df["seasonNumber"].cast(IntegerType()))
    episode_df = episode_df.withColumn("episodeNumber", episode_df["episodeNumber"].cast(IntegerType()))
    episode_df = episode_df.selectExpr("tconst as TITLE_ID", "parentTconst as PARENT_TITLE_ID", "seasonNumber as SEASON_NUMBER", 
                                       "episodeNumber as EPISODE_NUMBER")
    output_path = "s3a://udacity-dend-capstone-1995/parquet/imdb-episode/imdb_episode.parquet"
    episode_df.write.parquet(output_path, 'overwrite')
    del episode_df
    print("title-episode.tsv SUCCESSFULLY processed to imdb_episode.parquet")
    
    # Process title-ratings.tsv
    filepath = 's3a://udacity-dend-capstone-1995/datasets/imdb/title-ratings.tsv'
    rating_df = spark.read.csv(filepath, sep=r'\t', header=True)
    rating_df = rating_df.withColumn("averageRating", blank_as_null("averageRating"))
    rating_df = rating_df.withColumn("numVotes", blank_as_null("numVotes"))
    rating_df = rating_df.withColumn("numVotes", rating_df["numVotes"].cast(IntegerType()))
    rating_df = rating_df.withColumn("averageRating", rating_df["averageRating"].cast(DoubleType()))
    rating_df = rating_df.selectExpr("tconst as TITLE_ID", "averageRating as AVERAGE_RATING", "numVotes as NUM_VOTES")
    output_path = "s3a://udacity-dend-capstone-1995/parquet/imdb-rating/imdb_rating.parquet"
    rating_df.write.parquet(output_path, 'overwrite')
    del rating_df
    print("title-ratings.tsv SUCCESSFULLY processed to imdb_rating.parquet")
    
    # Process name-basics.tsv
    filepath = 's3a://udacity-dend-capstone-1995/datasets/imdb/name-basics.tsv'
    name_basic_df = spark.read.csv(filepath, sep=r'\t', header=True)
    name_basic_df = name_basic_df.withColumn("primaryName", blank_as_null("primaryName"))
    name_basic_df = name_basic_df.withColumn("birthYear", blank_as_null("birthYear"))
    name_basic_df = name_basic_df.withColumn("deathYear", blank_as_null("deathYear"))
    name_basic_df = name_basic_df.withColumn("primaryProfession", blank_as_null("primaryProfession"))
    name_basic_df = name_basic_df.withColumn("knownForTitles", blank_as_null("knownForTitles"))
    name_basic_df = name_basic_df.withColumn("birthYear", name_basic_df["birthYear"].cast(IntegerType()))
    name_basic_df = name_basic_df.withColumn("deathYear", name_basic_df["deathYear"].cast(IntegerType()))
    name_basic_df = name_basic_df.selectExpr("nconst as NAME_ID", "primaryName as PRIMARY_NAME", "birthYear as BIRTH_YEAR", 
                                             "deathYear as DEATH_YEAR", "primaryProfession as PRIMARY_PROFESSION", 
                                             "knownForTitles as KNOWN_FOR_TITLES")
    output_path = "s3a://udacity-dend-capstone-1995/parquet/imdb-name-basic/imdb_name_basic.parquet"
    name_basic_df.write.parquet(output_path, 'overwrite')
    del name_basic_df
    print("name-basics.tsv SUCCESSFULLY processed to imdb_name_basic.parquet")
    
    return
    
    
def clean_kaggle_data(spark):
    """
        Processes the 5 Kaggle .csv files
        
    Args:
        param1 (spark session object): The first parameter.
        
    """
    
    # Process movies_metadata.csv
    filepath = 's3a://udacity-dend-capstone-1995/datasets/kaggle-movie-dataset/movies_metadata.csv'
    movie_df = spark.read.csv(filepath, header=True)
    movie_df = movie_df.withColumn("budget", movie_df["budget"].cast(IntegerType()))
    movie_df = movie_df.withColumn("revenue", movie_df["revenue"].cast(IntegerType()))
    movie_df = movie_df.withColumn("id", movie_df["id"].cast(IntegerType()))
    movie_df = movie_df.where(col("id").isNotNull())
    movie_df = movie_df.withColumn("runtime", movie_df["runtime"].cast(DoubleType()))
    movie_df = movie_df.withColumn("vote_average", movie_df["vote_average"].cast(DoubleType()))
    movie_df = movie_df.withColumn("vote_count", movie_df["vote_count"].cast(IntegerType()))
    movie_df = movie_df.selectExpr("adult as ADULT", "belongs_to_collection as BELONGS_TO_COLLECTION", "budget as BUDGET", 
                                   "genres as GENRES",
                                   "homepage as HOMEPAGE", "id as TMDB_ID", "imdb_id as IMDB_ID", 
                                   "original_language as ORIGINAL_LANGUAGE",
                                   "original_title as ORIGINAL_TITLE", "overview as OVERVIEW", 
                                   "popularity as POPULARITY", "poster_path as POSTER_PATH",
                                   "production_companies as PRODUCTION_COMPANIES", 
                                   "production_countries as PRODUCTION_COUNTRIES", "release_date as RELEASE_DATE", "revenue as REVENUE",
                                   "runtime as RUNTIME", "spoken_languages as SPOKEN_LANGUAGES", "status as STATUS", "tagline as TAGLINE",
                                   "title as TITLE", "video as IS_VIDEO", "vote_average as VOTE_AVERAGE", "vote_count as VOTE_COUNT"
                                  )
    output_path = "s3a://udacity-dend-capstone-1995/parquet/kaggle-movie-metadata/kaggle_movie_metadata.parquet"
    movie_df.write.parquet(output_path, 'overwrite')
    del movie_df
    print("movies_metadata.csv SUCCESSFULLY processed to kaggle_movie_metadata.parquet")
    
    # Process keywords.csv
    filepath = "s3a://udacity-dend-capstone-1995/datasets/kaggle-movie-dataset/keywords.csv"
    keyword_df = spark.read.csv(filepath, header=True)
    keyword_df = keyword_df.withColumn("id", keyword_df["id"].cast(IntegerType()))
    keyword_df = keyword_df.where(col("id").isNotNull())
    keyword_df = keyword_df.selectExpr("id as TMDB_ID", "keywords as KEYWORDS")
    output_path = "s3a://udacity-dend-capstone-1995/parquet/kaggle-movie-keywords/kaggle_movie_keywords.parquet"
    keyword_df.write.parquet(output_path, 'overwrite')
    del keyword_df
    print("keywords.csv.csv SUCCESSFULLY processed to kaggle_movie_keywords.parquet")
    
    # Process credits.csv
    filepath = "s3a://udacity-dend-capstone-1995/datasets/kaggle-movie-dataset/credits.csv"
    credit_df = spark.read.csv(filepath, header=True)
    credit_df = credit_df.withColumn("id", credit_df["id"].cast(IntegerType()))
    credit_df = credit_df.where(col("id").isNotNull())
    credit_df = credit_df.selectExpr("id as TMDB_ID", "crew as MOVIE_CREW", "cast as MOVIE_CAST")
    output_path = "s3a://udacity-dend-capstone-1995/parquet/kaggle-movie-credits/kaggle_movie_credits.parquet"
    credit_df.write.parquet(output_path, 'overwrite')
    del credit_df
    print("credits.csv.csv SUCCESSFULLY processed to kaggle_movie_credits.parquet")
    
    # Process links.csv
    filepath = "s3a://udacity-dend-capstone-1995/datasets/kaggle-movie-dataset/links.csv"
    links_df = spark.read.csv(filepath, header=True)
    links_df = links_df.withColumn("movieId", links_df["movieId"].cast(IntegerType()))
    links_df = links_df.withColumn("tmdbId", links_df["tmdbId"].cast(IntegerType()))
    links_df = links_df.withColumn("imdb_id", concat(lit("tt"), col("imdbId") ) )
    links_df = links_df.drop('imdbId')
    links_df = links_df.selectExpr("movieId as LINKS_ID", "tmdbId as TMDB_ID", "imdb_id as IMDB_ID")
    output_path = "s3a://udacity-dend-capstone-1995/parquet/kaggle-movie-links/kaggle_movie_links.parquet"
    links_df.write.parquet(output_path, 'overwrite')
    del links_df
    print("links.csv SUCCESSFULLY processed to kaggle_movie_links.parquet")
    
    # Process ratings.csv
    filepath = "s3a://udacity-dend-capstone-1995/datasets/kaggle-movie-dataset/ratings.csv"
    rating_df = spark.read.csv(filepath, header=True)
    rating_df = rating_df.withColumn("userId", rating_df["userId"].cast(IntegerType()))
    rating_df = rating_df.withColumn("movieId", rating_df["movieId"].cast(IntegerType()))
    rating_df = rating_df.withColumn("rating", rating_df["rating"].cast(DoubleType()))
    rating_df = rating_df.withColumn("timestamp", rating_df["timestamp"].cast(LongType()))
    rating_df = rating_df.selectExpr("userId as USER_ID", "movieId as TMDB_ID", "rating as RATING", "timestamp as TIMESTAMP")
    output_path = "s3a://udacity-dend-capstone-1995/parquet/kaggle-movie-ratings/kaggle_movie_ratings.parquet"
    rating_df.write.parquet(output_path, 'overwrite')
    del rating_df
    print("ratings.csv SUCCESSFULLY processed to kaggle_movie_ratings.parquet")
    
    return

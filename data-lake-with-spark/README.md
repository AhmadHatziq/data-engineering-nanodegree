# DATA ENGINEERING NANODEGREE
# PROJECT: DATA LAKE
### SUBMITTED BY: AHMAD HATZIQ BIN MOHAMAD

## 1. Summary
As part of Sparkify's business needs, a ETL/ELT pipeline was constructed. The pipeline is as follows: 

1. The ETL pipeline will extract data from the source json files located at the S3 bucket `s3a://udacity-dend/`.
2. The json data will be loaded into Spark, running on the Udacity cloud workspace (Amazon EMR). The data will be cleaned and loaded into Spark dataframes.
3. The spark dataframes are then saved to `.parquet` files in another output directory. Ideally, this should be another S3 bucket. However, for this project, the local output directory was used. 

Note: I was unable to save the `.parquet` files to my S3 bucket due to some issues. Kindly see the notebook `Testing02.ipynb` for error details. I saved to a local directory as some other users were facing the same [issue](https://knowledge.udacity.com/questions/452735). 

## 2. Description of files in this folder

The directory of the folder is as follows:
* /data - local data for testing purposes
* /parquet_output - output directory to store `.parquet` files
* dl.cfg - Configuration file to store AWS credentials
* etl.py - Python script to execute the ETL process
* Testing 01.ipynb - Jupyter notebook to test the ETL pipeline process
* Testing 02.iynb - - Jupyter notebook that shows the error message in saving `.parquet` files to S3 bucket
* README.md - this file

## 3. Guide

Ensure the AWS credentials are entered into `dl.cfg`. 

If desired, edit the `output_data` variable in the `main()` method of `etl.py`. Additionally, for testing purposes, different subsets of the song data can be accessed by the following lines of code: 
```
song_files = "{}*/*/*/*.json".format(song_data_folder) # Use the whole folder. Note that it takes a long time
song_files = "{}A/A/A/*.json".format(song_data_folder) # Select a small subset of the data
song_files = "{}A/A/*/*.json".format(song_data_folder) # Only select files in the first A folder
```
Kindly leave one of the lines uncommented to select the appropriate song data. 

To run the pipeline process, run `python etl.py` in the terminal console. 

Print statements are provided, to indicate when the star schema tables are finished writing to the output folder. 

When finished, a subset of the output is as follows:

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
21/01/22 05:24:12 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
SUCCESS: song.parquet written                                                                                                             
SUCCESS: artists.parquet written                                                                                                          
SUCCESS: users.parquet written                                                                                                            
SUCCESS: time.parquet written                                                                                                            
SUCCESS: songplays.parquet written    

```
The output `.parquet` files will be in the specified output directory. 

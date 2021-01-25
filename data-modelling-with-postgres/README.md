# DATA ENGINEERING NANODEGREE
# PROJECT: DATA MODELLING WITH POSTGRES
### SUBMITTED BY: AHMAD HATZIQ BIN MOHAMAD

## 1. Description of files in this folder
The directory of the folder is as follows:
* /images
    * star_schema.png
    * gender_pie_chart.png
* /data - Test data to run the ETL and analysis on
* analytics.ipynb - Jupyter notebook running some simple analysis
* test.ipynb - Jupyter notebook for testing of SQL statements
* etl.ipynb - Jupyter notebook for testing of ETL scripts
* create_tables.py - Python script to `DROP` and `CREATE TABLES`. References `sql_queries.py`
* etl.py - Python script to execute the ETL process
* sql_queries.py - Python script containing `CREATE TABLE` and `INSERT` statements
* README.md - this file


## 2. Purpose of database & analytical goals
The purpose of this database is to support Sparkify in their analytics. Sparkify can execute most queries with minimal joins. This database will support OLAP workloads rather than OLTP workloads. 

## 3.1. Database schema design
![star schema](../images/start_schema.PNG)





The data model has 5 tables: 4 dimension tables and 1 fact table.

Table name: `songplays`
Type: Fact Table
| COLUMN  | TYPE  | ADDITIONAL INFO|
| ------------- | ------------- | --|
| songplay_id  | INT  | PRIMARY KEY, AUTO INCREMENT|
| start_time  | INT  | --|
| user_id  | INT  | --|
| level  | VARCHAR  | --|
| song_id  | VARCHAR  | --|
| artist_id  | VARCHAR  | --|
| session_id  | INT  | --|
| location  | TEXT  | --|
| user_agent  | TEXT  | --|

Table name: `time`
Type: Dimension Table
| COLUMN  | TYPE  | ADDITIONAL INFO|
| ------------- | ------------- | --|
| start_time  | BIGINT  | PRIMARY KEY|
| hour  | INT  | --|
| day  | INT  | --|
| week  | INT  | --|
| month  | INT  | --|
| year  | INT  | --|
| weekday  | VARCHAR  | --|

Table name: `artists`
Type: Dimension Table
| COLUMN  | TYPE  | ADDITIONAL INFO|
| ------------- | ------------- | --|
| artist_id  | VARCHAR  | PRIMARY KEY|
| name  | VARCHAR  | --|
| location  | VARCHAR  | --|
| latitude  | DECIMAL  | --|
| longitude  | DECIMAL  | --|
⠀
Table name: `users`
Type: Dimension Table
| COLUMN  | TYPE  | ADDITIONAL INFO|
| ------------- | ------------- | --|
| user_id  | INT  | PRIMARY KEY|
| first_name  | VARCHAR  | --|
| last_name  | VARCHAR  | --|
| gender  | VARCHAR(2)  | --|
| level  | VARCHAR  | --|

Table name: `songs`
Type: Dimension Table
| COLUMN  | TYPE  | ADDITIONAL INFO|
| ------------- | ------------- | --|
| song_id  | VARCHAR  | PRIMARY KEY|
| title  | VARCHAR  | --|
| artist_id  | VARCHAR  | --|
| year  | INT  | --|
| duration  | DECIMAL  | -|



## 3.2. ETL pipeline 
The file `etl.py` handles the main logic of the ETL pipeline process. It can be executed by running `python etl.py` at the terminal.
The process can be broken down into 2 main stages.
1. Processing the song json file.
    * Data is extracted from the song json file and loaded into the `songs` table.
    * Data is extracted from the song json file and loaded into the `artists` table.
2. Processing the log json file.
    * Data is extracted from the log json file.
    * Data is pre-processed in the following manner.
        * Filtered by `NextSong`.
        * Parse the `ts` column to get the time attributes.
    * Data is loaded into the `time` table.
    * Data is loaded into the `users` table.
    * Data is pre-processed to obtain the row's respective song_id and artist_id.
    * Data is loaded into the 'songplays' table.

## 4. Song play analysis

The ETL processed a total of 72 rows for `song_data` and 6820 rows for `log_data`. 
After the ETL, these are the row counts for each table:

| TABLE  | ROW COUNT  |
| ------------- | -------------|
| songplays  | 6820  |
| time  | 6813  |
| artists  | 69  |
| users  | 96  |
| songs  | 71|
| TOTAL | 13,869|

As we can see, using a star schema has almost doubled the number of rows. However, the performance increase comes in the ability to make joins with much smaller tables.

### 4.1. Analysis of most popular browsers
To ascertain the most popular browser, the command `SELECT DISTINCT user_agent FROM songplays;` was executed. The dominant 3 browsers (irrespective of version) are Google Chrome, Firefox and Safari.

The counts are as follows:
| Browser  | ROW COUNT  |
| ------------- | -------------|
| Firefox  | 1589  |
| Chrome  | 3896  |
| Safari  | 5203  |

People who use Safari are generally Apple Macintosh or iOS users. Hence, perhaps Sparkify could run more campaigns to target users from Windows and Android to use its applications.

### 4.2. Analysis of user genders
To get the breakdown of user genders, the following command was executed: `SELECT users.gender, COUNT(*)  FROM songplays JOIN users ON songplays.user_id = users.user_id GROUP BY users.gender;`.

The counts are as follows:
| Gender  | COUNT  |
| ------------- | -------------|
| Female  | 1933 |
| Male  | 4887  |


A pie chart visualization is below. 
![gender pie chart](../images/gender_pie_chart.PNG)

A large proportion of the users are females. Perhaps more changes can be done to target more male users to use Sharkify's services.



 




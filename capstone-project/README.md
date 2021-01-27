# DATA ENGINEERING NANODEGREE
# PROJECT: CAPSTONE PROJECT
### SUBMITTED BY: AHMAD HATZIQ BIN MOHAMAD

## 1. Description of files in this folder

The directory of the folder is as follows:
* /images - image resources
* /workings 
	* data_quality.ipynb - Jupyter used in data quality checks
	* exploration_and_cleaning.ipynb- Jupyter used in data exploration and data cleaning
* `create_cluster.py`: IaC file. Used to create the Redshift database and store login credentials. 
* `delete_cluster.py`: IaC file. Used to delete the Redshift database. 
* `dwh.cfg`: Configuration file used for storing AWS and Redshift credentials.
* `source_file_to_parquet.py`: Loads the raw data files from S3 into Spark, performs data cleaning and writes parquet files back into S3.
* `parquet_to_redshift.py`: Loads the parquet files from S3 into the Redshift database.
* `sql_statements.py`: SQL statements used in the ETL process.
* `data_quality.py`: Performs data quality checks.
* `etl.py`: Executes the whole ETL process, using all the above files except for `create_cluster.py` and `delete_cluster.py`.
* `README.md` - this file
* `data_dictionary.md` - Data dictionary of final tables
* `capstone_project_report.ipynb` - Final report of this project

## 2. Project details

To view more details, please read this [notebook](capstone_project_report.ipynb).
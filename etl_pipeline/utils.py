from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def spark():
    spark = SparkSession \
            .builder\
            .config('spark.ui.showConsoleProgress', 'false')\
            .config('spark.sql.execution.arrow.pyspark.enabled', 'true')\
            .getOrCreate()
            
    return spark

spark = spark()

def get_csv(url):
    csv_data = spark.read.option('header','true').csv(url)
    
    return csv_data


def persist_dataframe(database: str, table: str, dataframe: DataFrame):
    
    dataframe.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver:ip-here;databaseName={database};encrypt=true;trustServerCertificate=true") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()
    msg = 'Dataframe was persisted on '+ database + ' in ' + table

    return print(msg)


def calculate_age(latest_date, earliest_date):
    """
    Calculate age based on the difference between two dates.
    
    Args:
        latest_date (Column): Column representing the latest date.
        earliest_date (Column): Column representing the earliest date.
        
    Returns:
        Column: Column representing the calculated age.
    """
    
    latest_date_int = 10000*(substring(latest_date, 1,4)) + 100*(substring(latest_date, 6,2)) + (substring(latest_date, 9,2))
    
    earliest_date_int = 10000*(substring(earliest_date, 1,4)) + 100*(substring(earliest_date, 6,2)) + (substring(earliest_date, 9,2))
    
    age = (latest_date_int - earliest_date_int) / 10000
    age = age.cast('int')

    return age
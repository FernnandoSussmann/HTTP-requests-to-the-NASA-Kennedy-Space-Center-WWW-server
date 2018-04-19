from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

def load_file(sc,file_path):
    return sc.textFile(file_path)

def organize_data_into_dataframe(data_rdd):
    from datetime import datetime
    
    invalid_data_list = [
                        "Invalid_data",
                        "-",
                        "-",
                        datetime.now().strftime("%d/%b/%Y:%H:%M:%S"),
                        "-",
                        "-",
                        "-",
                        "0"
                        ]

    final_df = data_rdd.map(lambda line: line.split(" ") if len(line.split(" ")) >= 8
                                                         else invalid_data_list) \
                       .map(lambda line: Row(host=line[0], 
                       timestamp=datetime.strptime(str(line[3]).replace('[',''), "%d/%b/%Y:%H:%M:%S"),
                       requestMethod=line[5].encode('utf-8').replace('"',''),
                       request=line[6].encode('utf-8').replace('"',''),
                       HTTPcode=line[-2], 
                       totalBytes=int(line[-1].encode('utf-8').replace('-', '0')))).toDF()

    return final_df

def data_frame_debug(df):
    df.printSchema() 
    df.show() 

def distinct_hosts(spark):
    query = """
    SELECT count(DISTINCT Host) as HostsQuantity
    FROM logs_table
    """
    return spark.sql(query)

def error_404_count(spark):
    query = """
    SELECT count(HTTPcode) as PagesNotFound
    FROM logs_table
    WHERE HTTPcode = '404'
    """

    return spark.sql(query)

def error_404_count_top_url(spark):
    query = """
    SELECT concat(Host, Request) as URL,
           count(HTTPcode) as PagesNotFound
    FROM logs_table
    WHERE HTTPcode = '404'
    GROUP BY URL
    ORDER BY PagesNotFound DESC LIMIT 5
    """

    return spark.sql(query)

def error_404_count_by_day(spark):
    query = """
    SELECT cast(timestamp as date) as Date,
           count(HTTPcode) as PagesNotFound
    FROM logs_table
    WHERE HTTPcode = '404'
    GROUP BY Date
    """

    return spark.sql(query)

def get_total_bytes(spark):
    query = """
    SELECT sum(totalBytes) as TotalBytes
    FROM logs_table
    """

    return spark.sql(query)

def main():
    from pandas import DataFrame

    # Loading Spark Context and Session
    sc = SparkContext()
    spark = SparkSession(sc)

    # Loading files
    log_aug95_RDD = load_file(sc,"access_log_Aug95")
    log_jul95_RDD = load_file(sc,"access_log_Jul95")

    # Converting RDD to dataframe
    log_aug95_DF = organize_data_into_dataframe(log_aug95_RDD)
    log_jul95_DF = organize_data_into_dataframe(log_jul95_RDD)

    data_frame_debug(log_aug95_DF)
    data_frame_debug(log_jul95_DF)

    # Unifying data in one dataframe
    all_logs_df = log_aug95_DF.union(log_jul95_DF)
    # Cleaning invalid lines and registering valid data as temp table
    all_logs_df.where("host != 'Invalid_data'").registerTempTable("logs_table")

    data_frame_debug(all_logs_df)

    # Getting number of disinct hosts and saving it as csv
    unique_hosts_df = distinct_hosts(spark)
    unique_hosts_df.toPandas().to_csv('unique_hosts.csv')

    data_frame_debug(unique_hosts_df)

    # Getting number of 404 errors and saving it as csv
    error_404_count_df = error_404_count(spark)
    error_404_count_df.toPandas().to_csv('error_404_count.csv')

    data_frame_debug(error_404_count_df)

    # Getting top 5 URLs that showed 404 errors and saving it as csv
    error_404_count_top_url_df = error_404_count_top_url(spark)
    error_404_count_top_url_df.toPandas().to_csv('error_404_count_top_urls.csv')

    data_frame_debug(error_404_count_top_url_df)

    # Getting number of 404 errors by day and saving it as csv
    error_404_count_by_day_df = error_404_count_by_day(spark)
    error_404_count_by_day_df.toPandas().to_csv('error_404_count_by_day.csv')

    data_frame_debug(error_404_count_by_day_df)

    # Getting total of bytes and saving it as csv
    total_bytes_df = get_total_bytes(spark)
    total_bytes_df.toPandas().to_csv('total_bytes.csv')

    data_frame_debug(total_bytes_df)    

main()
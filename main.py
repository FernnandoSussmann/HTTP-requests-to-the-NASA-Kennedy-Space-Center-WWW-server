from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

def load_file(sc,file_path):
    return sc.textFile(file_path)

def organize_data_into_dataframe(data_rdd):
    from datetime import datetime
    return data_rdd.map(lambda line: line.split(" ")) \
            .map(lambda c: Row(host=c[0], 
            timestamp=datetime.strptime(str(c[3]).replace('[',''), "%d/%b/%Y:%H:%M:%S"), 
            request=c[5]+c[6],
            HTTPcode=c[-2], 
            totalBytes=c[-1])).toDF()

def data_frame_debug(df):
    df.printSchema() 
    df.show() 

def distinct_hosts(spark):
    query = """
    SELECT DISTINCT HOST
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

def main():
    sc = SparkContext()
    spark = SparkSession(sc)

    log_aug95_RDD = load_file(sc,"access_log_Aug95")
    log_jul95_RDD = load_file(sc,"access_log_Jul95")

    log_aug95_DF = organize_data_into_dataframe(log_aug95_RDD)
    log_jul95_DF = organize_data_into_dataframe(log_jul95_RDD)

    data_frame_debug(log_aug95_DF)
    data_frame_debug(log_jul95_DF)

    all_logs_df = log_aug95_DF.union(log_jul95_DF)
    all_logs_df.registerTempTable("logs_table")
    # all_logs_df.collect()
    # all_logs_df.cache()

    data_frame_debug(all_logs_df)

    unique_hosts_df = distinct_hosts(spark)
    # unique_hosts_df.collect()

    data_frame_debug(unique_hosts_df)

    http_count_df = error_404_count(spark)
    # http_count_df.collect()

    data_frame_debug(http_count_df)

main()
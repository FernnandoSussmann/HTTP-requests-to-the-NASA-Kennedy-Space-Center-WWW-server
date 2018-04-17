from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

def load_file(sc,file_path):
    return sc.textFile(file_path)

def organize_data_into_dataframe(data_rdd):
    from datetime import datetime
    return data_rdd.map(lambda line: line.split(" ")) \
            .map(lambda c: Row(host=c[0], 
            timestamp=datetime.strptime(str(c[3].replace('[','')), '%d/%b/%Y:%H:%M:%S'), 
            request=str(c[5]+c[6]).replace('"',''),
            HTTPcode=int(c[8]), 
            totalBytes=int(str(c[9]).replace('-','0')))).toDF()

def data_frame_debug(df):
    df.printSchema() 
    df.show() 

def main():
    # from datetime import datetime
    sc = SparkContext()
    spark = SparkSession(sc)

    log_aug95_RDD = load_file(sc,"access_log_Aug95")
    log_jul95_RDD = load_file(sc,"access_log_Jul95")

    log_aug95_DF = organize_data_into_dataframe(log_aug95_RDD)
    log_jul95_DF = organize_data_into_dataframe(log_jul95_RDD)

    data_frame_debug(log_aug95_DF)
    data_frame_debug(log_jul95_DF)

main()
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

def load_file(sc,file_path):
    return sc.textFile(file_path)

def main():
    sc = SparkContext()
    spark = SparkSession(sc)

    log_aug95_RDD = load_file(sc,"access_log_Aug95")
    log_jul95_RDD = load_file(sc,"access_log_Jul95")

    log_aug95_DF = log_aug95_RDD.map(lambda line: line.split(" ")) \
        .map(lambda c: Row(host=c[0], timestamp=c[3]+c[4], request=c[5]+c[6],\
         HTTPcode=c[8], totalBytes=c[9])).toDF()

    log_jul95_DF = log_jul95_RDD.map(lambda line: line.split(" ")) \
        .map(lambda c: Row(host=c[0], timestamp=c[3]+c[4], request=c[5]+c[6],\
         HTTPcode=c[8], totalBytes=c[9])).toDF()

    log_aug95_DF.printSchema() 
    log_jul95_DF.printSchema() 

    log_aug95_DF.show() 
    log_jul95_DF.show() 


main()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob

import os
import sys , shutil
if os.path.isdir('check/'):
    shutil.rmtree("check/")
if os.path.isdir('parc/'):
    shutil.rmtree( "parc/")
# spark_path = os.environ['SPARK_HOME']
# print(os.environ['JAVA_HOME'])
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# sys.path.insert(0, spark_path + "/bin")
# sys.path.insert(0, spark_path + "/python/pyspark/")
# sys.path.insert(0, spark_path + "/python/lib/pyspark.zip")
# sys.path.insert(0, spark_path + "/python/lib/py4j-0.10.9-src.zip")
#print(os.environ['PYSPARK_PYTHON'],
#      os.environ['PYSPARK_DRIVER_PYTHON'])


def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words


def polarity_detection(text):
    return TextBlob(text).sentiment.polarity


def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity


def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words


if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
    # read the tweet data from socket
    lines = spark.readStream.format("socket").option("host", "0.0.0.0").option("port", 5555).load()
    # Preprocess the data
    #lines.printSchema()
    words = preprocessing(lines)
    # text classification to define polarity and subjectivity
    words = text_classification(words)
    words = words.repartition(1)
    query = words.writeStream.queryName("all_tweets") \
        .outputMode("append").format("parquet") \
        .option("path", "./parc") \
        .option("checkpointLocation", "./check") \
        .trigger(processingTime='60 seconds').start()
    query.awaitTermination()

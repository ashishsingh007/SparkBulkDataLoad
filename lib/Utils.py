from lib.ConfigLoader import get_spark_conf
from pyspark.sql import SparkSession

# .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1")\
def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
                           .config(conf=get_spark_conf(env)) \
                           .config('spark.sql.autoBroadcastJoinThreshold', -1) \
                           .config('spark.sql.adaptive.enabled', 'false') \
                           .config('spark.driver.extraJavaOptions','-Dlog4j.configuration=file:log4j.properties') \
                           .master("local[2]") \
                           .enableHiveSupport() \
                           .getOrCreate()
    else:
        return SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()



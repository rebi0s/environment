import json
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from decouple import config
from utils import init_spark
import requests

# aws_access_key = config('AWS_ACCESS_KEY_ID')
# aws_secret_key = config('AWS_SECRET_ACCESS_KEY')

spark = init_spark()

# hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3a.access.key", aws_access_key)
# hadoop_conf.set("fs.s3a.secret.key", aws_secret_key)
# hadoop_conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
# hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

sparkContext = spark.sparkContext

spark.sql("SELECT * from rebios.person").show(5)

spark.stop()


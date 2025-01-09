from pyspark.sql import SparkSession
from config.config import configuration  # Assuming configuration is a module where you retrieve AWS credentials

if __name__=="__main__":
# Set up the Spark session
    spark = SparkSession.builder \
        .appName('Data-eng-pipeline(unstructured)') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.access.key', configuration.get('AWS_ACCESS_KEY')) \
        .config('spark.hadoop.fs.s3a.secret.key', configuration.get('AWS_SECRET_KEY')) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

# Now you can use the spark session to access data from AWS S3

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import explode, col
from datetime import datetime
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_source_path = "s3://adzuna-etl-project/raw_data/to_process/"
s3_output_path = "s3://adzuna-etl-project/transformed_data/"

source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [s3_source_path]
    }
)

jobs_df = source_dyf.toDF()
jobs_df = jobs_df.withColumn("items", explode("items"))\
       .select(col("items.id").alias("job_id"),
               col("items.title").alias("job_title"),
               col("items.location.display_name").alias("job_location"),
               col("items.company.display_name").alias("job_company"),
               col("items.category.label").alias("job_category"),
               col("items.description").alias("job_description"),
               col("items.redirect_url").alias("job_url"),
               col("items.created").cast("timestamp").alias("job_created"))\
       .dropDuplicates(["job_id"])

jobs_df.write.mode("append").parquet(s3_output_path)
job.commit()
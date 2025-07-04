import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3


def delete_s3_prefix(bucket: str, prefix: str):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    delete_us = dict(Objects=[])
    for page in pages:
        for obj in page.get("Contents", []):
            delete_us["Objects"].append(dict(Key=obj["Key"]))
            if len(delete_us["Objects"]) >= 1000:
                s3.delete_objects(Bucket=bucket, Delete=delete_us)
                delete_us = dict(Objects=[])
    if delete_us["Objects"]:
        s3.delete_objects(Bucket=bucket, Delete=delete_us)


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


# Initialize
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configuration
bucket_name = "cd0030-sdl-amal"
accel_input_path = "s3://cd0030-sdl-amal/accelerometer_landing/"
customer_input_path = "s3://cd0030-sdl-amal/customer_trusted/"
output_path = "accelerometer_trusted/"

# Clean output path
delete_s3_prefix(bucket_name, output_path)

# Load source data directly from S3
accelerometer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [accel_input_path]},
    format="json",
    transformation_ctx="accelerometer_landing",
)

customer_trusted = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [customer_input_path]},
    format="parquet",
    transformation_ctx="customer_trusted",
)

# Filter accelerometer data for trusted customers only
query = """
    SELECT a.* 
    FROM accelerometer_landing a 
    JOIN customer_trusted c 
    ON a.user = c.email
"""
accelerometer_trusted = sparkSqlQuery(
    glueContext,
    query,
    {
        "accelerometer_landing": accelerometer_landing,
        "customer_trusted": customer_trusted,
    },
    "accelerometer_trusted",
)

# Write filtered data to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=accelerometer_trusted,
    connection_type="s3",
    connection_options={
        "path": f"s3://{bucket_name}/{output_path}",
        "partitionKeys": [],
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
    },
    format="parquet",
    format_options={"compression": "snappy"},
    catalog_database="default",
    catalog_table_name="accelerometer_trusted",
)

job.commit()

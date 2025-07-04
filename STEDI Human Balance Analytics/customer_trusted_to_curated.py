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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configuration
bucket_name = "cd0030-sdl-amal"
output_path = "customer_curated/"

# Clean output path for idempotency
delete_s3_prefix(bucket_name, output_path)

# Load source tables
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted",
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted",
)

# Curate customers who have accelerometer data
query = "SELECT DISTINCT c.* FROM customer_trusted c JOIN accelerometer_trusted a ON c.email = a.user"
customer_curated = sparkSqlQuery(
    glueContext,
    query,
    {
        "customer_trusted": customer_trusted,
        "accelerometer_trusted": accelerometer_trusted,
    },
    "customer_curated",
)

# Write to S3 and update catalog
glueContext.write_dynamic_frame.from_options(
    frame=customer_curated,
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
    catalog_table_name="customer_curated",
)

job.commit()

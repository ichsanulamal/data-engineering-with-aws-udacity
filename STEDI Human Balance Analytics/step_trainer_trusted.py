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
step_input_path = "s3://cd0030-sdl-amal/step_trainer_landing/"
customer_input_path = "s3://cd0030-sdl-amal/customer_curated/"
output_path = "step_trainer_trusted/"

# Clean output path for idempotency
delete_s3_prefix(bucket_name, output_path)

# ✅ Load source data from S3
step_trainer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [step_input_path]},
    format="json",
    transformation_ctx="step_trainer_landing",
)

customer_curated = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [customer_input_path]},
    format="parquet",
    transformation_ctx="customer_curated",
)

# Filter step trainer data for curated customers only
query = """
    SELECT s.* 
    FROM step_trainer_landing s 
    JOIN customer_curated c 
    ON s.serialNumber = c.serialNumber
"""
step_trainer_trusted = sparkSqlQuery(
    glueContext,
    query,
    {
        "step_trainer_landing": step_trainer_landing,
        "customer_curated": customer_curated,
    },
    "step_trainer_trusted",
)

# Write to S3 in Parquet format (optional: add to catalog)
glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_trusted,
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
    catalog_table_name="step_trainer_trusted",
)

job.commit()

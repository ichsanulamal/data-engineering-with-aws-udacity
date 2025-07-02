import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# s3_cleanup.py
import boto3

def delete_s3_prefix(bucket: str, prefix: str):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    delete_us = dict(Objects=[])
    for page in pages:
        for obj in page.get('Contents', []):
            delete_us['Objects'].append(dict(Key=obj['Key']))
            if len(delete_us['Objects']) >= 1000:
                s3.delete_objects(Bucket=bucket, Delete=delete_us)
                delete_us = dict(Objects=[])
    if delete_us['Objects']:
        s3.delete_objects(Bucket=bucket, Delete=delete_us)


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Clean output paths for idempotency
output_paths = {
    "customer_trusted": "customer_trusted/",
    "accelerometer_trusted": "accelerometer_trusted/",
    "customer_curated": "customer_curated/",
    "step_trainer_trusted": "step_trainer_trusted/",
    "machine_learning_curated": "machine_learning_curated/"
}
bucket_name = "cd0030-sdl-amal"
for prefix in output_paths.values():
    delete_s3_prefix(bucket_name, prefix)

# Load source tables
customer_landing = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="customer_landing", transformation_ctx="customer_landing")
accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing")
step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing")

# 1. customer_trusted
query = "SELECT * FROM customer_landing WHERE shareWithResearchAsOfDate IS NOT NULL"
customer_trusted = sparkSqlQuery(glueContext, query, {"customer_landing": customer_landing}, "customer_trusted")
glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted,
    connection_type="s3",
    connection_options={"path": f"s3://{bucket_name}/{output_paths['customer_trusted']}", "partitionKeys": []},
    format="parquet",
    format_options={"compression": "snappy"},
    catalog_database="default",
    catalog_table_name="customer_trusted"
)

# 2. accelerometer_trusted
query = "SELECT a.* FROM accelerometer_landing a JOIN customer_trusted c ON a.user = c.email"
accelerometer_trusted = sparkSqlQuery(glueContext, query, {
    "accelerometer_landing": accelerometer_landing,
    "customer_trusted": customer_trusted
}, "accelerometer_trusted")
glueContext.write_dynamic_frame.from_options(
    frame=accelerometer_trusted,
    connection_type="s3",
    connection_options={"path": f"s3://{bucket_name}/{output_paths['accelerometer_trusted']}", "partitionKeys": []},
    format="parquet",
    format_options={"compression": "snappy"},
    catalog_database="default",
    catalog_table_name="accelerometer_trusted"
)

# 3. customer_curated
query = "SELECT DISTINCT c.* FROM customer_trusted c JOIN accelerometer_trusted a ON c.email = a.user"
customer_curated = sparkSqlQuery(glueContext, query, {
    "customer_trusted": customer_trusted,
    "accelerometer_trusted": accelerometer_trusted
}, "customer_curated")
glueContext.write_dynamic_frame.from_options(
    frame=customer_curated,
    connection_type="s3",
    connection_options={"path": f"s3://{bucket_name}/{output_paths['customer_curated']}", "partitionKeys": []},
    format="parquet",
    format_options={"compression": "snappy"},
    catalog_database="default",
    catalog_table_name="customer_curated"
)

# 4. step_trainer_trusted
query = "SELECT s.* FROM step_trainer_landing s JOIN customer_curated c ON s.serialNumber = c.serialNumber"
step_trainer_trusted = sparkSqlQuery(glueContext, query, {
    "step_trainer_landing": step_trainer_landing,
    "customer_curated": customer_curated
}, "step_trainer_trusted")
glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_trusted,
    connection_type="s3",
    connection_options={"path": f"s3://{bucket_name}/{output_paths['step_trainer_trusted']}", "partitionKeys": []},
    format="parquet",
    format_options={"compression": "snappy"},
    catalog_database="default",
    catalog_table_name="step_trainer_trusted"
)

# 5. machine_learning_curated
query = "SELECT a.*, s.distanceFromObject FROM accelerometer_trusted a JOIN step_trainer_trusted s ON a.timestamp = s.sensorReadingTime"
machine_learning_curated = sparkSqlQuery(glueContext, query, {
    "accelerometer_trusted": accelerometer_trusted,
    "step_trainer_trusted": step_trainer_trusted
}, "machine_learning_curated")
glueContext.write_dynamic_frame.from_options(
    frame=machine_learning_curated,
    connection_type="s3",
    connection_options={"path": f"s3://{bucket_name}/{output_paths['machine_learning_curated']}", "partitionKeys": []},
    format="parquet",
    format_options={"compression": "snappy"},
    catalog_database="default",
    catalog_table_name="machine_learning_curated"
)

job.commit()

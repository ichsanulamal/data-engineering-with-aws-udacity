import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load trusted customer and accelerometer
customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="customer_trusted"
)
accel_df = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="accelerometer_trusted"
)

# Join on email = user (re-join for customers that actually have accelerometer data)
joined_df = Join.apply(
    frame1=customer_df,
    frame2=accel_df,
    keys1=["email"],
    keys2=["user"]
)

# Drop duplicates (PII cleanup optional)
final_df = joined_df.drop_fields(["user", "x", "y", "z", "timestamp"])

# Write curated customer data
glueContext.write_dynamic_frame.from_options(
    frame=final_df,
    connection_type="s3",
    connection_options={"path": "s3://cd0030-sdl-amal/customer/curated/"},
    format="json"
)

job.commit()

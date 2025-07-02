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

# Load accelerometer and trusted customer data
accel_df = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="accelerometer_landing"
)
customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="customer_trusted"
)

# Join on email = user
joined_df = Join.apply(
    frame1=accel_df,
    frame2=customer_df,
    keys1=["user"],
    keys2=["email"]
)

# Drop PII fields
selected_df = joined_df.drop_fields(["user", "email", "customername", "phone", "birthday", "registrationdate", "lastupdatedate"])

# Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=selected_df,
    connection_type="s3",
    connection_options={"path": "s3://cd0030-sdl-amal/accelerometer/trusted/"},
    format="json"
)

job.commit()

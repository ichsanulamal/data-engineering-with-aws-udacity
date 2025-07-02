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

# Load customer landing data
customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="customer_landing"
)

# Filter for customers who shared data for research
trusted_df = Filter.apply(
    frame=customer_df,
    f=lambda row: row["sharewithresearchasofdate"] is not None
)

# Write to trusted S3 location
glueContext.write_dynamic_frame.from_options(
    frame=trusted_df,
    connection_type="s3",
    connection_options={"path": "s3://cd0030-sdl-amal/customer/trusted/"},
    format="json"
)

job.commit()

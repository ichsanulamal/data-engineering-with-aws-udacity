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

# Load step trainer trusted and accelerometer trusted
step_df = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="step_trainer_trusted"
)
accel_df = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="accelerometer_trusted"
)

# Join on timestamps
joined_df = Join.apply(
    frame1=accel_df,
    frame2=step_df,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"]
)

# Drop unnecessary fields
clean_df = joined_df.drop_fields(["sensorreadingtime"])

# Write ML-curated dataset
glueContext.write_dynamic_frame.from_options(
    frame=clean_df,
    connection_type="s3",
    connection_options={"path": "s3://cd0030-sdl-amal/ml/curated/"},
    format="json"
)

job.commit()

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

# Load step trainer and curated customer data
step_df = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="step_trainer_landing"
)
customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="customers_curated"
)

# Select rows where serialNumber matches
joined_df = Join.apply(
    frame1=step_df,
    frame2=customer_df,
    keys1=["serialnumber"],
    keys2=["serialnumber"]
)

# Drop unnecessary fields
clean_df = joined_df.drop_fields(["email", "customername", "phone", "birthday", "registrationdate", "lastupdatedate", "sharewithfriendsasofdate", "sharewithpublicasofdate", "sharewithresearchasofdate"])

# Write trusted step trainer data
glueContext.write_dynamic_frame.from_options(
    frame=clean_df,
    connection_type="s3",
    connection_options={"path": "s3://cd0030-sdl-amal/step_trainer/trusted/"},
    format="json"
)

job.commit()

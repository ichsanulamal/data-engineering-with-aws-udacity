import boto3

session = boto3.Session(profile_name="udacity", region_name="us-east-1")
glue = session.client("glue")

# Constants
bucket = "cd0030-sdl-amal"
prefixes = {
    "customer_trusted": "customer_trusted/",
    "accelerometer_trusted": "accelerometer_trusted/",
    "customer_curated": "customer_curated/",
    "step_trainer_trusted": "step_trainer_trusted/",
    "machine_learning_curated": "machine_learning_curated/",
}

database_name = "default"
iam_role = (
    "arn:aws:iam::550842457597:role/service-role/AWSGlueServiceRole"  # <-- Replace this
)


# Create database if not exists
def ensure_database_exists():
    try:
        glue.get_database(Name=database_name)
        print(f"Database '{database_name}' already exists.")
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": database_name})
        print(f"Created Glue database: {database_name}")


def create_or_update_crawler(name, s3_path, table_name):
    s3_target = {
        "Path": f"s3://{bucket}/{s3_path}",
        "Exclusions": [],
        "ConnectionName": "",
    }

    try:
        glue.get_crawler(Name=name)
        glue.update_crawler(
            Name=name,
            Role=iam_role,
            DatabaseName=database_name,
            Targets={"S3Targets": [s3_target]},
            TablePrefix="",  # <- Keep empty for clean names
        )
        print(f"Updated crawler: {name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(
            Name=name,
            Role=iam_role,
            DatabaseName=database_name,
            Targets={"S3Targets": [s3_target]},
            TablePrefix="",  # <- Table name will be just s3_path's last folder
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE",
            },
        )
        print(f"Created crawler: {name}")


# Trigger crawler run
def start_crawler(name):
    glue.start_crawler(Name=name)
    print(f"Started crawler: {name}")


# Execute
for table_name, prefix in prefixes.items():
    crawler_name = f"{table_name}_crawler"
    create_or_update_crawler(crawler_name, prefix, table_name)
    start_crawler(crawler_name)

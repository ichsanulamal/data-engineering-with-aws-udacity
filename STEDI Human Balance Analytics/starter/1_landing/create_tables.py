import boto3

session = boto3.Session(profile_name='udacity', region_name='us-east-1')
athena = session.client('athena')

sql_create_tables_path = [
    './sql/customer_landing.sql',
    './sql/accelerometer_landing.sql',
    './sql/step_trainer_landing.sql'
]

for sql_file in sql_create_tables_path:
    with open(sql_file) as f:
        query = f.read()

    athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'default'},
        ResultConfiguration={'OutputLocation': 's3://cd0030-sdl-amal/query-results/'}
    )
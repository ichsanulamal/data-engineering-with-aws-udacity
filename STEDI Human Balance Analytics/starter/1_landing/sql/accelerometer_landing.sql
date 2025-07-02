CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
  user STRING,
  timestamp BIGINT,
  x FLOAT,
  y FLOAT,
  z FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://cd0030-sdl-amal/accelerometer_landing/';
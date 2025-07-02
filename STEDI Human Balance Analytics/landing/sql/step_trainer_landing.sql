CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
  sensorReadingTime BIGINT,
  serialNumber STRING,
  distanceFromObject INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://cd0030-sdl-amal/step_trainer_landing/';
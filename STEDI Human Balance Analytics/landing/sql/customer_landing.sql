CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
  customerName STRING,
  email STRING,
  phone STRING,
  birthDay DATE,
  serialNumber STRING,
  registrationDate BIGINT,
  lastUpdateDate BIGINT,
  shareWithResearchAsOfDate BIGINT,
  shareWithPublicAsOfDate BIGINT,
  shareWithFriendsAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://cd0030-sdl-amal/customer_landing/';
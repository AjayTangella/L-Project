-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'SQL@13254';
-- GO
-- CREATE DATABASE SCOPED CREDENTIAL cred_aja
-- WITH 
-- IDENTITY = 'Managed Identity';

CREATE EXTERNAL DATA SOURCE soure_silver
WITH
(
    LOCATION='https://awsprojectdatalake.blob.core.windows.net/silver/',
    CREDENTIAL=cred_aja
)

CREATE EXTERNAL DATA SOURCE soure_gold
WITH
(
    LOCATION='https://awsprojectdatalake.blob.core.windows.net/gold/',
    CREDENTIAL=cred_aja
)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE=PARQUET,
    DATA_COMPRESSION='org.apache.hadoop.io.compress.GzipCodec'
)

CREATE EXTERNAL TABLE gold.extsales
WITH(
    LOCATION='extsales',
    DATA_SOURCE=soure_gold,
    FILE_FORMAT=format_parquet
)
AS
SELECT * from gold.sales


select * from gold.extsales





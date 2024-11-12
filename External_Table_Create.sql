CREATE EXTERNAL TABLE `silver_gcastro_ifood_poc`(
  `vendorid` bigint, 
  `passenger_count` bigint, -- passageiros na corrida (valor foi atribuído como double, alterei para bigint por entender que um passageiro sempre contará 1)
  `total_amount` double, -- total valor da corrida
  `tpep_pickup_datetime` timestamp, -- data de abertura da corrida
  `tpep_dropoff_datetime` timestamp, -- data de fechamento da corrida
  `arquivo_origem` string) -- caminho do arquivo no bucket_origem; exemplo: "parquets_origem"\yellow_taxi_tripdata_2023_01.parquet
PARTITIONED BY ( 
  `partition_0` string) -- dados sem partição ... poderia ter aberto por cor/ano/mês | yellow\2023\01
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://silver-gcastro-ifood-poc/' -- caminho para decompor o dado parquet.
TBLPROPERTIES ( -- Dado que pode ser removido, pois não afeta a criação da external table, porém, são dados importantes para catalogo de dados e estruturação das tabelas
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='silver-crawler-gcastro', 
  'averageRecordSize'='2', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='1', 
  'partition_filtering.enabled'='true', 
  'recordCount'='16526016', 
  'sizeKey'='42524293', 
  'typeOfData'='file')
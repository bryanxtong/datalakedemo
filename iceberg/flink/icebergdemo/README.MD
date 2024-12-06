# tables create in this samples

Java examples to write avro/protobuf/json records into iceberg tables using Flink.
It includes confluent and flink official avro/protobuf/json serialize/deserialize libraries

create a hadoop catalog and tables
```
CREATE CATALOG hadoop_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='hdfs://localhost:9000/user/hdfs/warehouse',
  'property-version'='1'
);

CREATE TABLE `hadoop_catalog`.`default`.`sample` (
  id BIGINT COMMENT 'unique id',
  data STRING,
  PRIMARY KEY (`id`) NOT ENFORCED
);

create table `hadoop_catalog`.`default`.`stock_ticks`(
  `volume`  bigint,
  `symbol` string,
  `ts` string,
  `month` string,
  ` high` double,
  `low` double,
  `key` string,
  `year` int,
  `date` string,
  `close` double,
  `open` double,
  `day` string
);
```
create a hive catalog and tables
```
CREATE CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'uri'='thrift://localhost:9083',
  'clients'='5',
  'property-version'='1',
  'warehouse'='hdfs://localhost:9000/user/hive/warehouse'
);

create table `stock_ticks`(
  `volume`  bigint,
  `symbol` string,
  `ts` string,
  `month` string,
  ` high` double,
  `low` double,
  `key` string,
  `year` int,
  `date` string,
  `close` double,
  `open` double,
  `day` string
) WITH (
  'catalog-name'='hive_catalog',
  'catalog-database'='hive_db',
  'uri'='thrift://localhost:9083',
  'warehouse'='hdfs://localhost:9000/user/hive/warehouse'
);

```



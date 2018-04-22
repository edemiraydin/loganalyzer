CREATE TABLE `particle_test` (
`coreid` STRING,
`published_at` STRING,
`data` STRING,
`event` STRING,
`ttl` BIGINT
)
DISTRIBUTE BY HASH (coreid) INTO 16 BUCKETS
TBLPROPERTIES(
 'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
 'kudu.table_name' = 'particle_test',
 'kudu.master_addresses' = 'ip-10-0-0-224.ec2.internal:7051',
 'kudu.key_columns' = 'coreid,published_at',
 'kudu.num_tablet_replicas' = '3'
);
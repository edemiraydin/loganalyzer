CREATE TABLE `ip_counts_last_30` (
`ip` STRING,
`os` STRING,
`count` INT
)
DISTRIBUTE BY HASH (ip) INTO 16 BUCKETS
TBLPROPERTIES(
 'storage_handler' = 'com.cloudera.kudu.hive.KuduStorageHandler',
 'kudu.table_name' = 'p_counts_last_30',
 'kudu.master_addresses' = 'xx:7051',
 'kudu.key_columns' = 'ip',
 'kudu.num_tablet_replicas' = '3'
);

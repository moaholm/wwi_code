CREATE OR REPLACE EXTERNAL TABLE `data-evolution-moa.raw_wwi.customerCategory`(
    CustomerCategoryID STRING,
    CustomerCategoryName STRING,
    LastEditedBy STRING,
    ValidFrom STRING,
    ValidTo STRING
)
WITH PARTITION COLUMNS (
    year STRING,
    month STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://raw-wwi-moa/data-evolution-wwi/csv/sales.customercategories/*.csv'],
    hive_partition_uri_prefix = 'gs://raw-wwi-moa/data-evolution-wwi/csv/sales.customercategories/',
    require_hive_partition_filter = false
    )
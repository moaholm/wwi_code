CREATE OR REPLACE EXTERNAL TABLE `data-evolution-moa.raw_wwi.invoiceLines` (
    invoiceLineID STRING,
    invoiceID STRING,
    stockItemID STRING,
    description STRING,
    packageTypeID STRING,
    quantity STRING,
    unitPrice STRING,
    taxRate STRING,
    taxAmount STRING,
    lineProfit STRING,
    extendedPrice STRING,
    lastEditedBy STRING,
    lastEditedWhen STRING
)
WITH PARTITION COLUMNS (
    year STRING,
    month STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://raw-wwi-moa/data-evolution-wwi/csv/sales.invoicelines/*.csv'],
    hive_partition_uri_prefix = 'gs://raw-wwi-moa/data-evolution-wwi/csv/sales.invoicelines/',
    require_hive_partition_filter = false
    )
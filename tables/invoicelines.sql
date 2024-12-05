CREATE EXTERNAL TABLE `data-evolution-moa.raw_wwi.invoiceLines` (
    InvoiceLineID STRING,
    InvoiceID STRING,
    StockItemID STRING,
    Description STRING,
    PackageTypeID STRING,
    Quantity STRING,
    UnitPrice STRING,
    TaxRate STRING,
    TaxAmount STRING,
    LineProfit STRING,
    ExtendedPrice STRING,
    LastEditedBy STRING,
    LastEditedWhen STRING
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

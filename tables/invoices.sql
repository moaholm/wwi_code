CREATE OR REPLACE EXTERNAL TABLE `data-evolution-moa.raw_wwi.invoices` (
    invoiceID STRING,
    customerID STRING,
    billToCustomerID STRING,
    orderID STRING,
    deliveryMethodID STRING,
    contactPersonID STRING,
    accountsPersonID STRING,
    salespersonPersonID STRING,
    packedByPersonID STRING,
    invoiceDate STRING,
    customerPurchaseOrderNumber STRING,
    isCreditNote STRING,
    creditNoteReason STRING,
    comments STRING,
    deliveryInstructions STRING,
    internalComments STRING,
    totalDryItems STRING,
    totalChillerItems STRING,
    deliveryRun STRING,
    runPosition STRING,
    returnedDeliveryData STRING,
    confirmedDeliveryTime STRING,
    confirmedReceivedBy STRING,
    lastEditedBy STRING,
    lastEditedWhen STRING
)
WITH PARTITION COLUMNS (
    year STRING,
    month STRING
)
OPTIONS (
    format = 'CSV',
    uris = ['gs://raw-wwi-moa/data-evolution-wwi/csv/sales.invoices/*.csv'],
    hive_partition_uri_prefix = 'gs://raw-wwi-moa/data-evolution-wwi/csv/sales.invoices/',
    require_hive_partition_filter = false
    )
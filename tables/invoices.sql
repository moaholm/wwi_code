CREATE EXTERNAL TABLE `data-evolution-moa.raw_wwi.invoices` (
    InvoiceID STRING,
    CustomerID STRING,
    BillToCustomerID STRING,
    OrderID STRING,
    DeliveryMethodID STRING,
    ContactPersonID STRING,
    AccountsPersonID STRING,
    SalespersonPersonID STRING,
    PackedByPersonID STRING,
    InvoiceDate STRING,
    CustomerPurchaseOrderNumber STRING,
    IsCreditNote STRING,
    CreditNoteReason STRING,
    Comments STRING,
    DeliveryInstructions STRING,
    InternalComments STRING,
    TotalDryItems STRING,
    TotalChillerItems STRING,
    DeliveryRun STRING,
    RunPosition STRING,
    ReturnedDeliveryData STRING,
    ConfirmedDeliveryTime STRING,
    ConfirmedReceivedBy STRING,
    LastEditedBy STRING,
    LastEditedWhen STRING
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
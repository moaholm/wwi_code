CREATE TABLE `invoicelines` (
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
PARTITIONED BY (year STRING, month STRING)
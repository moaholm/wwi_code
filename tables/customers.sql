-- Comment to self in the Data Evo assignment: 
-- wouldn't it be reasonable to divide this information into different tables
-- e.g customer, customerAddress, customerBilling and maybe more?
CREATE EXTERNAL TABLE `data-evolution-moa.raw_wwi.customers`(
    CustomerID STRING,
    CustomerName STRING,
    BillToCustomerID STRING,
    CustomerCategoryID STRING,
    BuyingGroupID STRING,
    PrimaryContactPersonID STRING,
    AlternateContactPersonID STRING,
    DeliveryMethodID STRING,
    DeliveryCityID STRING,
    PostalCityID STRING,
    CreditLimit STRING,
    AccountOpenedDate STRING,
    StandardDiscountPercentage STRING,
    IsStatementSent STRING,
    IsOnCreditHold STRING,
    PaymentDays STRING,
    PhoneNumber STRING,
    FaxNumber STRING,
    DeliveryRun STRING,
    RunPosition STRING,
    WebsiteURL STRING,
    DeliveryAddressLine1 STRING,
    DeliveryAddressLine2 STRING,
    DeliveryPostalCode STRING,
    DeliveryLocation STRING,
    PostalAddressLine1 STRING,
    PostalAddressLine2 STRING,
    PostalPostalCode STRING,
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
    uris = ['gs://raw-wwi-moa/data-evolution-wwi/csv/sales.customers/*.csv'],
    hive_partition_uri_prefix = 'gs://raw-wwi-moa/data-evolution-wwi/csv/sales.customers/',
    require_hive_partition_filter = false
    )
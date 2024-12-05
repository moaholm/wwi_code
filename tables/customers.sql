-- Comment to self in the Data Evo assignment: 
-- wouldn't it be reasonable to divide this information into different tables
-- e.g customer, customerAddress, customerBilling and maybe more?
CREATE OR REPLACE EXTERNAL TABLE `data-evolution-moa.raw_wwi.customers`(
    customerID STRING,
    customerName STRING,
    billToCustomerID STRING,
    customerCategoryID STRING,
    buyingGroupID STRING,
    packageTypeIDrimaryContactPersonID STRING,
    accountsPersonIDlternateContactPersonID STRING,
    deliveryMethodID STRING,
    deliveryCityID STRING,
    postalCityID STRING,
    creditLimit STRING,
    accountOpenedDate STRING,
    standardDiscountPercentage STRING,
    isStatementSent STRING,
    isOnCreditHold STRING,
    paymentDays STRING,
    phoneNumber STRING,
    faxNumber STRING,
    deliveryRun STRING,
    runPosition STRING,
    websiteURL STRING,
    deliveryAddressLine1 STRING,
    deliveryAddressLine2 STRING,
    deliveryPostalCode STRING,
    deliveryLocation STRING,
    postalAddressLine1 STRING,
    postalAddressLine2 STRING,
    postalPostalCode STRING,
    lastEditedBy STRING,
    validFrom STRING,
    validTo STRING
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
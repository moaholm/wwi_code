CREATE TABLE `data-evolution-moa.raw_wwi.customerCategory`(
    CustomerCategoryID STRING,
    CustomerCategoryName STRING,
    LastEditedBy STRING,
    ValidFro STRING,
    ValidTo STRING
)
PARTITIONED BY (year STRING, month STRING)

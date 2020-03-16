create database amfi_data;
use amfi_data;
CREATE TABLE amfihistory (
  SchemeCode int(11) NOT NULL,
  SchemeName varchar(90) DEFAULT NULL,
  ISINDivPayoutISINGrowth varchar(45) DEFAULT NULL,
  ISINDivReinvestment varchar(45) DEFAULT NULL,
  NetAssetValue float DEFAULT NULL,
  RepurchasePrice varchar(45) DEFAULT NULL,
  SalePrice varchar(45) DEFAULT NULL,
  Date date DEFAULT NULL
);





CREATE SCHEMA [SalesMigration];

CREATE TABLE [SalesMigration].[DimAccount] ([AccountKey] Int NOT NULL
,[ParentAccountKey] Int NULL
,[AccountCodeAlternateKey] Int NULL
,[ParentAccountCodeAlternateKey] Int NULL
,[AccountDescription] NVarchar(50) NULL
,[AccountType] NVarchar(50) NULL
,[Operator] NVarchar(50) NULL
,[CustomMembers] NVarchar(300) NULL
,[ValueType] NVarchar(50) NULL
,[CustomMemberOptions] NVarchar(200) NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[DimCurrency] ([CurrencyKey] Int NOT NULL
,[CurrencyAlternateKey] NChar(3) NOT NULL
,[CurrencyName] NVarchar(50) NOT NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[DimCustomer] ([CustomerKey] Int NOT NULL
,[GeographyKey] Int NULL
,[CustomerAlternateKey] NVarchar(15) NOT NULL
,[Title] NVarchar(8) NULL
,[FirstName] NVarchar(50) NULL
,[MiddleName] NVarchar(50) NULL
,[LastName] NVarchar(50) NULL
,[NameStyle] Bit NULL
,[BirthDate] Date NULL
,[MaritalStatus] NChar(1) NULL
,[Suffix] NVarchar(10) NULL
,[Gender] NVarchar(1) NULL
,[EmailAddress] NVarchar(50) NULL
,[YearlyIncome] Money NULL
,[TotalChildren] TinyInt NULL
,[NumberChildrenAtHome] TinyInt NULL
,[EnglishEducation] NVarchar(40) NULL
,[SpanishEducation] NVarchar(40) NULL
,[FrenchEducation] NVarchar(40) NULL
,[EnglishOccupation] NVarchar(100) NULL
,[SpanishOccupation] NVarchar(100) NULL
,[FrenchOccupation] NVarchar(100) NULL
,[HouseOwnerFlag] NChar(1) NULL
,[NumberCarsOwned] TinyInt NULL
,[AddressLine1] NVarchar(120) NULL
,[AddressLine2] NVarchar(120) NULL
,[Phone] NVarchar(20) NULL
,[DateFirstPurchase] Date NULL
,[CommuteDistance] NVarchar(15) NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[DimDate] ([DateKey] Int NOT NULL
,[FullDateAlternateKey] Date NOT NULL
,[DayNumberOfWeek] TinyInt NOT NULL
,[EnglishDayNameOfWeek] NVarchar(10) NOT NULL
,[SpanishDayNameOfWeek] NVarchar(10) NOT NULL
,[FrenchDayNameOfWeek] NVarchar(10) NOT NULL
,[DayNumberOfMonth] TinyInt NOT NULL
,[DayNumberOfYear] SmallInt NOT NULL
,[WeekNumberOfYear] TinyInt NOT NULL
,[EnglishMonthName] NVarchar(10) NOT NULL
,[SpanishMonthName] NVarchar(10) NOT NULL
,[FrenchMonthName] NVarchar(10) NOT NULL
,[MonthNumberOfYear] TinyInt NOT NULL
,[CalendarQuarter] TinyInt NOT NULL
,[CalendarYear] SmallInt NOT NULL
,[CalendarSemester] TinyInt NOT NULL
,[FiscalQuarter] TinyInt NOT NULL
,[FiscalYear] SmallInt NOT NULL
,[FiscalSemester] TinyInt NOT NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[DimGeography] ([GeographyKey] Int NOT NULL
,[City] NVarchar(30) NULL
,[StateProvinceCode] NVarchar(3) NULL
,[StateProvinceName] NVarchar(50) NULL
,[CountryRegionCode] NVarchar(3) NULL
,[EnglishCountryRegionName] NVarchar(50) NULL
,[SpanishCountryRegionName] NVarchar(50) NULL
,[FrenchCountryRegionName] NVarchar(50) NULL
,[PostalCode] NVarchar(15) NULL
,[SalesTerritoryKey] Int NULL
,[IpAddressLocator] NVarchar(15) NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[DimProduct] ([ProductKey] Int NOT NULL
,[ProductAlternateKey] NVarchar(25) NULL
,[ProductSubcategoryKey] Int NULL
,[WeightUnitMeasureCode] NChar(3) NULL
,[SizeUnitMeasureCode] NChar(3) NULL
,[EnglishProductName] NVarchar(50) NOT NULL
,[SpanishProductName] NVarchar(50) NOT NULL
,[FrenchProductName] NVarchar(50) NOT NULL
,[StandardCost] Money NULL
,[FinishedGoodsFlag] Bit NOT NULL
,[Color] NVarchar(15) NOT NULL
,[SafetyStockLevel] SmallInt NULL
,[ReorderPoint] SmallInt NULL
,[ListPrice] Money NULL
,[Size] NVarchar(50) NULL
,[SizeRange] NVarchar(50) NULL
,[Weight] Float NULL
,[DaysToManufacture] Int NULL
,[ProductLine] NChar(2) NULL
,[DealerPrice] Money NULL
,[Class] NChar(2) NULL
,[Style] NChar(2) NULL
,[ModelName] NVarchar(50) NULL
,[EnglishDescription] NVarchar(400) NULL
,[FrenchDescription] NVarchar(400) NULL
,[ChineseDescription] NVarchar(400) NULL
,[ArabicDescription] NVarchar(400) NULL
,[HebrewDescription] NVarchar(400) NULL
,[ThaiDescription] NVarchar(400) NULL
,[GermanDescription] NVarchar(400) NULL
,[JapaneseDescription] NVarchar(400) NULL
,[TurkishDescription] NVarchar(400) NULL
,[StartDate] DateTime NULL
,[EndDate] DateTime NULL
,[Status] NVarchar(7) NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[DimProductCategory] ([ProductCategoryKey] Int NOT NULL
,[ProductCategoryAlternateKey] Int NULL
,[EnglishProductCategoryName] NVarchar(50) NOT NULL
,[SpanishProductCategoryName] NVarchar(50) NOT NULL
,[FrenchProductCategoryName] NVarchar(50) NOT NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[DimProductSubcategory] ([ProductSubcategoryKey] Int NOT NULL
,[ProductSubcategoryAlternateKey] Int NULL
,[EnglishProductSubcategoryName] NVarchar(50) NOT NULL
,[SpanishProductSubcategoryName] NVarchar(50) NOT NULL
,[FrenchProductSubcategoryName] NVarchar(50) NOT NULL
,[ProductCategoryKey] Int NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[DimReseller] ([ResellerKey] Int NOT NULL
,[GeographyKey] Int NULL
,[ResellerAlternateKey] NVarchar(15) NULL
,[Phone] NVarchar(25) NULL
,[BusinessType] VarChar(20) NOT NULL
,[ResellerName] NVarchar(50) NOT NULL
,[NumberEmployees] Int NULL
,[OrderFrequency] Char(1) NULL
,[OrderMonth] TinyInt NULL
,[FirstOrderYear] Int NULL
,[LastOrderYear] Int NULL
,[ProductLine] NVarchar(50) NULL
,[AddressLine1] NVarchar(60) NULL
,[AddressLine2] NVarchar(60) NULL
,[AnnualSales] Money NULL
,[BankName] NVarchar(50) NULL
,[MinPaymentType] TinyInt NULL
,[MinPaymentAmount] Money NULL
,[AnnualRevenue] Money NULL
,[YearOpened] Int NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[DimSalesReason] ([SalesReasonKey] Int NOT NULL
,[SalesReasonAlternateKey] Int NOT NULL
,[SalesReasonName] NVarchar(50) NOT NULL
,[SalesReasonReasonType] NVarchar(50) NOT NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[FactInternetSales] ([ProductKey] Int NOT NULL
,[OrderDateKey] Int NOT NULL
,[DueDateKey] Int NOT NULL
,[ShipDateKey] Int NOT NULL
,[CustomerKey] Int NOT NULL
,[PromotionKey] Int NOT NULL
,[CurrencyKey] Int NOT NULL
,[SalesTerritoryKey] Int NOT NULL
,[SalesOrderNumber] NVarchar(20) NOT NULL
,[SalesOrderLineNumber] TinyInt NOT NULL
,[RevisionNumber] TinyInt NOT NULL
,[OrderQuantity] SmallInt NOT NULL
,[UnitPrice] Money NOT NULL
,[ExtendedAmount] Money NOT NULL
,[UnitPriceDiscountPct] Float NOT NULL
,[DiscountAmount] Float NOT NULL
,[ProductStandardCost] Money NOT NULL
,[TotalProductCost] Money NOT NULL
,[SalesAmount] Money NOT NULL
,[TaxAmt] Money NOT NULL
,[Freight] Money NOT NULL
,[CarrierTrackingNumber] NVarchar(25) NULL
,[CustomerPONumber] NVarchar(25) NULL
,[OrderDate] DateTime NULL
,[DueDate] DateTime NULL
,[ShipDate] DateTime NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([ProductKey]))

CREATE TABLE [SalesMigration].[FactInternetSalesReason] ([SalesOrderNumber] NVarchar(20) NOT NULL
,[SalesOrderLineNumber] TinyInt NOT NULL
,[SalesReasonKey] Int NOT NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN)

CREATE TABLE [SalesMigration].[FactProductInventory] ([ProductKey] Int NOT NULL
,[DateKey] Int NOT NULL
,[MovementDate] Date NOT NULL
,[UnitCost] Money NOT NULL
,[UnitsIn] Int NOT NULL
,[UnitsOut] Int NOT NULL
,[UnitsBalance] Int NOT NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([ProductKey]))

CREATE TABLE [SalesMigration].[FactResellerSales] ([ProductKey] Int NOT NULL
,[OrderDateKey] Int NOT NULL
,[DueDateKey] Int NOT NULL
,[ShipDateKey] Int NOT NULL
,[ResellerKey] Int NOT NULL
,[EmployeeKey] Int NOT NULL
,[PromotionKey] Int NOT NULL
,[CurrencyKey] Int NOT NULL
,[SalesTerritoryKey] Int NOT NULL
,[SalesOrderNumber] NVarchar(20) NOT NULL
,[SalesOrderLineNumber] TinyInt NOT NULL
,[RevisionNumber] TinyInt NULL
,[OrderQuantity] SmallInt NULL
,[UnitPrice] Money NULL
,[ExtendedAmount] Money NULL
,[UnitPriceDiscountPct] Float NULL
,[DiscountAmount] Float NULL
,[ProductStandardCost] Money NULL
,[TotalProductCost] Money NULL
,[SalesAmount] Money NULL
,[TaxAmt] Money NULL
,[Freight] Money NULL
,[CarrierTrackingNumber] NVarchar(25) NULL
,[CustomerPONumber] NVarchar(25) NULL
,[OrderDate] DateTime NULL
,[DueDate] DateTime NULL
,[ShipDate] DateTime NULL
) WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([ProductKey]))


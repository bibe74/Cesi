
/**
 * @table Staging.MySolutionCustomer
 * @description

 * @depends Landing.MYSOLUTION_Customer
 * @depends Landing.MYSOLUTION_CustomerAddresses
 * @depends Landing.MYSOLUTION_Address
 * @depends Landing.MYSOLUTION_GenericAttribute
 * @depends Landing.MYSOLUTION_Country
 * @depends Landing.MYSOLUTION_StateProvince
 * @depends Landing.MYSOLUTION_Customer_CustomerRole_Mapping

SELECT TOP (1) * FROM Landing.MYSOLUTION_Customer;
SELECT TOP (1) * FROM Landing.MYSOLUTION_CustomerAddresses;
SELECT TOP (1) * FROM Landing.MYSOLUTION_Address;
SELECT TOP (1) * FROM Landing.MYSOLUTION_GenericAttribute;
SELECT TOP (1) * FROM Landing.MYSOLUTION_Country;
SELECT TOP (1) * FROM Landing.MYSOLUTION_StateProvince;
SELECT TOP (1) * FROM Landing.MYSOLUTION_Customer_CustomerRole_Mapping;
*/

CREATE   VIEW Staging.MySolutionCustomerView
AS
WITH TableDataDetail
AS (
    SELECT
        C.Id,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            C.Username,
            C.Email,
            C.IdCometa,
            GA1.Value,
            GA2.Value,
            GA3.Value,
            GA4.Value,
            GA5.Value,
            GA6.Value,
            GA7.Value,
            GA8.Value,
            GA9.Value,
            GA10.Value,
            GA11.Value,
            CY.Name,
            COALESCE(SPC.Id, GA12.Value),
            COALESCE(SPC.Name, SP.Name, A.StateProvince),
            CCRM11.Customer_Id,
            CCRM12.Customer_Id,
            CONVERT(DATE, C.CreatedOnUtc),
            CONVERT(DATE, C.DateExpiration),
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        LOWER(COALESCE(C.Username, C.Email)) AS Username,
        LOWER(C.Email) AS Email,
        C.IdCometa,
        COALESCE(GA1.Value, A.Company, N'') AS Company,
        COALESCE(GA2.Value, A.CodiceFiscale, N'') AS CodiceFiscale,
        COALESCE(GA3.Value, A.Piva, N'') AS VATNumber,
        COALESCE(GA4.Value, A.FirstName, N'') AS FirstName,
        COALESCE(GA5.Value, A.LastName, N'') AS LastName,
        COALESCE(GA6.Value, A.Address1 + N' ' + COALESCE(A.Address2, N''), N'') AS StreetAddress,
        COALESCE(GA7.Value, A.ZipPostalCode, N'') AS ZipPostalCode,
        COALESCE(GA8.Value, A.PhoneNumber, N'') AS Phone,
        COALESCE(GA9.Value, N'') AS Cellulare,
        COALESCE(GA10.Value, A.City, N'') AS City,
        COALESCE(GA11.Value, N'') AS CountryId,
        COALESCE(CY.Name, A.Country, N'') AS Country,
        COALESCE(SPC.Id, GA12.Value, N'') AS StateProvinceId,
        COALESCE(SPC.Name, SP.Name, A.StateProvince, N'') AS StateProvince,
        ROW_NUMBER() OVER (PARTITION BY C.Id ORDER BY A.Id DESC) AS rnAddressDESC,
        ROW_NUMBER() OVER (PARTITION BY C.Username ORDER BY C.Id DESC, A.Id DESC) AS rnCustomerDESC,
        CASE WHEN CCRM11.Customer_Id IS NOT NULL THEN 1 ELSE 0 END AS HasRoleMySolutionDemo,
        CASE WHEN CCRM12.Customer_Id IS NOT NULL THEN 1 ELSE 0 END AS HasRoleMySolutionInterno,
        CONVERT(DATE, C.CreatedOnUtc) AS CreatedOnUtc,
        CONVERT(DATE, C.DateExpiration) AS DateExpiration

    FROM Landing.MYSOLUTION_Customer C
    LEFT JOIN Landing.MYSOLUTION_CustomerAddresses CA ON CA.Customer_Id = C.Id
        AND CA.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_Address A ON A.Id = CA.Address_Id
        AND A.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA1 ON GA1.EntityId = C.Id
        AND GA1.[Key] = N'Company'
        AND GA1.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA2 ON GA2.EntityId = C.Id
        AND GA2.[Key] = N'CodiceFiscale'
        AND GA2.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA3 ON GA3.EntityId = C.Id
        AND GA3.[Key] = N'VATNumber'
        AND GA3.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA4 ON GA4.EntityId = C.Id
        AND GA4.[Key] = N'FirstName'
        AND GA4.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA5 ON GA5.EntityId = C.Id
        AND GA5.[Key] = N'LastName'
        AND GA5.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA6 ON GA6.EntityId = C.Id
        AND GA6.[Key] = N'StreetAddress'
        AND GA6.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA7 ON GA7.EntityId = C.Id
        AND GA7.[Key] = N'ZipPostalCode'
        AND GA7.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA8 ON GA8.EntityId = C.Id
        AND GA8.[Key] = N'Phone'
        AND GA8.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA9 ON GA9.EntityId = C.Id
        AND GA9.[Key] = N'Cellulare'
        AND GA9.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA10 ON GA10.EntityId = C.Id
        AND GA10.[Key] = N'City'
        AND GA10.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA11 ON GA11.EntityId = C.Id
        AND GA11.[Key] = N'CountryId'
        AND GA11.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_Country CY ON CY.Id = GA11.Value
        AND CY.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA12 ON GA12.EntityId = C.Id
        AND GA12.[Key] = N'StateProvinceId'
        AND GA12.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_StateProvince SP ON SP.Id = GA12.Value
        AND SP.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_Customer_CustomerRole_Mapping CCRM11 ON CCRM11.Customer_Id = C.Id
        AND CCRM11.CustomerRole_Id = 11 -- 11: MySolution.Demo
        AND CCRM11.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_Customer_CustomerRole_Mapping CCRM12 ON CCRM12.Customer_Id = C.Id
        AND CCRM12.CustomerRole_Id = 12 -- 12: MySolution.Interno
        AND CCRM12.IsDeleted = CAST(0 AS BIT)
    LEFT JOIN Landing.MYSOLUTION_StateProvince SPC ON SPC.Id = C.StateProvinceId
        AND SPC.IsDeleted = CAST(0 AS BIT)
    WHERE C.IsDeleted = CAST(0 AS BIT)
)
SELECT
    -- Chiavi
    TDD.Id,

    -- Campi per sincronizzazione
    TDD.HistoricalHashKey,
    TDD.ChangeHashKey,
    CONVERT(VARCHAR(34), TDD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TDD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TDD.InsertDatetime,
    TDD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TDD.Username,
    TDD.Email,
    TDD.IdCometa,
    TDD.Company,
    TDD.CodiceFiscale,
    TDD.VATNumber,
    TDD.FirstName,
    TDD.LastName,
    TDD.StreetAddress,
    TDD.ZipPostalCode,
    TDD.Phone,
    TDD.Cellulare,
    TDD.City,
    TDD.CountryId,
    TDD.Country,
    TDD.StateProvinceId,
    TDD.StateProvince,
    TDD.rnCustomerDESC,
    TDD.HasRoleMySolutionDemo,
    TDD.HasRoleMySolutionInterno,
    TDD.CreatedOnUtc,
    TDD.DateExpiration

FROM TableDataDetail TDD
WHERE TDD.rnAddressDESC = 1;

GO


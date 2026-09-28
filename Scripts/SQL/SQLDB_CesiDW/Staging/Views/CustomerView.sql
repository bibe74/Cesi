
CREATE VIEW Staging.CustomerView
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
            GA12.Value,
            SP.Name,
            CCRM11.Customer_Id,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        LOWER(C.Username) AS Username,
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
        COALESCE(GA12.Value, N'') AS StateProvinceId,
        COALESCE(SP.Name, A.StateProvince, N'') AS StateProvince,
        ROW_NUMBER() OVER (PARTITION BY C.Id ORDER BY A.Id DESC) AS rnAddressDESC,
        ROW_NUMBER() OVER (PARTITION BY C.Username ORDER BY C.Id DESC, A.Id DESC) AS rnCustomerDESC,
        CASE WHEN CCRM11.Customer_Id IS NOT NULL THEN 1 ELSE 0 END AS HasRoleMySolutionDemo

    FROM Landing.MYSOLUTION_Customer C
    LEFT JOIN Landing.MYSOLUTION_CustomerAddresses CA ON CA.Customer_Id = C.Id
    LEFT JOIN Landing.MYSOLUTION_Address A ON A.Id = CA.Address_Id
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA1 ON GA1.EntityId = C.Id
        AND GA1.[Key] = N'Company'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA2 ON GA2.EntityId = C.Id
        AND GA2.[Key] = N'CodiceFiscale'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA3 ON GA3.EntityId = C.Id
        AND GA3.[Key] = N'VATNumber'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA4 ON GA4.EntityId = C.Id
        AND GA4.[Key] = N'FirstName'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA5 ON GA5.EntityId = C.Id
        AND GA5.[Key] = N'LastName'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA6 ON GA6.EntityId = C.Id
        AND GA6.[Key] = N'StreetAddress'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA7 ON GA7.EntityId = C.Id
        AND GA7.[Key] = N'ZipPostalCode'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA8 ON GA8.EntityId = C.Id
        AND GA8.[Key] = N'Phone'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA9 ON GA9.EntityId = C.Id
        AND GA9.[Key] = N'Cellulare'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA10 ON GA10.EntityId = C.Id
        AND GA10.[Key] = N'City'
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA11 ON GA11.EntityId = C.Id
        AND GA11.[Key] = N'CountryId'
    LEFT JOIN Landing.MYSOLUTION_Country CY ON CY.Id = GA11.Value
    LEFT JOIN Landing.MYSOLUTION_GenericAttribute GA12 ON GA12.EntityId = C.Id
        AND GA12.[Key] = N'StateProvinceId'
    LEFT JOIN Landing.MYSOLUTION_StateProvince SP ON SP.Id = GA12.Value
    LEFT JOIN Landing.MYSOLUTION_Customer_CustomerRole_Mapping CCRM11 ON CCRM11.Customer_Id = C.Id
        AND CCRM11.CustomerRole_Id = 11 -- 11: MySolution.Demo
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
    TDD.HasRoleMySolutionDemo

FROM TableDataDetail TDD
WHERE TDD.rnAddressDESC = 1;

GO


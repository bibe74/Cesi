
CREATE VIEW Landing.MYSOLUTION_AddressView
AS
WITH TableData
AS (
    SELECT
        A.Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            A.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            A.FirstName,
            A.LastName,
            A.Email,
            A.Company,
            C.Name,
            SP.Name,
            A.City,
            A.Address1,
            A.Address2,
            A.ZipPostalCode,
            A.PhoneNumber,
            A.County,
            A.CodiceFiscale,
            A.Piva,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        COALESCE(A.FirstName, N'') AS FirstName,
        COALESCE(A.LastName, N'') AS LastName,
        COALESCE(A.Email, N'') AS Email,
        COALESCE(A.Company, N'') AS Company,
        --A.CountryId,
        COALESCE(C.Name, N'') AS Country,
        --A.StateProvinceId,
        COALESCE(SP.Name, N'') AS StateProvince,
        COALESCE(A.City, N'') AS City,
        COALESCE(A.Address1, N'') AS Address1,
        COALESCE(A.Address2, N'') AS Address2,
        COALESCE(A.ZipPostalCode, N'') AS ZipPostalCode,
        COALESCE(A.PhoneNumber, N'') AS PhoneNumber,
        COALESCE(A.County, N'') AS County,
        COALESCE(A.CodiceFiscale, N'') AS CodiceFiscale,
        COALESCE(A.Piva, N'') AS Piva

    FROM MYSOLUTION.Address A
    LEFT JOIN MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.Country C ON C.Id = A.CountryId
    LEFT JOIN MYSOLUTIONPRODUZIONE2.Nop_MySolution.dbo.StateProvince SP ON SP.Id = A.StateProvinceId
)
SELECT
    -- Chiavi
    TD.Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.FirstName,
    TD.LastName,
    TD.Email,
    TD.Company,
    TD.Country,
    TD.StateProvince,
    TD.City,
    TD.Address1,
    TD.Address2,
    TD.ZipPostalCode,
    TD.PhoneNumber,
    TD.County,
    TD.CodiceFiscale,
    TD.Piva

FROM TableData TD;

GO


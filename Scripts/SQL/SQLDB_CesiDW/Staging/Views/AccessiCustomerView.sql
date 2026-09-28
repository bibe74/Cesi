
/**
 * @table Staging.AccessiCustomer
 * @description

 * @depends Landing.MYSOLUTION_LogsForReport

SELECT TOP (1) * FROM Landing.MYSOLUTION_LogsForReport;
*/

CREATE   VIEW Staging.AccessiCustomerView
AS
WITH TableDataDetail
AS (
    SELECT
        AU.Username,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AU.Username,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CC.id_sog_commerciale,
            MSC.Id,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        CC.id_sog_commerciale AS IDSoggettoCommerciale,
        MSC.Id AS IDMySolution,
        ROW_NUMBER() OVER (PARTITION BY AU.Username ORDER BY CC.id_sog_commerciale DESC, MSC.Id DESC) AS rn

    FROM Staging.AccessiUsername AU
    LEFT JOIN Staging.CometaCustomer CC ON CC.Email = AU.Username
    LEFT JOIN Staging.MySolutionCustomer MSC ON MSC.Email = AU.Username
)
SELECT
    -- Chiavi
    TDD.Username,

    -- Campi per sincronizzazione
    TDD.HistoricalHashKey,
    TDD.ChangeHashKey,
    CONVERT(VARCHAR(34), TDD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TDD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TDD.InsertDatetime,
    TDD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TDD.IDSoggettoCommerciale,
    TDD.IDMySolution

FROM TableDataDetail TDD
WHERE TDD.rn = 1;

GO


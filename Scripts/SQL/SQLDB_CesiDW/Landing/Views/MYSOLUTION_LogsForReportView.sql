
CREATE VIEW Landing.MYSOLUTION_LogsForReportView
AS
WITH AggregatedData
AS (
    SELECT
        CAST(DataOra AS DATE) AS Data,
        Username,
        SUM(CASE WHEN PageType = N'Login' THEN 1 ELSE 0 END) AS NumeroAccessi,
        COUNT(1) AS NumeroPagineVisitate,
        SUM(CASE WHEN PageType = N'Login' AND Url LIKE N'%BILANCIOCRISI%' THEN 1 ELSE 0 END) AS NumeroAccessiBC,
        SUM(CASE WHEN Url LIKE N'%BILANCIOCRISI%' THEN 1 ELSE 0 END) AS NumeroPagineVisitateBC

    FROM MYSOLUTION.LogsEpiServer
    WHERE COALESCE(Username, N'') <> N''
    GROUP BY CAST(DataOra AS DATE),
        Username
),
TableData
AS (
    SELECT
        AD.Data,
        AD.Username,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AD.Data,
            AD.Username,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AD.NumeroAccessi,
            AD.NumeroPagineVisitate,
            AD.NumeroAccessiBC,
            AD.NumeroPagineVisitateBC,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        AD.NumeroAccessi,
        AD.NumeroPagineVisitate,
        AD.NumeroAccessiBC,
        AD.NumeroPagineVisitateBC

    FROM AggregatedData AD
    WHERE AD.Data > CAST('19000101' AS DATE)
)
SELECT
    -- Chiavi
    TD.Data, -- PKData
    TD.Username, -- Username

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.NumeroAccessi,
    TD.NumeroPagineVisitate,
    TD.NumeroAccessiBC,
    TD.NumeroPagineVisitateBC

FROM TableData TD;

GO


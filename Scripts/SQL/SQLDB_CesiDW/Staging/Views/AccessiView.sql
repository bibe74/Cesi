
CREATE VIEW Staging.AccessiView
AS
WITH UtentiConPagineVisitate
AS (
    SELECT DISTINCT Username
    FROM Landing.MYSOLUTION_LogsForReport LFR
),
UtentiConPagineVisitateClienti
AS (
    SELECT
        UCPV.Username,
        C.PKCliente

    FROM UtentiConPagineVisitate UCPV
    INNER JOIN Staging.SoggettoCommerciale_Email SCE ON SCE.Email = UCPV.Username
        AND SCE.rnSoggettoCommercialeDESC = 1
    INNER JOIN Dim.Cliente C ON C.IDSoggettoCommerciale = SCE.IDSoggettoCommerciale
        AND C.IsDeleted = CAST(0 AS BIT)

    UNION ALL

    SELECT
        UCPV.Username,
        C.PKCliente

    FROM UtentiConPagineVisitate UCPV
    LEFT JOIN Staging.SoggettoCommerciale_Email SCE ON SCE.Email = UCPV.Username
        AND SCE.rnSoggettoCommercialeDESC = 1
    INNER JOIN Dim.Cliente C ON C.Email = UCPV.Username
        AND C.HasAnagraficaCometa = CAST(0 AS BIT)
        AND C.IsDeleted = CAST(0 AS BIT)
    WHERE SCE.Email IS NULL
),
AccessiDettaglio
AS (
    SELECT
        --LFR.Data,
        COALESCE(D.PKData, CAST('19000101' AS DATE)) AS PKData,
        --LFR.IDUser,
        COALESCE(UCAC.PKCliente, -101) AS PKCliente,
        LFR.NumeroAccessi,
        LFR.NumeroPagineVisitate

    FROM Landing.MYSOLUTION_LogsForReport LFR
    LEFT JOIN Dim.Data D ON D.PKData = LFR.Data
    LEFT JOIN UtentiConPagineVisitateClienti UCAC ON UCAC.Username = LFR.Username
    WHERE LFR.IsDeleted = CAST(0 AS BIT)
        AND LFR.Username <> N''
),
TableData
AS (
    SELECT
        AD.PKData,
        AD.PKCliente,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            AD.PKData,
            AD.PKCliente,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            GA.PKCapoArea,
            SUM(AD.NumeroAccessi),
            SUM(AD.NumeroPagineVisitate),
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        GA.PKCapoArea,
        SUM(AD.NumeroAccessi) AS NumeroAccessi,
        SUM(AD.NumeroPagineVisitate) AS NumeroPagineVisitate

    FROM AccessiDettaglio AD
    INNER JOIN Dim.Cliente C ON C.PKCliente = AD.PKCliente
    INNER JOIN Dim.GruppoAgenti GA ON GA.PKGruppoAgenti = C.PKGruppoAgenti
    GROUP BY AD.PKData,
        AD.PKCliente,
        GA.PKCapoArea
)
SELECT
    -- Chiavi
    TD.PKData,
    TD.PKCliente,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Dimensioni
    TD.PKCapoArea,

    -- Misure
    TD.NumeroAccessi,
    TD.NumeroPagineVisitate

FROM TableData TD;

GO


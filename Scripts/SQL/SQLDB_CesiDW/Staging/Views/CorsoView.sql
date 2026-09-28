CREATE VIEW Staging.CorsoView
AS
WITH TableData
AS (
    SELECT
        W.Source AS IDCorso,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            W.Source,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            W.CourseTitle,
            W.CourseType,
            W.VideoTitle,
            W.VideoStartDate,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        W.CourseTitle AS Corso,
        W.CourseType AS TipoCorso,
        W.VideoTitle AS Giornata,
        CONVERT(DATE, W.VideoStartDate) AS DataInizioCorso,
        COALESCE(DIC.PKData, CAST('19000101' AS DATE)) AS PKDataInizioCorso,
        FORMAT(W.VideoStartDate, 'HH:mm') AS OraInizioCorso

    FROM Landing.WEBINARS_WeBinars W
    LEFT JOIN Dim.Data DIC ON DIC.PKData = CONVERT(DATE, W.VideoStartDate)
    WHERE W.IsDeleted = CAST(0 AS BIT)
)
SELECT
    -- Chiavi
    TD.IDCorso,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.Corso,
    TD.TipoCorso,
    TD.Giornata,
    TD.PKDataInizioCorso,
    TD.OraInizioCorso

FROM TableData TD;

GO


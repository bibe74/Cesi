
CREATE VIEW Staging.SoggettoCommerciale_EmailView
AS
WITH TelefonoSoggettoCommerciale
AS (
    SELECT DISTINCT
        SC.IDSoggettoCommerciale,
        T.num_riferimento AS Email

    FROM Landing.COMETA_Telefono T
    INNER JOIN Staging.SoggettoCommerciale SC ON SC.IDAnagrafica = T.id_anagrafica
        AND SC.TipoSoggettoCommerciale = 'C'
    WHERE T.tipo = 'E'
        AND T.num_riferimento LIKE N'%@%'
        AND T.descrizione = N'Abbonato'
        AND T.IsDeleted = CAST(0 AS BIT)
),
TableData
AS (
    SELECT
        TSC.IDSoggettoCommerciale,
        TSC.Email,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            TSC.IDSoggettoCommerciale,
            TSC.Email,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            ROW_NUMBER() OVER (PARTITION BY TSC.IDSoggettoCommerciale ORDER BY TSC.Email),
            ROW_NUMBER() OVER (PARTITION BY TSC.Email ORDER BY TSC.IDSoggettoCommerciale DESC),
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        ROW_NUMBER() OVER (PARTITION BY TSC.IDSoggettoCommerciale ORDER BY TSC.Email) AS rnEmail,
        ROW_NUMBER() OVER (PARTITION BY TSC.Email ORDER BY TSC.IDSoggettoCommerciale DESC) AS rnSoggettoCommercialeDESC

    FROM TelefonoSoggettoCommerciale TSC
)
SELECT
    -- Chiavi
    TD.IDSoggettoCommerciale,
    TD.Email,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.rnEmail,
    TD.rnSoggettoCommercialeDESC

FROM TableData TD;

GO


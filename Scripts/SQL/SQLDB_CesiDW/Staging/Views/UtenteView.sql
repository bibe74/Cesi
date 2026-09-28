CREATE VIEW Staging.UtenteView
AS
WITH EmailCliente
AS (
    SELECT
        C.Email,
        C.HasAnagraficaCometa,
        C.HasAnagraficaNopCommerce,
        C.HasAnagraficaMySolution,
        C.PKCliente,
        ROW_NUMBER() OVER (PARTITION BY C.Email ORDER BY C.PKCliente DESC) AS rn
    FROM Dim.Cliente C
    WHERE C.Email <> N''
        AND C.IsDeleted = CAST(0 AS BIT)
),
TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.ID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            T.Email,
            T.RagioneSociale,
            T.Nome,
            T.Cognome,
            T.Citta,
            COALESCE(EC.PKCliente, -1),
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        T.ID,
        COALESCE(T.Email, N'') AS Email,
        T.RagioneSociale,
        T.Nome,
        T.Cognome,
        T.Citta,
        COALESCE(EC.PKCliente, -1) AS PKCliente

    FROM Landing.MYSOLUTION_Users T
    LEFT JOIN EmailCliente EC ON EC.Email = T.Email
        AND EC.rn = 1
)
SELECT
    -- Chiavi
    TD.ID AS IDUtente,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Altri campi
    TD.Email,
    TD.RagioneSociale,
    TD.Nome,
    TD.Cognome,
    TD.Citta,
    TD.PKCliente

FROM TableData TD;

GO



CREATE VIEW Landing.MYSOLUTION_UsersView
AS
WITH TableData
AS (
    SELECT
        U.ID,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            U.ID,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            U.EMAIL,
            U.RagioneSociale,
            U.Nome,
            U.Cognome,
            U.Citta,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        U.EMAIL AS Email,
        U.RagioneSociale,
        U.Nome,
        U.Cognome,
        U.Citta

    FROM MYSOLUTION.Users U
)
SELECT
    -- Chiavi
    TD.ID,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.Email,
    TD.RagioneSociale,
    TD.Nome,
    TD.Cognome,
    TD.Citta

FROM TableData TD;

GO


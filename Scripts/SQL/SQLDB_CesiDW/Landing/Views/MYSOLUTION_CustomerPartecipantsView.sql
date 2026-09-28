
CREATE VIEW Landing.MYSOLUTION_CustomerPartecipantsView
AS
WITH TableData
AS (
    SELECT
        CP.Customer_Id,
        CP.Partecipant_Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CP.Customer_Id,
            CP.Partecipant_Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            CP.Customer_Id,
            CP.Partecipant_Id,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime

    FROM MYSOLUTION.CustomerPartecipants CP
)
SELECT
    -- Chiavi
    TD.Customer_Id,
    TD.Partecipant_Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted

FROM TableData TD;

GO


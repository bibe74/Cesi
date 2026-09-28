
CREATE VIEW Landing.MYSOLUTION_PartecipantView
AS
WITH TableData
AS (
    SELECT
        P.Id,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            P.Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            P.FirstName,
            P.LastName,
            P.Email,
            P.PhoneNumber,
            P.Ssn,
            P.CreatedOnUtc,
            P.IdProfession,
            P.IdProfessionDetail,
            P.MobilePhone,
            P.OriginalPartecipantId,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        P.FirstName,
        P.LastName,
        P.Email,
        P.PhoneNumber,
        P.Ssn,
        P.CreatedOnUtc,
        P.IdProfession,
        P.IdProfessionDetail,
        P.MobilePhone,
        P.OriginalPartecipantId

    FROM MySOLUTION.Partecipant P
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
    TD.PhoneNumber,
    TD.Ssn,
    TD.CreatedOnUtc,
    TD.IdProfession,
    TD.IdProfessionDetail,
    TD.MobilePhone,
    TD.OriginalPartecipantId

FROM TableData TD;

GO


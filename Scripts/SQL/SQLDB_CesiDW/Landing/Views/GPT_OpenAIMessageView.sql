
CREATE VIEW Landing.GPT_OpenAIMessageView
AS
WITH TableData
AS (
    SELECT

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            OpenAIThreadId,
            Message,
            IsQuestion,
            CreatedOn,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,
        Id,
        OpenAIThreadId,
        Message,
        IsQuestion,
        CreatedOn

    FROM GPT.OpenAIMessage
    WHERE CreatedOn >= CAST('20250901' AS SMALLDATETIME)
)
SELECT
    -- Chiavi
    0+TD.Id AS Id,

    -- Campi per sincronizzazione
    TD.HistoricalHashKey,
    TD.ChangeHashKey,
    CONVERT(VARCHAR(34), TD.HistoricalHashKey, 1) AS HistoricalHashKeyASCII,
    CONVERT(VARCHAR(34), TD.ChangeHashKey, 1) AS ChangeHashKeyASCII,
    TD.InsertDatetime,
    TD.UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,

    -- Attributi
    TD.OpenAIThreadId,
    TD.Message,
    TD.IsQuestion,
    TD.CreatedOn

FROM TableData TD;

GO


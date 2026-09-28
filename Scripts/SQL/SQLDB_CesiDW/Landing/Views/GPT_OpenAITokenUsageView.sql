
CREATE VIEW Landing.GPT_OpenAITokenUsageView
AS
WITH TableData
AS (
    SELECT
       Id,

        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            Id,
            ' '
        ))) AS HistoricalHashKey,
        CONVERT(VARBINARY(20), HASHBYTES('MD5', CONCAT(
            email,
            threadId,
            conversationId,
            createdOn,
            assistantArea,
            responseMode,
            model,
            vectorStorageId,
            input_tokens,
            output_tokens,
            reasoning_tokens,
            total_tokens,
            estimatedTotalCostUsd,
            ' '
        ))) AS ChangeHashKey,
        CURRENT_TIMESTAMP AS InsertDatetime,
        CURRENT_TIMESTAMP AS UpdateDatetime,

        email,
        threadId,
        conversationId,
        createdOn,
        assistantArea,
        responseMode,
        model,
        vectorStorageId,
        input_tokens,
        output_tokens,
        reasoning_tokens,
        total_tokens,
        estimatedTotalCostUsd

    FROM GPT.OpenAITokenUsage
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
    TD.email,
    TD.threadId,
    TD.conversationId,
    TD.createdOn,
    TD.assistantArea,
    TD.responseMode,
    TD.model,
    TD.vectorStorageId,
    TD.input_tokens,
    TD.output_tokens,
    TD.reasoning_tokens,
    TD.total_tokens,
    TD.estimatedTotalCostUsd

FROM TableData TD;

GO


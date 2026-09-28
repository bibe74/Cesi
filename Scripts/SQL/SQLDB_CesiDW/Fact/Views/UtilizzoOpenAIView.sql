
CREATE   VIEW Fact.UtilizzoOpenAIView
AS
SELECT
    -- Chiavi
    TD.IDUtilizzoOpenAI,

    -- Campi per data warehouse
    CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT(
        TD.IDUtilizzoOpenAI,
    	' '
    ))) AS HistoricalHashKey,
    CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT(
        TD.PKCliente,
        TD.IDThread,
        TD.IDConversazione,
        TD.PKDataConversazione,
        TD.Area,
        TD.ModalitaRisposta,
        TD.Modello,
        TD.IDVectorStorage,
        TD.InputTokens,
        TD.OutputTokens,
        TD.ReasoningTokens,
        TD.TotalTokens,
        TD.EstimatedTotalCostUSD,
    	' '
    ))) AS ChangeHashKey,
    CURRENT_TIMESTAMP AS InsertDatetime,
    CURRENT_TIMESTAMP AS UpdateDatetime,
    CAST(0 AS BIT) AS IsDeleted,
    
    -- Dimensioni
    TD.PKCliente,
    TD.IDThread,
    TD.IDConversazione,
    TD.PKDataConversazione,
    TD.Area,
    TD.ModalitaRisposta,
    TD.Modello,
    TD.IDVectorStorage,
    	
    -- Misure
    TD.InputTokens,
    TD.OutputTokens,
    TD.ReasoningTokens,
    TD.TotalTokens,
    TD.EstimatedTotalCostUSD
    	
FROM (
    
    SELECT
        OAITU.Id AS IDUtilizzoOpenAI,
        --OAITU.email,
        C.PKCliente,
        OAITU.threadId AS IDThread,
        OAITU.conversationId AS IDConversazione,
        --OAITU.createdOn,
        D.PKData AS PKDataConversazione,
        OAITU.assistantArea AS Area,
        OAITU.responseMode AS ModalitaRisposta,
        OAITU.model AS Modello,
        OAITU.vectorStorageId AS IDVectorStorage,
        OAITU.input_tokens AS InputTokens,
        OAITU.output_tokens AS OutputTokens,
        OAITU.reasoning_tokens AS ReasoningTokens,
        OAITU.total_tokens AS TotalTokens,
        OAITU.estimatedTotalCostUsd AS EstimatedTotalCostUSD

    FROM Landing.GPT_OpenAITokenUsage OAITU
    INNER JOIN Dim.Cliente C ON C.Email = OAITU.email
        AND C.IsDeleted = CAST(0 AS BIT)
    INNER JOIN Dim.Data D ON D.PKData = CONVERT(DATE, OAITU.createdOn)
    WHERE OAITU.IsDeleted = CAST(0 AS BIT)
        AND OAITU.email <> N''

) TD;

GO


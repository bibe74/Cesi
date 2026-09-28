
CREATE   PROCEDURE Landing.usp_Merge_GPT_OpenAITokenUsage
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Landing.GPT_OpenAITokenUsage AS TGT
    USING Landing.GPT_OpenAITokenUsageView AS SRC ON (
         SRC.Id = TGT.Id 
    )

    WHEN MATCHED AND SRC.ChangeHashKey <> TGT.ChangeHashKey
      THEN UPDATE SET TGT.ChangeHashKey = SRC.ChangeHashKey, TGT.ChangeHashKeyASCII = SRC.ChangeHashKeyASCII, TGT.UpdateDatetime = SRC.UpdateDatetime, TGT.IsDeleted = SRC.IsDeleted, 
        TGT.email = SRC.email, TGT.threadId = SRC.threadId, TGT.conversationId = SRC.conversationId, TGT.createdOn = SRC.createdOn, TGT.assistantArea = SRC.assistantArea, TGT.responseMode = SRC.responseMode, TGT.model = SRC.model, TGT.vectorStorageId = SRC.vectorStorageId, TGT.input_tokens = SRC.input_tokens, TGT.output_tokens = SRC.output_tokens, TGT.reasoning_tokens = SRC.reasoning_tokens, TGT.total_tokens = SRC.total_tokens, TGT.estimatedTotalCostUsd = SRC.estimatedTotalCostUsd

    WHEN NOT MATCHED BY TARGET
      THEN INSERT (Id, HistoricalHashKey, ChangeHashKey, HistoricalHashKeyASCII, ChangeHashKeyASCII, InsertDatetime, UpdateDatetime, IsDeleted, email, threadId, conversationId, createdOn, assistantArea, responseMode, model, vectorStorageId, input_tokens, output_tokens, reasoning_tokens, total_tokens, estimatedTotalCostUsd)
        VALUES (Id, HistoricalHashKey, ChangeHashKey, HistoricalHashKeyASCII, ChangeHashKeyASCII, InsertDatetime, UpdateDatetime, IsDeleted, email, threadId, conversationId, createdOn, assistantArea, responseMode, model, vectorStorageId, input_tokens, output_tokens, reasoning_tokens, total_tokens, estimatedTotalCostUsd)

    WHEN NOT MATCHED BY SOURCE AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE SET TGT.ChangeHashKey = CONVERT(VARBINARY(32), 0),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.IsDeleted = CAST(1 AS BIT)
    
    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Landing.GPT_OpenAITokenUsage' AS full_olap_table_name,
        'Id = ' + CAST(COALESCE(inserted.Id, deleted.Id) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END

GO


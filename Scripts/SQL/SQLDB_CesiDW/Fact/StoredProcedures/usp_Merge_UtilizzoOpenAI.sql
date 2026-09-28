
CREATE   PROCEDURE Fact.usp_Merge_UtilizzoOpenAI
AS
BEGIN
    SET NOCOUNT ON;

    MERGE INTO Fact.UtilizzoOpenAI AS TGT
    USING Fact.UtilizzoOpenAIView AS SRC ON (
         SRC.IDUtilizzoOpenAI = TGT.IDUtilizzoOpenAI 
    )

    WHEN MATCHED AND SRC.ChangeHashKey <> TGT.ChangeHashKey
      THEN UPDATE SET TGT.ChangeHashKey = SRC.ChangeHashKey, TGT.UpdateDatetime = SRC.UpdateDatetime, TGT.IsDeleted = SRC.IsDeleted, 
        TGT.PKCliente = SRC.PKCliente, TGT.IDThread = SRC.IDThread, TGT.IDConversazione = SRC.IDConversazione, TGT.PKDataConversazione = SRC.PKDataConversazione, TGT.Area = SRC.Area, TGT.ModalitaRisposta = SRC.ModalitaRisposta, TGT.Modello = SRC.Modello, TGT.IDVectorStorage = SRC.IDVectorStorage, TGT.InputTokens = SRC.InputTokens, TGT.OutputTokens = SRC.OutputTokens, TGT.ReasoningTokens = SRC.ReasoningTokens, TGT.TotalTokens = SRC.TotalTokens, TGT.EstimatedTotalCostUSD = SRC.EstimatedTotalCostUSD

    WHEN NOT MATCHED BY TARGET
      THEN INSERT (IDUtilizzoOpenAI, HistoricalHashKey, ChangeHashKey, InsertDatetime, UpdateDatetime, IsDeleted, PKCliente, IDThread, IDConversazione, PKDataConversazione, Area, ModalitaRisposta, Modello, IDVectorStorage, InputTokens, OutputTokens, ReasoningTokens, TotalTokens, EstimatedTotalCostUSD)
        VALUES (IDUtilizzoOpenAI, HistoricalHashKey, ChangeHashKey, InsertDatetime, UpdateDatetime, IsDeleted, PKCliente, IDThread, IDConversazione, PKDataConversazione, Area, ModalitaRisposta, Modello, IDVectorStorage, InputTokens, OutputTokens, ReasoningTokens, TotalTokens, EstimatedTotalCostUSD)

    WHEN NOT MATCHED BY SOURCE AND TGT.IsDeleted = CAST(0 AS BIT)
      THEN UPDATE SET TGT.ChangeHashKey = CONVERT(VARBINARY(32), 0),
        TGT.UpdateDatetime = CURRENT_TIMESTAMP,
        TGT.IsDeleted = CAST(1 AS BIT)
    
    OUTPUT
        CURRENT_TIMESTAMP AS merge_datetime,
        CASE WHEN Inserted.IsDeleted = CAST(1 AS BIT) THEN N'DELETE' ELSE $action END AS merge_action,
        'Fact.UtilizzoOpenAI' AS full_olap_table_name,
        'IDUtilizzoOpenAI = ' + CAST(COALESCE(inserted.IDUtilizzoOpenAI, deleted.IDUtilizzoOpenAI) AS NVARCHAR) AS primary_key_description
    INTO audit.merge_log_details;

END

GO

